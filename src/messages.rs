//! Broadcast from the database: a row written to `realtime.messages` (by `realtime.send()`, or
//! by a customer's own trigger) is broadcast to the topic it names.
//!
//! Streamed, not polled: a logical replication connection with pgoutput, on a publication of
//! realtime.messages alone, through a TEMPORARY slot that lives only as long as the connection,
//! so a process that dies holds no WAL. The publication publishes via the partitioned root, so
//! a row from any day's partition arrives as realtime.messages.
//!
//! The publication and slot carry our own names (the pinned server's are its own); a project
//! switched back to it makes its own.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use crate::db::{DbError, quote_ident};
use crate::hub::{self, TenantRt, TopicKey};
use crate::pgoutput::{self, Message, Relation};
use crate::protocol::Encoding;
use crate::replication::{Connection, Event, Target};
use crate::tenants::Database;

pub const PUBLICATION: &str = "snout_realtime_messages";

/// Run the stream for one project until it fails or the project has had no sockets for a
/// while. The caller restarts it on the next join.
pub async fn run(rt: Arc<TenantRt>, pool: deadpool_postgres::Pool, database: Database) {
	loop {
		match stream(&rt, &pool, &database).await {
			Ok(()) => return,
			Err(e) => {
				tracing::warn!(tenant = %rt.id, error = %e, "broadcast from database");
				tokio::time::sleep(Duration::from_secs(2)).await;
				if rt.connections() == 0 {
					return;
				}
			}
		}
	}
}

async fn ensure_publication(pool: &deadpool_postgres::Pool) -> Result<(), String> {
	let client = pool.get().await.map_err(|e| e.to_string())?;
	let sql = format!(
		"do $$ begin
			if not exists (select 1 from pg_publication where pubname = '{PUBLICATION}') then
				create publication {pub} for table realtime.messages with (publish_via_partition_root = true);
			end if;
		end $$",
		pub = quote_ident(PUBLICATION)
	);
	client
		.batch_execute(&sql)
		.await
		.map_err(|e| DbError::from_pg(e).to_string())
}

async fn stream(
	rt: &TenantRt,
	pool: &deadpool_postgres::Pool,
	database: &Database,
) -> Result<(), String> {
	ensure_publication(pool).await?;
	let target = Target {
		host: database.host.clone(),
		port: database.port,
		user: database.user.clone(),
		password: database.password.clone(),
		database: database.name.clone(),
		application_name: "snout_realtime_messages".into(),
	};
	let mut conn = Connection::connect(&target)
		.await
		.map_err(|e| e.to_string())?;
	let slot = format!("snout_realtime_messages_{}", std::process::id());
	conn.simple_query(&format!(
		"CREATE_REPLICATION_SLOT {} TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT",
		quote_ident(&slot)
	))
	.await
	.map_err(|e| e.to_string())?;
	conn.start(&format!(
		"START_REPLICATION SLOT {} LOGICAL 0/0 (proto_version '1', publication_names '{PUBLICATION}')",
		quote_ident(&slot)
	))
	.await
	.map_err(|e| e.to_string())?;
	let mut relations: HashMap<u32, Relation> = HashMap::new();
	let mut idle_since: Option<std::time::Instant> = None;
	loop {
		let event = match tokio::time::timeout(Duration::from_secs(10), conn.next()).await {
			Ok(e) => e.map_err(|e| e.to_string())?,
			Err(_) => {
				// No keepalive in ten seconds: stop if nobody is listening any more.
				if rt.connections() == 0 {
					let since = *idle_since.get_or_insert_with(std::time::Instant::now);
					if since.elapsed() > Duration::from_secs(60) {
						return Ok(());
					}
				} else {
					idle_since = None;
				}
				continue;
			}
		};
		match event {
			Event::Keepalive { end_lsn, reply } => {
				if reply {
					conn.ack(end_lsn).await.map_err(|e| e.to_string())?;
				}
			}
			Event::Data { end_lsn, data, .. } => {
				match pgoutput::decode(data).map_err(|e| e.to_string())? {
					Message::Relation(r) => {
						relations.insert(r.id, r);
					}
					Message::Insert { relation, new } => {
						if let Some(r) = relations.get(&relation)
							&& r.schema == "realtime"
							&& r.name == "messages"
						{
							broadcast_row(rt, r, &new);
						}
					}
					Message::Commit {
						end_lsn: commit_end,
						..
					} => {
						conn.ack(commit_end.max(end_lsn))
							.await
							.map_err(|e| e.to_string())?;
					}
					_ => {}
				}
			}
		}
	}
}

/// A column's value by name.
fn value<'a>(r: &Relation, row: &'a [pgoutput::Value], name: &str) -> Option<&'a str> {
	let i = r.columns.iter().position(|c| c.name == name)?;
	match row.get(i)? {
		pgoutput::Value::Text(s) => Some(s.as_str()),
		_ => None,
	}
}

fn hex_bytes(text: &str) -> Option<Vec<u8>> {
	let hex = text.strip_prefix("\\x")?;
	(0..hex.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(hex.get(i..i + 2)?, 16).ok())
		.collect()
}

fn broadcast_row(rt: &TenantRt, r: &Relation, row: &[pgoutput::Value]) {
	let (Some(topic), Some(event), Some(id)) = (
		value(r, row, "topic"),
		value(r, row, "event"),
		value(r, row, "id"),
	) else {
		return;
	};
	let private = value(r, row, "private") == Some("t");
	let (encoding, payload) = if let Some(b) = value(r, row, "binary_payload").and_then(hex_bytes) {
		(Encoding::Binary, b)
	} else if let Some(p) = value(r, row, "payload") {
		(Encoding::Json, p.as_bytes().to_vec())
	} else {
		return;
	};
	let tenant = rt.tenant();
	if payload.len() as i64 > tenant.max_payload_size_in_kb * 1000 + 500 {
		return;
	}
	let key = TopicKey {
		name: topic.to_string(),
		private,
	};
	hub::deliver_user(
		rt,
		&key,
		None,
		hub::UserMessage {
			event,
			encoding,
			payload: &payload,
			metadata: Some(json!({ "id": id })),
			id: Some(id.to_string()),
		},
	);
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn a_bytea_value_decodes() {
		assert_eq!(hex_bytes("\\x00ff10"), Some(vec![0, 255, 16]));
		assert_eq!(hex_bytes("nope"), None);
		assert_eq!(hex_bytes("\\x0"), None);
	}
}
