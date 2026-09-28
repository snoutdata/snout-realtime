//! postgres_changes: subscriptions in `realtime.subscription`, and the changes each one may see.
//!
//! Changes are STREAMED from a logical replication slot (pgoutput) the moment they commit, never
//! polled, and decided in batches: the changes to one table that have arrived and not been
//! decided yet, one when changes are sparse. `migrations/0002_changes.sql` decides who may see
//! each and what they see; the row-level security check runs here, as each distinct set of
//! claims, once per batch (`decide`).
//!
//!  - The slot exists BEFORE a subscriber is told "Subscribed to PostgreSQL", so no change
//!    committed after that sentence is lost, and none from before the subscription is sent.
//!  - Nothing is streamed, and no slot is held, while nobody is subscribed.
//!  - The slot is TEMPORARY: it lives as long as the stream's own connection, so a process that
//!    dies holds no WAL.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use serde_json::{Map, Value};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{Mutex, Notify};
use uuid::Uuid;

use crate::db::quote_ident;
use crate::hub::{Out, TenantRt};
use crate::pgoutput::{self, Message, Old, Relation};
use crate::replication::{Connection, Event, Target};
use crate::tenants::Database;

/// One binding of one channel: a row in realtime.subscription.
#[derive(Debug, Clone)]
pub struct Binding {
	pub subscription_id: Uuid,
	/// The id the client matches changes by (in the join reply and each change's `ids`).
	pub id: i64,
	pub action: String,
	pub schema: String,
	pub table: String,
	/// `(column, op, value)`, value already in the form the table stores (`in` as `{a,b}`).
	pub filters: Vec<(String, String, String)>,
	pub selected: Option<Vec<String>>,
}

struct Listener {
	/// The channel join this binding came with: all of a channel's bindings share it.
	channel: u64,
	tx: UnboundedSender<Out>,
	join_topic: String,
	id: i64,
	schema: String,
	table: String,
	action: String,
}

/// A project's postgres_changes: who is subscribed, and the stream.
pub struct Changes {
	database: Database,
	listeners: Mutex<HashMap<Uuid, Listener>>,
	wake: Notify,
	/// The poller's slot is ready (made on its own connection).
	ready: Notify,
	running: std::sync::atomic::AtomicBool,
}

/// The id a binding is known by. A stable hash of its parameters, so the same binding gets the
/// same id on every join, as the client expects.
pub fn binding_id(params: &Value) -> i64 {
	// FNV-1a over the canonical JSON, folded to 27 bits like an Erlang phash2.
	let text = canonical(params);
	let mut h: u64 = 0xcbf29ce484222325;
	for b in text.as_bytes() {
		h ^= *b as u64;
		h = h.wrapping_mul(0x100000001b3);
	}
	(h % (1 << 27)) as i64
}

fn canonical(v: &Value) -> String {
	match v {
		Value::Object(m) => {
			let mut keys: Vec<&String> = m.keys().collect();
			keys.sort();
			let inner: Vec<String> = keys
				.iter()
				.map(|k| format!("{}:{}", Value::String((*k).clone()), canonical(&m[*k])))
				.collect();
			format!("{{{}}}", inner.join(","))
		}
		Value::Array(a) => format!(
			"[{}]",
			a.iter().map(canonical).collect::<Vec<_>>().join(",")
		),
		other => other.to_string(),
	}
}

/// A filter: `(column, op, value)`.
pub type Filter = (String, String, String);

/// A binding's parameters, parsed: action, schema, table, filters and selected columns.
pub type Parsed = (String, String, String, Vec<Filter>, Option<Vec<String>>);

/// Parse a binding's parameters the way the pinned server does, with its sentences.
pub fn parse(params: &Map<String, Value>) -> Result<Parsed, String> {
	let action = match params.get("event").and_then(Value::as_str) {
		Some(e) => match e.to_ascii_uppercase().as_str() {
			"INSERT" => "INSERT",
			"UPDATE" => "UPDATE",
			"DELETE" => "DELETE",
			_ => "*",
		},
		None => "*",
	}
	.to_string();
	let selected = match params.get("select") {
		Some(Value::Array(cols)) => {
			let v: Vec<String> = cols.iter().filter_map(|c| c.as_str().map(str::to_string)).collect();
			if v.is_empty() { None } else { Some(v) }
		}
		Some(Value::String(_)) => return Err("Error parsing `select` params: expected a list of column name strings, e.g. select: [\"col1\", \"col2\"]".into()),
		_ => None,
	};
	let schema = params.get("schema").and_then(Value::as_str);
	let table = params.get("table").and_then(Value::as_str);
	let filter = params.get("filter");
	let (schema, table, filters) = match (schema, table, filter) {
		(Some(s), Some(t), Some(Value::String(f))) => {
			(s.to_string(), t.to_string(), parse_filters(f)?)
		}
		(Some(s), Some(t), None) => (s.to_string(), t.to_string(), Vec::new()),
		(Some(s), None, None) => (s.to_string(), "*".to_string(), Vec::new()),
		(None, Some(t), None) => ("public".to_string(), t.to_string(), Vec::new()),
		_ => {
			return Err(format!(
				"No subscription params provided. Please provide at least a `schema` or `table` to subscribe to: {}",
				elixir_map(params)
			));
		}
	};
	if selected.is_some() && (schema == "*" || table == "*") {
		return Err("Column selection is not supported for wildcard subscriptions. Provide an explicit schema and table name.".into());
	}
	Ok((action, schema, table, filters, selected))
}

/// A map as Elixir's `inspect` prints it, for the one sentence that quotes one.
fn elixir_map(m: &Map<String, Value>) -> String {
	let mut keys: Vec<&String> = m.keys().collect();
	keys.sort();
	let parts: Vec<String> = keys
		.iter()
		.map(|k| format!("{} => {}", Value::String((*k).clone()), m[*k]))
		.collect();
	format!("%{{{}}}", parts.join(", "))
}

const FILTER_TYPES: [&str; 7] = ["eq", "neq", "lt", "lte", "gt", "gte", "in"];

/// `col=op.value[,col=op.value...]`, commas inside parentheses not splitting.
pub fn parse_filters(filter: &str) -> Result<Vec<(String, String, String)>, String> {
	let trimmed = filter.trim();
	if trimmed.is_empty() {
		return Ok(Vec::new());
	}
	let mut segments = Vec::new();
	let (mut depth, mut start) = (0i32, 0usize);
	for (i, c) in trimmed.char_indices() {
		match c {
			'(' => depth += 1,
			')' => depth = (depth - 1).max(0),
			',' if depth == 0 => {
				segments.push(&trimmed[start..i]);
				start = i + 1;
			}
			_ => {}
		}
	}
	segments.push(&trimmed[start..]);
	let mut out = Vec::new();
	for segment in segments {
		let s = segment.trim();
		if s.is_empty() {
			return Err("Error parsing `filter` params: filter must not contain empty segments (check for extra commas)".into());
		}
		let Some((col, rest)) = s.split_once('=') else {
			return Err(format!("Error parsing `filter` params: [\"{}\"]", s));
		};
		let Some((op, value)) = rest
			.split_once('.')
			.filter(|(op, _)| FILTER_TYPES.contains(op))
		else {
			let parts: Vec<String> = rest.splitn(2, '.').map(|p| format!("\"{p}\"")).collect();
			return Err(format!(
				"Error parsing `filter` params: [{}]",
				parts.join(", ")
			));
		};
		let value = if op == "in" {
			if value.len() >= 2 && value.starts_with('(') && value.ends_with(')') {
				format!("{{{}}}", &value[1..value.len() - 1])
			} else {
				return Err("Error parsing `filter` params: `in` filter value must be wrapped by parentheses".into());
			}
		} else {
			value.to_string()
		};
		out.push((col.to_string(), op.to_string(), value));
	}
	Ok(out)
}

/// How a binding is described in the pinned server's sentences.
pub fn describe(b: &Binding) -> String {
	let filters: Vec<String> = b
		.filters
		.iter()
		.map(|(c, o, v)| format!("{{{:?}, {:?}, {:?}}}", c, o, v))
		.collect();
	let select = match &b.selected {
		None => "nil".to_string(),
		Some(s) => format!(
			"[{}]",
			s.iter()
				.map(|c| format!("{c:?}"))
				.collect::<Vec<_>>()
				.join(", ")
		),
	};
	format!(
		"event: {}, schema: {}, table: {}, filters: [{}], select: {}",
		b.action,
		b.schema,
		b.table,
		filters.join(", "),
		select
	)
}

const INSERT_SQL: &str = "with sub_tables as (
	select rr.entity from pg_publication_tables pub,
	lateral (select format('%I.%I', pub.schemaname, pub.tablename)::regclass entity) rr
	where pub.pubname = $1
	  and pub.schemaname like (case $2 when '*' then '%' else $2 end) escape ''
	  and pub.tablename like (case $3 when '*' then '%' else $3 end) escape ''
)
insert into realtime.subscription as x (subscription_id, entity, filters, claims, action_filter, selected_columns)
select $4::text::uuid, sub_tables.entity,
	(select coalesce(array_agg(row(f->>0, (f->>1)::realtime.equality_op, f->>2)::realtime.user_defined_filter), '{}') from jsonb_array_elements($6::jsonb) f),
	$5, $7, $8
from sub_tables
on conflict (subscription_id, entity, filters, action_filter, coalesce(selected_columns, '{}'))
do update set claims = excluded.claims, created_at = now()
returning id";

impl Changes {
	pub fn new(database: Database) -> Arc<Changes> {
		Arc::new(Changes {
			database,
			listeners: Mutex::new(HashMap::new()),
			wake: Notify::new(),
			ready: Notify::new(),
			running: false.into(),
		})
	}

	/// Insert a channel's bindings, all or none. The error is the sentence the client gets.
	pub async fn subscribe(
		self: &Arc<Self>,
		rt: Arc<TenantRt>,
		pool: &deadpool_postgres::Pool,
		claims: &Map<String, Value>,
		bindings: &[Binding],
		tx: UnboundedSender<Out>,
		join_topic: &str,
	) -> Result<(), String> {
		self.ensure_stream(rt).await;
		static CHANNELS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
		let channel = CHANNELS.fetch_add(1, Ordering::Relaxed);
		// Listening BEFORE the rows exist: the stream's sweep of rows that are not its own
		// (`sweep`) holds this lock, so a row is either written after it or known to it.
		{
			let mut listeners = self.listeners.lock().await;
			for b in bindings {
				listeners.insert(
					b.subscription_id,
					Listener {
						channel,
						tx: tx.clone(),
						join_topic: join_topic.to_string(),
						id: b.id,
						schema: b.schema.clone(),
						table: b.table.clone(),
						action: b.action.clone(),
					},
				);
			}
		}
		let result = self.insert_bindings(pool, claims, bindings).await;
		if result.is_err() {
			let mut listeners = self.listeners.lock().await;
			for b in bindings {
				listeners.remove(&b.subscription_id);
			}
		}
		self.wake.notify_one();
		result
	}

	async fn insert_bindings(
		&self,
		pool: &deadpool_postgres::Pool,
		claims: &Map<String, Value>,
		bindings: &[Binding],
	) -> Result<(), String> {
		let mut client = pool
			.get()
			.await
			.map_err(|e| format!("Unable to subscribe to changes: {e}"))?;
		let t = client.transaction().await.map_err(|e| e.to_string())?;
		let claims = Value::Object(claims.clone());
		for b in bindings {
			let filters = Value::Array(
				b.filters
					.iter()
					.map(|(c, o, v)| {
						Value::Array(vec![c.clone().into(), o.clone().into(), v.clone().into()])
					})
					.collect(),
			);
			let result = t
				.query(
					INSERT_SQL,
					&[
						&self.database.publication,
						&b.schema,
						&b.table,
						&b.subscription_id.to_string(),
						&claims,
						&filters,
						&b.action,
						&b.selected,
					],
				)
				.await;
			match result {
				Ok(rows) if !rows.is_empty() => {}
				Ok(_) => {
					return Err(format!(
						"Unable to subscribe to changes with given parameters. Please check Realtime is enabled for the given connect parameters: [{}]",
						describe(b)
					));
				}
				Err(e) => {
					let detail = match e.as_db_error() {
						Some(db) => format!(
							"ERROR {} ({}) {}",
							db.code().code(),
							condition_name(db.code().code()),
							db.message()
						),
						None => e.to_string(),
					};
					return Err(format!(
						"Unable to subscribe to changes with given parameters. An exception happened so please check your connect parameters: [{}]. Exception: {}",
						describe(b),
						detail
					));
				}
			}
		}
		t.commit().await.map_err(|e| e.to_string())?;
		Ok(())
	}

	/// A channel's token changed: its bindings are checked as the new claims from the next change
	/// on. Without this a socket that swapped one user's token for another's kept receiving the
	/// first user's rows.
	pub async fn reclaim(
		&self,
		pool: &deadpool_postgres::Pool,
		ids: &[Uuid],
		claims: &Map<String, Value>,
	) -> Result<(), String> {
		if ids.is_empty() {
			return Ok(());
		}
		let client = pool.get().await.map_err(|e| e.to_string())?;
		let ids: Vec<String> = ids.iter().map(Uuid::to_string).collect();
		client
			.execute(
				"update realtime.subscription set claims = $1 where subscription_id = any($2::text[]::uuid[])",
				&[&Value::Object(claims.clone()), &ids],
			)
			.await
			.map_err(|e| e.to_string())?;
		Ok(())
	}

	/// Remove a channel's bindings.
	pub async fn unsubscribe(&self, pool: &deadpool_postgres::Pool, ids: &[Uuid]) {
		if ids.is_empty() {
			return;
		}
		let mut listeners = self.listeners.lock().await;
		for id in ids {
			listeners.remove(id);
		}
		drop(listeners);
		if let Ok(client) = pool.get().await {
			let ids: Vec<String> = ids.iter().map(Uuid::to_string).collect();
			let _ = client
				.execute(
					"delete from realtime.subscription where subscription_id = any($1::text[]::uuid[])",
					&[&ids],
				)
				.await;
		}
	}

	async fn ensure_stream(self: &Arc<Self>, rt: Arc<TenantRt>) {
		if self.running.swap(true, Ordering::SeqCst) {
			return;
		}
		let me = self.clone();
		let ready = self.ready.notified();
		tokio::spawn(async move { me.run(rt).await });
		// Wait for the slot, so no change after "Subscribed to PostgreSQL" is lost.
		let _ = tokio::time::timeout(Duration::from_secs(10), ready).await;
	}

	async fn run(self: Arc<Self>, rt: Arc<TenantRt>) {
		loop {
			match self.stream_until_idle(&rt).await {
				Ok(()) => break,
				Err(e) => {
					tracing::warn!(tenant = %rt.id, error = %e, "database changes");
					tokio::time::sleep(Duration::from_millis(500)).await;
					if self.listeners.lock().await.is_empty() {
						break;
					}
				}
			}
		}
		self.running.store(false, Ordering::SeqCst);
	}

	/// Is anybody subscribed to this kind of change on this table?
	async fn wanted(&self, schema: &str, table: &str, action: &str) -> bool {
		self.listeners.lock().await.values().any(|l| {
			(l.schema == "*" || l.schema == schema)
				&& (l.table == "*" || l.table == table)
				&& (l.action == "*" || l.action == action)
		})
	}

	/// Stream the project's changes through a temporary slot until nobody has been subscribed
	/// for thirty seconds.
	async fn stream_until_idle(&self, rt: &TenantRt) -> Result<(), String> {
		let target = Target {
			host: self.database.host.clone(),
			port: self.database.port,
			user: self.database.user.clone(),
			password: self.database.password.clone(),
			database: self.database.name.clone(),
			application_name: "snout_realtime_changes".into(),
		};
		let mut conn = Connection::connect(&target)
			.await
			.map_err(|e| e.to_string())?;
		let slot = format!("snout_realtime_changes_{}", std::process::id());
		conn.simple_query(&format!(
			"CREATE_REPLICATION_SLOT {} TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT",
			quote_ident(&slot)
		))
		.await
		.map_err(|e| e.to_string())?;
		let publication = self.database.publication.replace('\'', "");
		conn.start(&format!(
			"START_REPLICATION SLOT {} LOGICAL 0/0 (proto_version '1', publication_names '{publication}')",
			quote_ident(&slot)
		))
		.await
		.map_err(|e| e.to_string())?;
		// The first connection for the checks; a second joins it once there are many subscribers.
		let mut checkers = vec![self.checker().await?];
		let mut statements = vec![Statements::new()];
		match self.sweep(&checkers[0]).await {
			Ok(0) => {}
			Ok(n) => {
				tracing::info!(tenant = %rt.id, rows = n, "subscriptions left by an earlier server removed")
			}
			Err(e) => {
				tracing::warn!(tenant = %rt.id, error = %e, "subscriptions left by an earlier server")
			}
		}
		self.ready.notify_waiters();
		let mut relations: HashMap<u32, Relation> = HashMap::new();
		let mut commit_time: i64 = 0;
		let mut xid: u32 = 0;
		let mut idle_since: Option<std::time::Instant> = None;
		// Changes read and not decided yet, all to one table and of one kind, and the commit to
		// acknowledge once they are.
		let mut batch: Option<Batch> = None;
		let mut ack: Option<u64> = None;
		loop {
			// While something waits to be decided, take only what has already arrived: when
			// nothing more is there, decide what has queued. So a lone change goes at once, and
			// changes that queue while one batch is decided are decided together.
			let event = if batch.is_some() || ack.is_some() {
				match futures_util::FutureExt::now_or_never(conn.next()) {
					Some(e) => e.map_err(|e| e.to_string())?,
					None => {
						self.flush(rt, &mut checkers, &mut statements, &relations, &mut batch)
							.await?;
						if let Some(lsn) = ack.take() {
							conn.ack(lsn).await.map_err(|e| e.to_string())?;
						}
						continue;
					}
				}
			} else {
				match tokio::time::timeout(Duration::from_secs(5), conn.next()).await {
					Ok(e) => e.map_err(|e| e.to_string())?,
					Err(_) => {
						if self.listeners.lock().await.is_empty() {
							let since = *idle_since.get_or_insert_with(std::time::Instant::now);
							if since.elapsed() > Duration::from_secs(30) {
								return Ok(());
							}
						} else {
							idle_since = None;
						}
						continue;
					}
				}
			};
			match event {
				Event::Keepalive { end_lsn, reply } => {
					if reply {
						self.flush(rt, &mut checkers, &mut statements, &relations, &mut batch)
							.await?;
						conn.ack(ack.take().unwrap_or(0).max(end_lsn))
							.await
							.map_err(|e| e.to_string())?;
					}
				}
				Event::Data { end_lsn, data, .. } => {
					let (relation, action, new, old) = match pgoutput::decode(data)
						.map_err(|e| e.to_string())?
					{
						Message::Begin {
							commit_time: t,
							xid: x,
							..
						} => {
							commit_time = t;
							xid = x;
							continue;
						}
						Message::Relation(r) => {
							// A table's shape changed: what was read of it is decided first.
							self.flush(rt, &mut checkers, &mut statements, &relations, &mut batch)
								.await?;
							relations.insert(r.id, r);
							continue;
						}
						Message::Commit {
							end_lsn: commit_end,
							..
						} => {
							ack = Some(commit_end.max(end_lsn));
							continue;
						}
						Message::Insert { relation, new } => (relation, "INSERT", Some(new), None),
						Message::Update { relation, old, new } => {
							(relation, "UPDATE", Some(new), old)
						}
						Message::Delete { relation, old } => (relation, "DELETE", None, Some(old)),
						Message::Other(_) => continue,
					};
					let Some(r) = relations.get(&relation) else {
						continue;
					};
					if batch.as_ref().is_some_and(|b| {
						b.relation != relation || b.action != action || b.changes.len() >= BATCH
					}) {
						self.flush(rt, &mut checkers, &mut statements, &relations, &mut batch)
							.await?;
					}
					if !self.wanted(&r.schema, &r.name, action).await {
						continue;
					}
					batch
						.get_or_insert_with(|| Batch {
							relation,
							action,
							changes: Vec::new(),
						})
						.changes
						.push(Change::read(
							r,
							action,
							commit_time,
							xid,
							new.as_deref(),
							old.as_ref(),
						));
				}
			}
		}
	}

	/// Decide a batch of changes in the project's database and send them, in the order they were
	/// committed. An error is a checker's connection gone.
	async fn flush(
		&self,
		rt: &TenantRt,
		checkers: &mut Vec<tokio_postgres::Client>,
		statements: &mut Vec<Statements>,
		relations: &HashMap<u32, Relation>,
		batch: &mut Option<Batch>,
	) -> Result<(), String> {
		let Some(b) = batch.take() else {
			return Ok(());
		};
		if !relations.contains_key(&b.relation) {
			return Ok(());
		}
		if checkers.len() < CHECKERS && self.listeners.lock().await.len() >= SHARE_FROM {
			match self.checker().await {
				Ok(c) => {
					checkers.push(c);
					statements.push(Statements::new());
				}
				Err(e) => tracing::warn!(tenant = %rt.id, error = %e, "a second connection for the checks"),
			}
		}
		let started = std::time::Instant::now();
		let clients: Vec<&tokio_postgres::Client> = checkers.iter().collect();
		match decide(
			&clients,
			statements,
			b.relation,
			b.action,
			&b.changes,
			self.database.poll_max_record_bytes as i32,
		)
		.await
		{
			Ok(groups) => {
				let decided = started.elapsed();
				for (_, group) in groups {
					self.dispatch(rt, &group.ids, record(&group.payload, group.errors))
						.await;
				}
				tracing::debug!(
					tenant = %rt.id,
					changes = b.changes.len(),
					decide_ms = decided.as_secs_f64() * 1000.0,
					total_ms = started.elapsed().as_secs_f64() * 1000.0,
					"a batch of changes"
				);
				Ok(())
			}
			// The connection is gone: the stream starts again with a new one.
			Err(e) if checkers.iter().any(|c| c.is_closed()) => Err(e.to_string()),
			Err(e) => {
				tracing::warn!(tenant = %rt.id, error = %e, changes = b.changes.len(), "changes could not be evaluated");
				Ok(())
			}
		}
	}

	/// Remove every subscription row that is not this process's: rows a previous server left
	/// behind (another server's, after a swap, or ours after a crash) would otherwise be checked on
	/// every change and never delivered to. Holds the listeners' lock, and a subscriber is a
	/// listener before its rows are written, so no live row is taken.
	async fn sweep(&self, client: &tokio_postgres::Client) -> Result<u64, tokio_postgres::Error> {
		let listeners = self.listeners.lock().await;
		let ours: Vec<String> = listeners.keys().map(Uuid::to_string).collect();
		client
			.execute(
				"delete from realtime.subscription where not (subscription_id = any($1::text[]::uuid[]))",
				&[&ours],
			)
			.await
	}

	/// The stream's own connection for deciding changes. The row checks switch its role, so it is
	/// never lent to anything else, and they stay prepared on it for as long as it lives.
	async fn checker(&self) -> Result<tokio_postgres::Client, String> {
		let mut config = tokio_postgres::Config::new();
		config
			.host(&self.database.host)
			.port(self.database.port)
			.dbname(&self.database.name)
			.user(&self.database.user)
			.password(&self.database.password)
			.application_name("snout_realtime_checks")
			.connect_timeout(Duration::from_secs(10));
		let (client, connection) = config
			.connect(tokio_postgres::NoTls)
			.await
			.map_err(|e| e.to_string())?;
		tokio::spawn(async move {
			let _ = connection.await;
		});
		Ok(client)
	}

	async fn dispatch(&self, rt: &TenantRt, ids: &[String], data: String) {
		let listeners = self.listeners.lock().await;
		// One frame per channel, with every one of its bindings the change matched.
		let mut per_channel: HashMap<u64, (UnboundedSender<Out>, String, Vec<i64>)> =
			HashMap::with_capacity(ids.len());
		for id in ids {
			let Ok(uuid) = Uuid::parse_str(id) else {
				continue;
			};
			let Some(l) = listeners.get(&uuid) else {
				continue;
			};
			let (_, _, v) = per_channel
				.entry(l.channel)
				.or_insert_with(|| (l.tx.clone(), l.join_topic.clone(), Vec::new()));
			if !v.contains(&l.id) {
				v.push(l.id);
			}
		}
		drop(listeners);
		let data: Arc<str> = Arc::from(data);
		rt.counters
			.db_events
			.fetch_add(per_channel.len() as u64, Ordering::Relaxed);
		for (tx, join_topic, ids) in per_channel.into_values() {
			let _ = tx.send(Out::Changes {
				join_topic,
				ids,
				data: data.clone(),
			});
		}
	}
}

/// The connections a project's row checks run on at most, and the subscribers it takes to open
/// the second: a thousand users' checks take two processes half the time one takes.
const CHECKERS: usize = 2;
const SHARE_FROM: usize = 64;

/// The most changes decided together: enough that a backlog clears in a few calls, few enough
/// that one call stays short.
const BATCH: usize = 500;

/// Changes to one table, of one kind, read from the stream and not decided yet.
struct Batch {
	relation: u32,
	action: &'static str,
	changes: Vec<Change>,
}

/// One change as `decide_changes` takes it: its values as the text Postgres printed.
#[derive(Debug, Clone)]
pub struct Change {
	pub commit_seconds: f64,
	/// The transaction it came from (0 for none: not waited for).
	pub xid: u32,
	pub new: Option<Value>,
	pub old: Option<Value>,
}

impl Change {
	fn read(
		r: &Relation,
		action: &str,
		commit_time: i64,
		xid: u32,
		new: Option<&[pgoutput::Value]>,
		old: Option<&Old>,
	) -> Change {
		let as_json = |row: &[pgoutput::Value], keys_only: bool| -> Value {
			let mut out = Map::new();
			for (c, v) in r.columns.iter().zip(row) {
				if keys_only && !c.key {
					continue;
				}
				match v {
					pgoutput::Value::Text(t) => {
						out.insert(c.name.clone(), Value::String(t.clone()));
					}
					pgoutput::Value::Null => {
						out.insert(c.name.clone(), Value::Null);
					}
					pgoutput::Value::Unchanged => {}
				}
			}
			Value::Object(out)
		};
		Change {
			commit_seconds: commit_time as f64 / 1_000_000.0 + 946_684_800.0,
			xid,
			new: new.map(|n| as_json(n, false)),
			// The old row: what the stream sent (its key, or all of it), or for an update that
			// did not change the key, the key as the new row has it.
			old: match (old, new) {
				(Some(o), _) => Some(as_json(&o.row, o.key_only)),
				(None, Some(n)) if action == "UPDATE" => Some(as_json(n, true)),
				_ => None,
			},
		}
	}
}

/// One group of subscribers who see a change the same way.
#[derive(Debug, Clone)]
pub struct Decided {
	pub ids: Vec<String>,
	pub payload: Value,
	pub errors: Vec<String>,
}

/// Row checks already prepared on a connection, by their text: one per table and key shape, kept
/// for as long as the connection is.
pub type Statements = HashMap<String, tokio_postgres::Statement>;

/// Each distinct set of claims of a role, as JSON text, with its subscribers.
type ClaimGroups = Vec<(String, Vec<String>)>;

/// Which changes of a batch a subscriber sees, by their position from 1.
enum Sees {
	Every,
	These(std::collections::HashSet<i32>),
}

/// Who may see each of a batch of changes to one table, and what: `decide_changes` in one call,
/// then the row check as each distinct set of claims for the whole batch, one round trip per
/// role and connection (migrations/0002_changes.sql says why). Each group comes with its change's
/// position in the batch, in order. `clients` are connections of the caller's own, one or more,
/// with the statements prepared on each (`statements`, as many): the checks switch their role,
/// and it is given back before this returns. The first also answers `decide_changes`.
pub async fn decide(
	clients: &[&tokio_postgres::Client],
	statements: &mut [Statements],
	entity: u32,
	action: &str,
	changes: &[Change],
	max_record_bytes: i32,
) -> Result<Vec<(usize, Decided)>, tokio_postgres::Error> {
	let times: Vec<f64> = changes.iter().map(|c| c.commit_seconds).collect();
	let new: Vec<Option<&Value>> = changes.iter().map(|c| c.new.as_ref()).collect();
	let old: Vec<Option<&Value>> = changes.iter().map(|c| c.old.as_ref()).collect();
	let rows = clients[0]
		.query(
			"select outcome, idx, grp, role_name, claims, subscription_ids::text[], payload, errors, check_sql, keys, rows
			 from snout_realtime.decide_changes($1::oid::regclass, $2,
			   array(select to_timestamp(t) from unnest($3::float8[]) with ordinality u(t, o) order by o),
			   $4::jsonb[], $5::jsonb[], $6)",
			&[&entity, &action, &times, &new, &old, &max_record_bytes],
		)
		.await?;
	let mut out: Vec<(usize, Decided)> = Vec::new();
	let mut sees: HashMap<String, Sees> = HashMap::new();
	// Subscribers with filters: the changes they passed, and their role.
	let mut passed: HashMap<String, (String, std::collections::HashSet<i32>)> = HashMap::new();
	// Per role, each distinct set of claims with its subscribers.
	let mut checks: Vec<(String, ClaimGroups)> = Vec::new();
	let mut check: Option<(String, Value, Vec<i32>)> = None;
	let mut groups: HashMap<i32, Vec<String>> = HashMap::new();
	let mut payloads: Vec<(i32, i32, Value, Vec<String>)> = Vec::new();
	for row in rows {
		let outcome: String = row.get(0);
		let idx: Option<i32> = row.get(1);
		let ids: Vec<String> = row.get::<_, Option<Vec<String>>>(5).unwrap_or_default();
		match outcome.as_str() {
			"send" => out.push((
				idx.unwrap_or(1).max(1) as usize - 1,
				Decided {
					ids,
					payload: row.get::<_, Option<Value>>(6).unwrap_or(Value::Null),
					errors: row.get::<_, Option<Vec<String>>>(7).unwrap_or_default(),
				},
			)),
			"all" => {
				for id in ids {
					sees.insert(id, Sees::Every);
				}
			}
			"row" => {
				let role: String = row.get(3);
				for id in ids {
					passed
						.entry(id)
						.or_insert_with(|| (role.clone(), Default::default()))
						.1
						.insert(idx.unwrap_or(0));
				}
			}
			"check" => {
				let role: String = row.get(3);
				let claims: String = row.get(4);
				match checks.iter_mut().find(|(r, _)| *r == role) {
					Some((_, g)) => g.push((claims, ids)),
					None => checks.push((role, vec![(claims, ids)])),
				}
			}
			"keys" => {
				check = Some((
					row.get(8),
					row.get::<_, Option<Value>>(9).unwrap_or(Value::Null),
					row.get::<_, Option<Vec<i32>>>(10).unwrap_or_default(),
				))
			}
			"group" => {
				groups.insert(row.get::<_, Option<i32>>(2).unwrap_or(0), ids);
			}
			"payload" => payloads.push((
				idx.unwrap_or(0),
				row.get::<_, Option<i32>>(2).unwrap_or(0),
				row.get::<_, Option<Value>>(6).unwrap_or(Value::Null),
				row.get::<_, Option<Vec<String>>>(7).unwrap_or_default(),
			)),
			_ => {}
		}
	}
	// Filtered subscribers of a role that needs no row check see what they passed.
	for (id, (role, changes)) in &passed {
		if !checks.iter().any(|(r, _)| r == role) {
			sees.insert(id.clone(), Sees::These(changes.clone()));
		}
	}
	if let Some((sql, keys, eligible)) = &check {
		// The checks find the rows only once their transactions are visible, which the stream can
		// be a moment ahead of: each connection waits for that before it takes the role.
		let mut xids: Vec<u32> = changes.iter().map(|c| c.xid).filter(|x| *x != 0).collect();
		xids.sort_unstable();
		xids.dedup();
		let visible = if xids.is_empty() {
			String::new()
		} else {
			format!(
				"select snout_realtime.await_visible('{{{}}}'); ",
				xids.iter().map(u32::to_string).collect::<Vec<_>>().join(",")
			)
		};
		let wrapped = format!(
			"with s as materialized (select set_config('request.jwt.claims', $1, true)) select ({sql}) from s"
		);
		for (role, claim_groups) in &checks {
			// A role's claims, split across the connections there are, in order: each takes its
			// share of the checks at once, as the database has a process per connection.
			let share = claim_groups.len().div_ceil(clients.len().min(claim_groups.len()).max(1));
			let shares: Vec<&[(String, Vec<String>)]> = claim_groups.chunks(share.max(1)).collect();
			let mut prepared = Vec::with_capacity(shares.len());
			for (n, _) in shares.iter().enumerate() {
				let statement = match statements[n].get(&wrapped) {
					Some(s) => Some(s.clone()),
					None => match clients[n].prepare(&wrapped).await {
						Ok(s) => {
							statements[n].insert(wrapped.clone(), s.clone());
							Some(s)
						}
						Err(e) => {
							tracing::warn!(role = %role, error = %e, "a row check could not be prepared");
							None
						}
					},
				};
				prepared.push(statement);
			}
			// One round trip per connection: take the role (with a claims default the planner can
			// parse, since it reads the setting when it estimates), run every check, give the role
			// back. Requests on one connection are answered in the order they were sent.
			let take = format!(
				"{visible}set role {}; select set_config('request.jwt.claims', '{{}}', false)",
				quote_ident(role)
			);
			let runs = futures_util::future::join_all(shares.iter().zip(&prepared).enumerate().map(
				|(n, (share, statement))| {
					let (client, take) = (clients[n], &take);
					async move {
						let statement = statement.as_ref()?;
						Some(
							futures_util::future::join3(
								client.batch_execute(take),
								futures_util::future::join_all(share.iter().map(|(claims, _)| async move {
									client.query_one(statement, &[claims, keys]).await
								})),
								client.batch_execute("reset role"),
							)
							.await,
						)
					}
				},
			))
			.await;
			for (n, (share, run)) in shares.iter().zip(runs).enumerate() {
				let Some((taken, answers, given_back)) = run else {
					continue;
				};
				given_back?;
				if let Err(e) = taken {
					// The checks ran as the server itself, which no policy restrains: none counts.
					tracing::warn!(role = %role, error = %e, "a subscriber's role cannot be taken");
					continue;
				}
				for ((_, ids), answer) in share.iter().zip(answers) {
					let positions: Vec<i32> = match answer {
						Ok(row) => row.get::<_, Option<Vec<i32>>>(0).unwrap_or_default(),
						Err(e) => {
							// Prepared again next time, in case the table changed under it.
							statements[n].remove(&wrapped);
							tracing::warn!(
								subscriptions = ids.len(),
								error = %e,
								"the row-level security check of a subscriber raised"
							);
							continue;
						}
					};
					// A position counts among the keys; `eligible` says which change each belongs to.
					let found: std::collections::HashSet<i32> = positions
						.iter()
						.filter_map(|p| eligible.get((*p as usize).wrapping_sub(1)).copied())
						.collect();
					for id in ids {
						let these = match passed.get(id) {
							Some((_, filtered)) => found.intersection(filtered).copied().collect(),
							None => found.clone(),
						};
						sees.insert(id.clone(), Sees::These(these));
					}
				}
			}
		}
	}
	for (idx, grp, payload, errors) in payloads {
		let ids: Vec<String> = groups
			.get(&grp)
			.map(|ids| {
				ids.iter()
					.filter(|id| match sees.get(*id) {
						Some(Sees::Every) => true,
						Some(Sees::These(s)) => s.contains(&idx),
						None => false,
					})
					.cloned()
					.collect()
			})
			.unwrap_or_default();
		if !ids.is_empty() {
			out.push((
				idx.max(1) as usize - 1,
				Decided {
					ids,
					payload,
					errors,
				},
			));
		}
	}
	out.sort_by_key(|(i, _)| *i);
	Ok(out)
}

/// The change as the pinned server encodes it, from the payload Postgres built.
fn record(payload: &Value, errors: Vec<String>) -> String {
	let field = |name: &str, absent: &str| {
		payload
			.get(name)
			.map(Value::to_string)
			.unwrap_or_else(|| absent.to_string())
	};
	let kind = payload
		.get("type")
		.and_then(Value::as_str)
		.unwrap_or_default();
	let errors = if errors.is_empty() {
		"null".to_string()
	} else {
		Value::from(errors).to_string()
	};
	let mut out = format!(
		"{{\"table\":{},\"type\":{},",
		field("table", "null"),
		Value::from(kind)
	);
	if kind != "DELETE" {
		out.push_str(&format!("\"record\":{},", field("record", "{}")));
	}
	out.push_str(&format!(
		"\"columns\":{},\"errors\":{errors},\"schema\":{},\"commit_timestamp\":{}",
		field("columns", "[]"),
		field("schema", "null"),
		field("commit_timestamp", "null")
	));
	if kind != "INSERT" {
		out.push_str(&format!(",\"old_record\":{}", field("old_record", "{}")));
	}
	out.push('}');
	out
}

/// Postgrex prints an error's condition name beside its code; the few a subscription meets.
fn condition_name(code: &str) -> &'static str {
	match code {
		"P0001" => "raise_exception",
		"42501" => "insufficient_privilege",
		"42P01" => "undefined_table",
		"42703" => "undefined_column",
		"22P02" => "invalid_text_representation",
		"54000" => "program_limit_exceeded",
		"23505" => "unique_violation",
		_ => "internal_error",
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use serde_json::json;

	#[test]
	fn filters_parse_as_the_pinned_server_does() {
		assert_eq!(
			parse_filters("subject=eq.hey").unwrap(),
			vec![("subject".into(), "eq".into(), "hey".into())]
		);
		assert_eq!(
			parse_filters("subject=in.(hidee,ho)").unwrap(),
			vec![("subject".into(), "in".into(), "{hidee,ho}".into())]
		);
		assert_eq!(parse_filters("id=gt.0,id=lt.100").unwrap().len(), 2);
		assert!(parse_filters("  ").unwrap().is_empty());
		assert_eq!(
			parse_filters("subject=like.hey").unwrap_err(),
			"Error parsing `filter` params: [\"like\", \"hey\"]"
		);
		assert_eq!(
			parse_filters("undefined").unwrap_err(),
			"Error parsing `filter` params: [\"undefined\"]"
		);
		assert!(parse_filters("a=eq.1,,b=eq.2").is_err());
	}

	#[test]
	fn a_binding_is_described_in_the_same_words() {
		let map = json!({"event": "INSERT", "schema": "public", "table": "rt_items", "filter": "room=eq.big"});
		let (action, schema, table, filters, selected) = parse(map.as_object().unwrap()).unwrap();
		let b = Binding {
			subscription_id: Uuid::nil(),
			id: 1,
			action,
			schema,
			table,
			filters,
			selected,
		};
		assert_eq!(
			describe(&b),
			"event: INSERT, schema: public, table: rt_items, filters: [{\"room\", \"eq\", \"big\"}], select: nil"
		);
	}

	#[test]
	fn the_id_is_stable_and_the_order_of_keys_does_not_matter() {
		let a = binding_id(&json!({"event": "*", "schema": "public", "table": "t"}));
		assert_eq!(
			a,
			binding_id(&json!({"table": "t", "schema": "public", "event": "*"}))
		);
		assert!(a < (1 << 27));
	}

	#[test]
	fn a_record_has_the_fields_of_its_kind() {
		let d = record(
			&json!({"schema": "public", "table": "t", "type": "DELETE", "columns": [],
				"old_record": {"id": 1}, "commit_timestamp": "2026-01-01T00:00:00Z"}),
			Vec::new(),
		);
		let v: Value = serde_json::from_str(&d).unwrap();
		assert!(v.get("record").is_none());
		assert_eq!(v["old_record"]["id"], 1);
		assert_eq!(v["errors"], Value::Null);
		// A refusal carries the head alone: the fields a client reads are still there.
		let e = record(
			&json!({"schema": "public", "table": "t", "type": "INSERT"}),
			vec!["Error 401: Unauthorized".into()],
		);
		let v: Value = serde_json::from_str(&e).unwrap();
		assert_eq!(v["record"], json!({}));
		assert_eq!(v["columns"], json!([]));
		assert_eq!(v["errors"][0], "Error 401: Unauthorized");
	}
}
