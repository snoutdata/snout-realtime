//! Realtime handlers: an event on a channel (or a Socket.IO emit) runs a SQL function the
//! project registered with `realtime.on()`, AS the caller, and what it returns is the reply.
//!
//! The function runs in one transaction, as the subscriber (the same role, claims and headers the
//! authorisation probes set, so `auth.uid()` and row-level security apply), with a statement
//! timeout, on a pool of its own. What it asks for with `realtime.emit()`, `realtime.join()` and
//! `realtime.leave()` is read back from a transaction-local setting before the commit and acted on
//! only after it, so a handler that fails sends nothing. The user's guide is
//! https://docs.snoutdata.com/stack/realtime#handlers-your-code-on-a-message.

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use crate::App;
use crate::db::{self, AuthContext, DbError};
use crate::hub::{self, TenantRt};

/// How long a handler table read is trusted before it is read again. A registration takes
/// effect within this long (H8).
pub const FRESH_FOR: Duration = Duration::from_secs(2);
/// How long the first read may hold up a join or a connection.
const FIRST_LOAD: Duration = Duration::from_secs(2);
/// The longest one handler call may take.
pub const STATEMENT_TIMEOUT: &str = "5s";

/// One registration: `event` on `topic` runs `function` (schema-qualified and quoted).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handler {
	pub event: String,
	pub topic: String,
	pub function: String,
}

/// The handler for `event` on `topic`, if any (H2): an exact topic first, then the longest
/// matching prefix (`room:*`), then `*`.
pub fn find<'a>(list: &'a [Handler], event: &str, topic: &str) -> Option<&'a Handler> {
	list.iter()
		.filter(|h| h.event == event)
		.filter_map(|h| rank(&h.topic, topic).map(|r| (r, h)))
		.max_by_key(|(r, _)| *r)
		.map(|(_, h)| h)
}

/// How well a registered topic pattern matches a topic; higher is better, `None` is no match.
fn rank(pattern: &str, topic: &str) -> Option<usize> {
	if pattern == topic {
		return Some(usize::MAX);
	}
	let prefix = pattern.strip_suffix('*')?;
	topic.starts_with(prefix).then_some(prefix.len())
}

/// A project's handler table as last read.
#[derive(Default)]
pub struct Cache {
	state: std::sync::Mutex<State>,
}

#[derive(Default)]
struct State {
	list: Arc<Vec<Handler>>,
	read_at: Option<Instant>,
	reading: bool,
}

impl Cache {
	fn lock(&self) -> std::sync::MutexGuard<'_, State> {
		self.state.lock().unwrap_or_else(|e| e.into_inner())
	}
}

/// The project's handlers. Never read before: read now, waiting at most `FIRST_LOAD`. Read but
/// stale: the stale list is answered and a read is started behind it, so a broadcast is never
/// held up by the table once a project has been read (a project without a reachable database
/// simply has no handlers).
pub async fn current(app: &Arc<App>, rt: &Arc<TenantRt>) -> Arc<Vec<Handler>> {
	let (list, read_at, reading) = {
		let s = rt.handlers.lock();
		(s.list.clone(), s.read_at, s.reading)
	};
	match read_at {
		Some(at) if at.elapsed() < FRESH_FOR => list,
		Some(_) => {
			if !reading {
				rt.handlers.lock().reading = true;
				let (app, rt) = (app.clone(), rt.clone());
				tokio::spawn(async move { refresh(&app, &rt).await });
			}
			list
		}
		None => {
			let _ = tokio::time::timeout(FIRST_LOAD, refresh(app, rt)).await;
			rt.handlers.lock().list.clone()
		}
	}
}

/// Start reading the project's handlers if they have never been read, without waiting: a join
/// calls it, so the first broadcast on a channel finds them already there.
pub fn warm(app: &Arc<App>, rt: &Arc<TenantRt>) {
	{
		let mut s = rt.handlers.lock();
		if s.read_at.is_some() || s.reading {
			return;
		}
		s.reading = true;
	}
	let (app, rt) = (app.clone(), rt.clone());
	tokio::spawn(async move { refresh(&app, &rt).await });
}

async fn refresh(app: &Arc<App>, rt: &Arc<TenantRt>) {
	let read = read(app, rt).await;
	let mut s = rt.handlers.lock();
	s.reading = false;
	s.read_at = Some(Instant::now());
	match read {
		Ok(list) => s.list = Arc::new(list),
		Err(e) => tracing::debug!(tenant = %rt.id, error = %e, "reading realtime.handlers"),
	}
}

async fn read(app: &Arc<App>, rt: &Arc<TenantRt>) -> Result<Vec<Handler>, String> {
	let pool = pool(app, rt).await?;
	let names = pool.names().await.map_err(|e| e.to_string())?;
	let client = pool.get().await.map_err(|e| e.to_string())?;
	let rows = client
		.query(
			&*names.sql(
				"select h.event, h.topic, quote_ident(n.nspname) || '.' || quote_ident(p.proname)
			 from realtime.handlers h
			 join pg_proc p on p.oid = h.handler::oid
			 join pg_namespace n on n.oid = p.pronamespace",
			),
			&[],
		)
		.await
		.map_err(|e| DbError::from_pg(e).to_string())?;
	Ok(rows
		.into_iter()
		.map(|r| Handler {
			event: r.get(0),
			topic: r.get(1),
			function: r.get(2),
		})
		.collect())
}

/// The handler pool for a project, after its schema is ready.
pub async fn pool(app: &Arc<App>, rt: &Arc<TenantRt>) -> Result<crate::db::Pool, String> {
	crate::socket::project_pool(app, rt).await?;
	let tenant = rt.tenant();
	let database = tenant.database.clone().ok_or(
		"UnableToConnectToProject: Realtime was unable to connect to the project database",
	)?;
	app.dbs
		.handler_pool(&tenant.external_id, &database)
		.await
		.map_err(|e| format!("UnableToConnectToProject: {e}"))
}

/// What one handler call is given.
pub struct Call<'a> {
	/// Who is calling, and the topic (a channel's name, or a Socket.IO namespace).
	pub auth: &'a AuthContext,
	pub event: &'a str,
	pub payload: &'a Value,
	/// Every argument (Socket.IO); `[payload]` on a channel.
	pub args: &'a Value,
	/// The Socket.IO socket, when the call came through that door.
	pub socket_id: Option<&'a str>,
}

/// What the handler asked for besides its result, in the order it asked.
#[derive(Debug, Clone, PartialEq)]
pub enum Op {
	Emit {
		topic: String,
		event: String,
		payload: Value,
		except_self: bool,
	},
	Join(String),
	Leave(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Outcome {
	pub result: Value,
	pub ops: Vec<Op>,
}

/// A handler that did not complete.
#[derive(Debug, Clone, PartialEq)]
pub struct Failure {
	/// `unauthorized`, `timeout`, `unavailable`, or the SQLSTATE the handler raised.
	pub code: String,
	pub message: String,
}

impl Failure {
	/// The reply a client is given.
	pub fn to_json(&self) -> Value {
		json!({ "error": { "code": self.code, "message": self.message } })
	}

	fn from_db(e: DbError) -> Failure {
		match e {
			DbError::Unavailable(m) => Failure {
				code: "unavailable".into(),
				message: format!("Realtime was unable to reach the project database: {m}"),
			},
			DbError::Sql { code, message } => {
				let code = match code.as_str() {
					"42501" => "unauthorized".to_string(),
					"57014" => "timeout".to_string(),
					_ => code,
				};
				Failure { code, message }
			}
		}
	}
}

/// Run `handler` for `call`. On success the transaction has committed.
pub async fn invoke(
	pool: &crate::db::Pool,
	handler: &Handler,
	call: &Call<'_>,
) -> Result<Outcome, Failure> {
	let names = pool.names().await.map_err(Failure::from_db)?;
	let mut client = pool
		.get()
		.await
		.map_err(|e| Failure::from_db(DbError::Unavailable(e.to_string())))?;
	let tx = client
		.transaction()
		.await
		.map_err(|e| Failure::from_db(DbError::from_pg(e)))?;
	db::become_subscriber(&tx, call.auth, &names)
		.await
		.map_err(Failure::from_db)?;
	let args = call.args.to_string();
	tx.execute(
		"select set_config('statement_timeout', $1, true), set_config('realtime.event', $2, true),
		 set_config('realtime.args', $3, true), set_config('realtime.socket_id', $4, true),
		 set_config('realtime.out', '', true)",
		&[
			&STATEMENT_TIMEOUT,
			&call.event,
			&args,
			&call.socket_id.unwrap_or_default(),
		],
	)
	.await
	.map_err(|e| Failure::from_db(DbError::from_pg(e)))?;
	// `::text` so a `void` handler is the empty string rather than an error.
	let sql = format!("select ({}($1::jsonb))::text", handler.function);
	let row = tx
		.query_one(&sql, &[call.payload])
		.await
		.map_err(|e| Failure::from_db(DbError::from_pg(e)))?;
	let result = parse_result(row.get::<_, Option<String>>(0).as_deref());
	let out: Option<String> = tx
		.query_one("select current_setting('realtime.out', true)", &[])
		.await
		.map_err(|e| Failure::from_db(DbError::from_pg(e)))?
		.get(0);
	tx.commit()
		.await
		.map_err(|e| Failure::from_db(DbError::from_pg(e)))?;
	Ok(Outcome {
		result,
		ops: parse_ops(out.as_deref().unwrap_or_default()),
	})
}

/// A handler's `::text` result as JSON: a `void` (or null) result is `null`.
pub fn parse_result(text: Option<&str>) -> Value {
	match text {
		None | Some("") => Value::Null,
		Some(t) => serde_json::from_str(t).unwrap_or(Value::Null),
	}
}

/// What `realtime.emit/join/leave` appended to `realtime.out`. Anything malformed is skipped:
/// the setting is the handler's own to write.
pub fn parse_ops(text: &str) -> Vec<Op> {
	let Ok(Value::Array(list)) = serde_json::from_str::<Value>(text) else {
		return Vec::new();
	};
	let s = |v: &Value, k: &str| v.get(k).and_then(Value::as_str).map(str::to_string);
	list.iter()
		.filter_map(|op| match op.get("op").and_then(Value::as_str)? {
			"emit" => Some(Op::Emit {
				topic: s(op, "topic")?,
				event: s(op, "event")?,
				payload: op.get("payload").cloned().unwrap_or(Value::Null),
				except_self: op
					.get("except_self")
					.and_then(Value::as_bool)
					.unwrap_or(false),
			}),
			"join" => s(op, "topic").map(Op::Join),
			"leave" => s(op, "topic").map(Op::Leave),
			_ => None,
		})
		.filter(|op| match op {
			Op::Emit { topic, event, .. } => !topic.is_empty() && !event.is_empty(),
			Op::Join(t) | Op::Leave(t) => !t.is_empty(),
		})
		.collect()
}

/// Send an emit to everyone on `topic`, its public and private subscribers both (the handler is
/// the project's own server code and has already decided), leaving out `except_socket`.
pub fn deliver_emit(
	rt: &TenantRt,
	topic: &str,
	event: &str,
	payload: Value,
	except_socket: Option<u64>,
) -> usize {
	let message = json!({ "type": "broadcast", "event": event, "payload": payload });
	hub::deliver_json_to_name(rt, topic, except_socket, message)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn h(event: &str, topic: &str, f: &str) -> Handler {
		Handler {
			event: event.into(),
			topic: topic.into(),
			function: f.into(),
		}
	}

	#[test]
	fn an_exact_topic_beats_a_prefix_and_a_longer_prefix_beats_a_shorter() {
		let list = vec![
			h("send", "*", "any"),
			h("send", "room:*", "rooms"),
			h("send", "room:vip:*", "vip"),
			h("send", "room:1", "one"),
			h("other", "room:1", "other"),
		];
		assert_eq!(find(&list, "send", "room:1").unwrap().function, "one");
		assert_eq!(find(&list, "send", "room:vip:2").unwrap().function, "vip");
		assert_eq!(find(&list, "send", "room:2").unwrap().function, "rooms");
		assert_eq!(find(&list, "send", "lobby").unwrap().function, "any");
		assert!(find(&list, "nothing", "room:1").is_none());
	}

	#[test]
	fn a_pattern_without_a_star_matches_only_itself() {
		let list = vec![h("send", "room", "f")];
		assert!(find(&list, "send", "room:1").is_none());
		assert!(find(&list, "send", "room").is_some());
	}

	#[test]
	fn a_void_or_null_result_is_null() {
		assert_eq!(parse_result(None), Value::Null);
		assert_eq!(parse_result(Some("")), Value::Null);
		assert_eq!(parse_result(Some("{\"ok\": true}")), json!({ "ok": true }));
		assert_eq!(parse_result(Some("\"text\"")), json!("text"));
	}

	#[test]
	fn ops_come_back_in_order_and_malformed_ones_are_skipped() {
		let text = r#"[
			{"op":"join","topic":"room:1"},
			{"op":"emit","topic":"room:1","event":"msg","payload":{"a":1},"except_self":true},
			{"op":"emit","topic":"","event":"msg"},
			{"op":"emit","topic":"room:1"},
			{"op":"wat"},
			{"op":"leave","topic":"room:2"}
		]"#;
		assert_eq!(
			parse_ops(text),
			vec![
				Op::Join("room:1".into()),
				Op::Emit {
					topic: "room:1".into(),
					event: "msg".into(),
					payload: json!({ "a": 1 }),
					except_self: true,
				},
				Op::Leave("room:2".into()),
			]
		);
		assert!(parse_ops("").is_empty());
		assert!(parse_ops("not json").is_empty());
		assert!(parse_ops("{}").is_empty());
	}

	#[test]
	fn a_failure_names_permission_and_timeout_plainly() {
		let f = Failure::from_db(DbError::Sql {
			code: "42501".into(),
			message: "permission denied for function send".into(),
		});
		assert_eq!(f.code, "unauthorized");
		let f = Failure::from_db(DbError::Sql {
			code: "57014".into(),
			message: "canceling statement due to statement timeout".into(),
		});
		assert_eq!(f.code, "timeout");
		let f = Failure::from_db(DbError::Sql {
			code: "P0001".into(),
			message: "room is full".into(),
		});
		assert_eq!(
			f.to_json(),
			json!({ "error": { "code": "P0001", "message": "room is full" } })
		);
	}
}
