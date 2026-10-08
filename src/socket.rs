//! One WebSocket: the Phoenix socket protocol, and the channels joined on it.
//!
//! The socket is one task. It reads the client's frames, runs each channel's join, leave,
//! broadcast, presence and token refresh IN ORDER (so replies come back in the order asked),
//! and writes whatever the hub queues for it, serialised for this socket's `vsn`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::extract::ws::{Message as WsMessage, WebSocket};
use futures_util::{Sink, SinkExt, StreamExt};
use serde_json::{Map, Value, json};
use uuid::Uuid;

use crate::App;
use crate::changes::{self, Binding};
use crate::db::{self, AuthContext, Policies};
use crate::hub::{self, Inbox, Out, Outbox, Sub, TenantRt, TopicKey};
use crate::inspect::{self, Event, Kind};
use crate::jwt;
use crate::protocol::{self, Frame, Inbound, InboundPayload, Vsn};

/// How often a channel re-checks its token, at most (the pinned server's five minutes).
const CONFIRM_TOKEN_EVERY: Duration = Duration::from_secs(300);
/// Client presence calls allowed per window before the channel is shut (the pinned server's).
const PRESENCE_CALLS: u32 = 5;
const PRESENCE_WINDOW: Duration = Duration::from_secs(30);
/// A socket with no frame from its client for this long is closed: Phoenix's websocket default,
/// and realtime-js heartbeats every 25 seconds.
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// How long one frame may take to be written before the client is taken to have stopped
/// reading (the `send_timeout` the pinned server's listener has).
pub const WRITE_TIMEOUT: Duration = Duration::from_secs(30);
/// The largest frame, and message, read from a client: the pinned server's `max_frame_size`.
/// Without it tungstenite reads up to 64 MiB into memory per frame.
pub const MAX_FRAME_SIZE: usize = 5_000_000;

/// The largest payload a project allows, in bytes: its `max_payload_size_in_kb` plus the pinned
/// server's 500 bytes of padding.
pub fn payload_limit(max_payload_size_in_kb: i64) -> i64 {
	max_payload_size_in_kb
		.saturating_mul(1000)
		.saturating_add(500)
}

/// Whether a `track` payload is over the project's payload limit, measured as it is serialised.
fn presence_too_large(body: &Map<String, Value>, max_payload_size_in_kb: i64) -> bool {
	let size = serde_json::to_vec(body)
		.map(|v| v.len())
		.unwrap_or(usize::MAX);
	size as i64 > payload_limit(max_payload_size_in_kb)
}

/// Whether a channel may see presence (the state, and every diff after it). A private channel
/// needs its presence read policy; a public one always may.
fn can_read_presence(private: bool, policies: &Policies) -> bool {
	!private || policies.presence_read == Some(true)
}

/// Whether a join asks for presence. `config.presence.enabled` decides when it is there; a
/// `presence` object WITHOUT it (an older realtime-js, or a client written by hand to the wire
/// protocol) is taken as asking, so the members already on the topic are sent on join rather
/// than only after the first `track`. Current realtime-js always sends `enabled` (D19).
fn presence_asked(config: &Value) -> bool {
	match config.get("presence") {
		Some(Value::Object(p)) => p.get("enabled").is_none_or(|e| e.as_bool() == Some(true)),
		_ => false,
	}
}

/// The ref a channel is known by. V1 has no `join_ref` slot, so a V1 client written by hand
/// sends none, and its join's own `ref` stands in (realtime-js sends both, and they are equal).
/// The server's `phx_close` and `system` carry it, which is how a client tells a close for a
/// channel it already replaced from a close for the replacement.
fn channel_join_ref(vsn: Vsn, m: &Inbound) -> Option<String> {
	match vsn {
		Vsn::V1 => m.join_ref.clone().or_else(|| m.reference.clone()),
		Vsn::V2 => m.join_ref.clone(),
	}
}

/// A `system` message on a channel. V2 carries the join ref in its own slot; V1 has only `ref`,
/// so it rides there.
fn system_frame(vsn: Vsn, topic: &str, join_ref: &Option<String>, payload: Value) -> Frame {
	let reference = match vsn {
		Vsn::V1 => join_ref.clone(),
		Vsn::V2 => None,
	};
	protocol::message(vsn, join_ref, &reference, topic, "system", payload)
}

/// How many server-closed topics a socket remembers the reason for.
const CLOSED_REMEMBERED: usize = 64;

/// The topics the server closed on this socket, and why, so a message that arrives on one after
/// the close is told why it has no channel, not just that it has none. Bounded: the oldest is
/// forgotten first. A topic is forgotten when it is joined or left again.
#[derive(Default)]
struct Closed(std::collections::VecDeque<(String, String)>);

impl Closed {
	fn remember(&mut self, topic: &str, reason: &str) {
		self.forget(topic);
		if self.0.len() >= CLOSED_REMEMBERED {
			self.0.pop_front();
		}
		self.0.push_back((topic.to_string(), reason.to_string()));
	}

	fn forget(&mut self, topic: &str) {
		self.0.retain(|(t, _)| t != topic);
	}

	fn why(&self, topic: &str) -> Option<&str> {
		self.0
			.iter()
			.find(|(t, _)| t == topic)
			.map(|(_, r)| r.as_str())
	}
}

/// The reply to a message on a topic this socket has no channel for. `reason` stays the string
/// Phoenix sends; a topic the server closed also says why, since a client flooding a closed
/// channel is otherwise sent one bare `unmatched topic` per message with nothing tying them to the
/// `system` message that explained the close.
fn unmatched(closed: Option<&str>) -> Value {
	match closed {
		Some(why) => json!({
			"reason": "unmatched topic",
			"message": format!("channel closed by the server: {why}. Join it again, or open a new socket."),
		}),
		None => json!({ "reason": "unmatched topic" }),
	}
}

/// Whether a channel is sent a hub broadcast of `event`.
fn may_receive(private: bool, policies: &Policies, event: &str) -> bool {
	event != "presence_diff" || can_read_presence(private, policies)
}

pub fn now_secs() -> i64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map(|d| d.as_secs() as i64)
		.unwrap_or(0)
}

/// What a connection was accepted with.
pub struct Accepted {
	pub rt: Arc<TenantRt>,
	pub vsn: Vsn,
	/// The token the socket connected with (the project's key, usually).
	pub token: String,
	/// The upgrade request's `x-` headers, which policies read as `request.headers`.
	pub headers: Map<String, Value>,
	pub address: Option<String>,
}

struct Channel {
	join_ref: Option<String>,
	name: String,
	key: TopicKey,
	sub: u64,
	private: bool,
	self_broadcast: bool,
	ack: bool,
	presence_enabled: bool,
	presence_key: String,
	presence_payload: Option<Map<String, Value>>,
	presence_calls: (u32, Option<Instant>),
	policies: Policies,
	access_token: String,
	auth: AuthContext,
	bindings: Vec<Binding>,
	/// When the token must be looked at again.
	confirm_at: Instant,
}

struct Socket {
	app: Arc<App>,
	rt: Arc<TenantRt>,
	id: u64,
	vsn: Vsn,
	token: String,
	headers: Map<String, Value>,
	tx: Outbox,
	channels: HashMap<String, Channel>,
	/// Topics the server closed, and why.
	closed: Closed,
	/// Frames waiting to go out.
	out: Vec<Frame>,
}

pub async fn serve(app: Arc<App>, ws: WebSocket, accepted: Accepted) {
	let (tx, mut rx) = hub::outbox();
	let id = app.hub.next_id();
	let seen = accepted
		.rt
		.socket_opened(id, accepted.address.clone(), tx.clone());
	accepted.rt.log.push(Event::new(Kind::Connect).socket(id));
	// How the socket ended, for the project's log. Set where the loop is left.
	let mut ended = "connection lost (no close frame)".to_string();
	let mut s = Socket {
		app,
		rt: accepted.rt,
		id,
		vsn: accepted.vsn,
		token: accepted.token,
		headers: accepted.headers,
		tx,
		channels: HashMap::new(),
		closed: Closed::default(),
		out: Vec::new(),
	};
	let (mut sink, mut stream) = ws.split();
	let mut last_inbound = Instant::now();
	loop {
		let next_confirm = s.channels.values().map(|c| c.confirm_at).min();
		let idle_at = last_inbound + IDLE_TIMEOUT;
		let wait = async {
			match next_confirm {
				Some(at) => tokio::time::sleep_until(at.into()).await,
				None => std::future::pending::<()>().await,
			}
		};
		tokio::select! {
			incoming = stream.next() => {
				if matches!(incoming, Some(Ok(_))) {
					seen.store(inspect::now_ms(), Ordering::Relaxed);
					last_inbound = Instant::now();
				}
				match incoming {
					Some(Ok(WsMessage::Text(text))) => {
						match protocol::decode_text(s.vsn, text.as_str()) {
							Ok(m) => s.handle(m).await,
							Err(e) => tracing::debug!(error = %e, "an undecodable frame"),
						}
					}
					Some(Ok(WsMessage::Binary(bytes))) if s.vsn == Vsn::V2 => {
						match protocol::decode_binary(&bytes) {
							Ok(m) => s.handle(m).await,
							Err(e) => tracing::debug!(error = %e, "an undecodable frame"),
						}
					}
					Some(Ok(WsMessage::Close(frame))) => {
						ended = match frame {
							Some(f) if !f.reason.is_empty() => {
								format!("client closed ({}: {})", f.code, f.reason.as_str())
							}
							Some(f) => format!("client closed ({})", f.code),
							None => "client closed".to_string(),
						};
						break;
					}
					None => break,
					Some(Err(e)) => {
						ended = format!("connection error: {e}");
						break;
					}
					Some(Ok(_)) => {}
				}
			}
			out = rx.recv() => {
				if rx.overflowed() {
					let (n, bytes) = rx.queued();
					ended = format!(
						"the client fell too far behind ({n} messages, {bytes} bytes waiting)"
					);
					break;
				}
				match out {
					Some(Out::Disconnect) | None => {
						ended = "closed by the server (the project's Realtime settings were reset)".to_string();
						break;
					}
					Some(o) => s.deliver(o).await,
				}
			}
			_ = wait => s.confirm_tokens().await,
			_ = tokio::time::sleep_until(idle_at.into()) => {
				ended = format!("no frame from the client for {} s", IDLE_TIMEOUT.as_secs());
				break;
			}
		}
		let written = write_frames(
			&mut sink,
			s.out.drain(..),
			&s.rt.counters.output_bytes,
			WRITE_TIMEOUT,
		)
		.await;
		if !written {
			ended = "the client could not be written to".to_string();
			break;
		}
	}
	s.close_all().await;
	s.rt.socket_closed(s.id);
	s.rt.log
		.push(Event::new(Kind::Disconnect).socket(s.id).reason(ended));
	// A client that stopped reading would park the close here for good, as it parked the writes.
	let _ = tokio::time::timeout(WRITE_TIMEOUT, sink.close()).await;
	drain(&mut rx);
}

fn drain(rx: &mut Inbox) {
	while rx.try_recv().is_some() {}
}

/// Write frames in order, each within `timeout`; false when one could not be written (the
/// client is gone, or has stopped reading).
async fn write_frames<S>(
	sink: &mut S,
	frames: impl IntoIterator<Item = Frame>,
	output_bytes: &AtomicU64,
	timeout: Duration,
) -> bool
where
	S: Sink<WsMessage> + Unpin,
{
	for frame in frames {
		let (msg, len) = match frame {
			Frame::Text(t) => {
				let n = t.len();
				(WsMessage::Text(t.into()), n)
			}
			Frame::Binary(b) => {
				let n = b.len();
				(WsMessage::Binary(b.into()), n)
			}
		};
		output_bytes.fetch_add(len as u64, Ordering::Relaxed);
		match tokio::time::timeout(timeout, sink.send(msg)).await {
			Ok(Ok(())) => {}
			_ => return false,
		}
	}
	true
}

impl Socket {
	fn push(&mut self, frame: Frame) {
		self.out.push(frame);
	}

	fn reply(&mut self, m: &Inbound, status: &str, response: Value) {
		let f = protocol::reply(
			self.vsn,
			&m.join_ref,
			&m.reference,
			&m.topic,
			status,
			response,
		);
		self.push(f);
	}

	fn system(
		&mut self,
		topic: &str,
		join_ref: &Option<String>,
		extension: &str,
		status: &str,
		message: &str,
		name: &str,
	) {
		let payload = json!({ "message": message, "status": status, "extension": extension, "channel": name });
		let f = system_frame(self.vsn, topic, join_ref, payload);
		self.push(f);
	}

	fn close_frame(&mut self, topic: &str, join_ref: &Option<String>) {
		let f = protocol::message(self.vsn, join_ref, join_ref, topic, "phx_close", json!({}));
		self.push(f);
	}

	async fn handle(&mut self, m: Inbound) {
		if m.topic == "phoenix" && m.event == "heartbeat" {
			self.reply(&m, "ok", json!({}));
			return;
		}
		match m.event.as_str() {
			"phx_join" => self.join(m).await,
			"phx_leave" => {
				self.closed.forget(&m.topic);
				if let Some(ch) = self.channels.remove(&m.topic) {
					self.rt.log.push(
						Event::new(Kind::Leave)
							.socket(self.id)
							.channel(&ch.name)
							.presence_key(&ch.presence_key),
					);
					// The leave's own join ref, or the channel's when it sent none (V1 by hand).
					let join_ref = m.join_ref.clone().or_else(|| ch.join_ref.clone());
					self.cleanup(ch).await;
					self.reply(&m, "ok", json!({}));
					self.close_frame(&m.topic.clone(), &join_ref);
				} else {
					self.reply(&m, "ok", json!({}));
				}
			}
			_ if !self.channels.contains_key(&m.topic) => {
				let response = unmatched(self.closed.why(&m.topic));
				self.reply(&m, "error", response);
			}
			"broadcast" => self.broadcast(m).await,
			"presence" => self.presence(m).await,
			"access_token" => self.access_token(m).await,
			_ => {}
		}
	}

	// --- join -----------------------------------------------------------------------------

	async fn join(&mut self, m: Inbound) {
		self.closed.forget(&m.topic);
		if let Some(old) = self.channels.remove(&m.topic) {
			// A second join on a joined topic replaces the first, as Phoenix does. Logged, since a
			// client rejoining in a loop is otherwise a run of joins with nothing between them.
			let old_ref = old.join_ref.clone();
			self.rt.log.push(
				Event::new(Kind::ChannelClosed)
					.socket(self.id)
					.channel(&old.name)
					.presence_key(&old.presence_key)
					.reason("replaced by a new join on the same topic"),
			);
			self.cleanup(old).await;
			self.close_frame(&m.topic.clone(), &old_ref);
		}
		match self.try_join(&m).await {
			Ok((channel, response, after)) => {
				self.rt.log.push(
					Event::new(Kind::Join)
						.socket(self.id)
						.channel(&channel.name)
						.presence_key(&channel.presence_key),
				);
				self.reply(&m, "ok", response);
				let name = channel.name.clone();
				let join_ref = channel.join_ref.clone();
				let presence = channel.presence_enabled
					&& can_read_presence(channel.private, &channel.policies);
				let key = channel.key.clone();
				let bindings = channel.bindings.clone();
				let claims = channel.auth.claims.clone();
				self.channels.insert(m.topic.clone(), channel);
				self.rt.joined(self.id);
				self.ensure_messages().await;
				for f in after {
					self.push(f);
				}
				if presence {
					let state = self.rt.presence_state(&key);
					let f = protocol::message(
						self.vsn,
						&join_ref,
						&None,
						&m.topic,
						"presence_state",
						state,
					);
					self.push(f);
				}
				if !bindings.is_empty() {
					self.subscribe_changes(&m.topic, &name, &join_ref, &claims, &bindings)
						.await;
				}
			}
			Err(reason) => {
				let mut event = Event::new(Kind::JoinRefused)
					.socket(self.id)
					.reason(reason.clone());
				if let Some(name) = m.topic.strip_prefix("realtime:") {
					event = event.channel(name);
				}
				self.rt.log.push(event);
				self.reply(&m, "error", json!({ "reason": reason }));
			}
		}
	}

	async fn try_join(&mut self, m: &Inbound) -> Result<(Channel, Value, Vec<Frame>), String> {
		let Some(name) = m.topic.strip_prefix("realtime:") else {
			return Err("unmatched topic".into());
		};
		if name.is_empty() {
			return Err("TopicNameRequired: You must provide a topic name".into());
		}
		let InboundPayload::Json(params) = &m.payload else {
			return Err("InvalidJoinPayload: the join payload must be JSON".into());
		};
		let config = params.get("config").cloned().unwrap_or(Value::Null);
		let private = config
			.pointer("/private")
			.and_then(Value::as_bool)
			.unwrap_or(false);
		let tenant = self.rt.tenant();
		let presence_enabled = presence_asked(&config) || tenant.presence_enabled;

		let access_token = match params
			.get("access_token")
			.or_else(|| params.get("user_token"))
			.and_then(Value::as_str)
		{
			Some(t) if t.starts_with("sb_") => self.token.clone(),
			Some(t) => t.to_string(),
			None => self.token.clone(),
		};

		if tenant.private_only && !private {
			return Err("PrivateOnly: This project only allows private channels".into());
		}
		let (counted, users) = self.rt.census(self.id);
		if !counted && users as i64 >= tenant.max_concurrent_users {
			return Err("ConnectionRateLimitReached: Too many connected users".into());
		}
		if self.rt.joins.add(1, tenant.max_joins_per_second) {
			return Err("ClientJoinRateLimitReached: Too many joins per second".into());
		}
		self.rt.counters.joins.fetch_add(1, Ordering::Relaxed);
		if self.channels.len() as i64 >= tenant.max_channels_per_client {
			return Err("ChannelRateLimitReached: Too many channels".into());
		}
		let claims = jwt::authorize(&access_token, &tenant.jwt_secret, now_secs())
			.map_err(|e| e.reason())?;
		let auth = AuthContext {
			topic: name.to_string(),
			role: claims
				.get("role")
				.and_then(Value::as_str)
				.unwrap_or_default()
				.to_string(),
			sub: claims
				.get("sub")
				.and_then(Value::as_str)
				.map(str::to_string),
			claims: claims.clone(),
			headers: self.headers.clone(),
		};

		let mut policies = Policies::default();
		let needs_db = private || config.pointer("/broadcast/replay").is_some();
		let pool = if needs_db {
			Some(self.pool().await?)
		} else {
			None
		};
		if private {
			let pool = pool.as_ref().ok_or(
				"UnableToConnectToProject: Realtime was unable to connect to the project database",
			)?;
			policies = match db::authorize_read(pool, &auth, presence_enabled, policies).await {
				Ok(p) => p,
				Err(db::DbError::Sql { code, .. })
					if code == "22023" || code == "P0001" || code.starts_with("42") =>
				{
					return Err(format!(
						"Unauthorized: You do not have permissions to read from this Channel topic: {name}"
					));
				}
				Err(e) => return Err(format!("UnableToSetPolicies: {e}")),
			};
			if policies.broadcast_read == Some(false) {
				return Err(format!(
					"Unauthorized: You do not have permissions to read from this Channel topic: {name}"
				));
			}
		}

		let mut after = Vec::new();
		let mut replayed = HashSet::new();
		if let Some(replay) = config.pointer("/broadcast/replay") {
			if !private {
				return Err(
					"UnableToReplayMessages: Replay is not allowed for public channels".into(),
				);
			}
			let since = replay.get("since").and_then(Value::as_i64);
			let limit = match replay.get("limit") {
				None | Some(Value::Null) => Some(25),
				Some(v) => v.as_i64(),
			};
			let (Some(since), Some(limit)) = (since, limit) else {
				return Err("UnableToReplayMessages: Replay params are not valid".into());
			};
			let pool = pool
				.as_ref()
				.ok_or("UnableToReplayMessages: Realtime was unable to replay messages")?;
			let messages = db::replay(pool, name, since, limit).await.map_err(|_| {
				"UnableToReplayMessages: Realtime was unable to replay messages".to_string()
			})?;
			for msg in messages {
				replayed.insert(msg.id.clone());
				let payload = json!({ "payload": msg.payload, "event": msg.event, "type": "broadcast", "meta": { "replayed": true, "id": msg.id } });
				after.push(protocol::message(
					self.vsn,
					&m.join_ref,
					&None,
					&m.topic,
					"broadcast",
					payload,
				));
			}
		}

		// postgres_changes bindings, each with the id the client will match by.
		let mut bindings = Vec::new();
		let mut described = Vec::new();
		if let Some(Value::Array(list)) = config.get("postgres_changes") {
			for p in list.iter().filter(|p| !p.is_null()) {
				let id = changes::binding_id(p);
				let mut with_id = p.as_object().cloned().unwrap_or_default();
				with_id.insert("id".into(), Value::from(id));
				described.push(Value::Object(with_id));
				bindings.push((p.clone(), id));
			}
		}
		let bindings = bindings
			.into_iter()
			.map(|(p, id)| {
				let map = p.as_object().cloned().unwrap_or_default();
				match changes::parse(&map) {
					Ok((action, schema, table, filters, selected)) => Ok(Binding {
						subscription_id: Uuid::new_v4(),
						id,
						action,
						schema,
						table,
						filters,
						selected,
					}),
					Err(e) => Err(e),
				}
			})
			.collect::<Vec<_>>();
		let mut parsed = Vec::new();
		let mut parse_error = None;
		for b in bindings {
			match b {
				Ok(b) => parsed.push(b),
				Err(e) => parse_error = Some(e),
			}
		}
		if let Some(e) = parse_error {
			after.push(system_frame(
				self.vsn,
				&m.topic,
				&channel_join_ref(self.vsn, m),
				json!({ "message": e, "status": "error", "extension": "postgres_changes", "channel": name }),
			));
			parsed.clear();
		}

		let presence_key = match config.pointer("/presence/key").and_then(Value::as_str) {
			Some(k) if !k.is_empty() => k.to_string(),
			_ => Uuid::now_v1(&[0, 0, 0, 0, 0, 1]).to_string(),
		};
		let key = TopicKey {
			name: name.to_string(),
			private,
		};
		let sub = self.app.hub.next_id();
		self.rt.subscribe(
			&key,
			Sub {
				id: sub,
				socket: self.id,
				join_topic: m.topic.clone(),
				tx: self.tx.clone(),
				replayed: Arc::new(replayed),
				presence_key: Arc::from(presence_key.as_str()),
				joined_at: inspect::now_ms(),
			},
		);
		let exp = claims.get("exp").and_then(Value::as_i64).unwrap_or(0);
		let confirm_at = Instant::now()
			+ Duration::from_secs((exp - now_secs()).max(0) as u64).min(CONFIRM_TOKEN_EVERY);
		let channel = Channel {
			join_ref: channel_join_ref(self.vsn, m),
			name: name.to_string(),
			key,
			sub,
			private,
			self_broadcast: config
				.pointer("/broadcast/self")
				.and_then(Value::as_bool)
				.unwrap_or(false),
			ack: config
				.pointer("/broadcast/ack")
				.and_then(Value::as_bool)
				.unwrap_or(false),
			presence_enabled,
			presence_key,
			presence_payload: None,
			presence_calls: (0, None),
			policies,
			access_token,
			auth,
			bindings: parsed,
			confirm_at,
		};
		Ok((channel, json!({ "postgres_changes": described }), after))
	}

	/// Broadcast from the database runs while a project has sockets, started by its first join.
	async fn ensure_messages(&self) {
		let tenant = self.rt.tenant();
		let Some(database) = tenant.database.clone() else {
			return;
		};
		let streams = self.rt.streams.lock().await;
		if streams.messages.as_ref().is_some_and(|t| !t.is_finished()) {
			return;
		}
		drop(streams);
		let Ok(pool) = self.pool().await else { return };
		let rt = self.rt.clone();
		let task = tokio::spawn(crate::messages::run(rt, pool, database));
		self.rt.streams.lock().await.messages = Some(task);
	}

	async fn pool(&self) -> Result<deadpool_postgres::Pool, String> {
		let tenant = self.rt.tenant();
		let Some(database) = tenant.database.clone() else {
			return Err(
				"UnableToConnectToProject: Realtime was unable to connect to the project database"
					.into(),
			);
		};
		let pool = self
			.app
			.dbs
			.pool(&tenant.external_id, &database)
			.await
			.map_err(|e| format!("UnableToConnectToProject: {e}"))?;
		let mut prepared = self.rt.prepared.lock().await;
		if !*prepared {
			db::prepare(&pool).await.map_err(|e| {
				format!(
					"UnableToConnectToProject: Realtime was unable to connect to the project database: {e}"
				)
			})?;
			*prepared = true;
		}
		Ok(pool)
	}

	async fn subscribe_changes(
		&mut self,
		topic: &str,
		name: &str,
		join_ref: &Option<String>,
		claims: &Map<String, Value>,
		bindings: &[Binding],
	) {
		let tenant = self.rt.tenant();
		let outcome = async {
			let database = tenant.database.clone().ok_or_else(|| {
				tenant
					.postgres_changes_refusal
					.clone()
					.unwrap_or_else(|| crate::tenants::NO_CHANGES.to_string())
			})?;
			let pool = self.pool().await?;
			let changes = {
				let mut streams = self.rt.streams.lock().await;
				// A stream made for other settings (a re-registration that moved the password or
				// the database) can never connect again; a fresh one uses the tenant's own.
				if let Some(old) = &streams.changes
					&& old.database() != &database
				{
					old.retire();
					streams.changes = None;
				}
				streams
					.changes
					.get_or_insert_with(|| changes::Changes::new(database))
					.clone()
			};
			changes
				.subscribe(
					self.rt.clone(),
					&pool,
					claims,
					bindings,
					self.tx.clone(),
					topic,
				)
				.await
		}
		.await;
		match outcome {
			Ok(()) => self.system(
				topic,
				join_ref,
				"postgres_changes",
				"ok",
				"Subscribed to PostgreSQL",
				name,
			),
			Err(e) => {
				self.system(topic, join_ref, "postgres_changes", "error", &e, name);
				if let Some(ch) = self.channels.get_mut(topic) {
					ch.bindings.clear();
				}
			}
		}
	}

	// --- leave and close ------------------------------------------------------------------

	async fn cleanup(&mut self, ch: Channel) {
		let diff = self.rt.unsubscribe(&ch.key, ch.sub);
		self.rt.broadcast_diff(&ch.key, &diff);
		self.rt.left(self.id);
		if !ch.bindings.is_empty() {
			let changes = self.rt.streams.lock().await.changes.clone();
			if let Some(database) = self.rt.tenant().database.clone()
				&& let Ok(pool) = self.app.dbs.pool(&self.rt.id, &database).await
			{
				let ids: Vec<Uuid> = ch.bindings.iter().map(|b| b.subscription_id).collect();
				match changes {
					Some(changes) => changes.unsubscribe(&pool, &ids).await,
					// The stream was retired under this channel (`changes::forget_rows`).
					None => crate::changes::forget_rows(&pool, &ids).await,
				}
			}
		}
	}

	async fn close_all(&mut self) {
		let all: Vec<Channel> = self.channels.drain().map(|(_, c)| c).collect();
		for ch in all {
			self.cleanup(ch).await;
		}
	}

	/// Stop one channel with a sentence: a `system` error, then `phx_close`.
	async fn shutdown(&mut self, topic: &str, message: &str) {
		if let Some(ch) = self.channels.remove(topic) {
			let (join_ref, name) = (ch.join_ref.clone(), ch.name.clone());
			self.rt.log.push(
				Event::new(Kind::ChannelClosed)
					.socket(self.id)
					.channel(&name)
					.presence_key(&ch.presence_key)
					.reason(message),
			);
			self.cleanup(ch).await;
			self.closed.remember(topic, message);
			self.system(topic, &join_ref, "system", "error", message, &name);
			self.close_frame(topic, &join_ref);
		}
	}

	// --- tokens ---------------------------------------------------------------------------

	async fn confirm_tokens(&mut self) {
		let now = Instant::now();
		let due: Vec<String> = self
			.channels
			.iter()
			.filter(|(_, c)| c.confirm_at <= now)
			.map(|(t, _)| t.clone())
			.collect();
		let tenant = self.rt.tenant();
		for topic in due {
			let Some(ch) = self.channels.get_mut(&topic) else {
				continue;
			};
			match jwt::authorize(&ch.access_token, &tenant.jwt_secret, now_secs()) {
				Ok(claims) => {
					let exp = claims.get("exp").and_then(Value::as_i64).unwrap_or(0);
					ch.confirm_at = Instant::now()
						+ Duration::from_secs((exp - now_secs()).max(0) as u64)
							.min(CONFIRM_TOKEN_EVERY);
					ch.auth.claims = claims;
				}
				Err(e) => self.shutdown(&topic, &e.message()).await,
			}
		}
	}

	async fn access_token(&mut self, m: Inbound) {
		let InboundPayload::Json(payload) = &m.payload else {
			return;
		};
		let Some(token) = payload
			.get("access_token")
			.and_then(Value::as_str)
			.map(str::to_string)
		else {
			return;
		};
		let tenant = self.rt.tenant();
		let Some(ch) = self.channels.get(&m.topic) else {
			return;
		};
		// The same token again, or a project key: nothing to do.
		if token.starts_with("sb_") || token == ch.access_token {
			return;
		}
		let claims = match jwt::authorize(&token, &tenant.jwt_secret, now_secs()) {
			Ok(c) => c,
			Err(e) => {
				self.shutdown(&m.topic, &e.message()).await;
				return;
			}
		};
		let (private, presence_enabled, name) = (ch.private, ch.presence_enabled, ch.name.clone());
		let mut auth = ch.auth.clone();
		auth.claims = claims.clone();
		auth.role = claims
			.get("role")
			.and_then(Value::as_str)
			.unwrap_or_default()
			.to_string();
		auth.sub = claims
			.get("sub")
			.and_then(Value::as_str)
			.map(str::to_string);
		let mut policies = Policies::default();
		if private {
			let pool = match self.pool().await {
				Ok(p) => p,
				Err(_) => {
					self.shutdown(
						&m.topic,
						"Realtime was unable to connect to the project database",
					)
					.await;
					return;
				}
			};
			match db::authorize_read(&pool, &auth, presence_enabled, policies).await {
				Ok(p) if p.broadcast_read == Some(false) => {
					self.shutdown(
						&m.topic,
						&format!(
							"You do not have permissions to read from this Channel topic: {name}"
						),
					)
					.await;
					return;
				}
				Ok(p) => policies = p,
				Err(_) => {
					self.shutdown(
						&m.topic,
						"Realtime was unable to connect to the project database",
					)
					.await;
					return;
				}
			}
		}
		// Database changes are checked as the claims stored with each binding: they change too.
		let ids: Vec<Uuid> = self
			.channels
			.get(&m.topic)
			.map(|ch| ch.bindings.iter().map(|b| b.subscription_id).collect())
			.unwrap_or_default();
		if !ids.is_empty() {
			let changes = self.rt.streams.lock().await.changes.clone();
			let reclaimed = match (changes, self.pool().await) {
				(Some(changes), Ok(pool)) => changes.reclaim(&pool, &ids, &claims).await,
				(None, _) => Ok(()),
				(_, Err(e)) => Err(e),
			};
			if reclaimed.is_err() {
				self.shutdown(
					&m.topic,
					"Realtime was unable to connect to the project database",
				)
				.await;
				return;
			}
		}
		let exp = claims.get("exp").and_then(Value::as_i64).unwrap_or(0);
		if let Some(ch) = self.channels.get_mut(&m.topic) {
			ch.access_token = token;
			ch.auth = auth;
			ch.policies = policies;
			ch.confirm_at = Instant::now()
				+ Duration::from_secs((exp - now_secs()).max(0) as u64).min(CONFIRM_TOKEN_EVERY);
		}
	}

	// --- broadcast ------------------------------------------------------------------------

	async fn write_allowed(&mut self, topic: &str, extension: &str) -> Result<bool, String> {
		let Some(ch) = self.channels.get(topic) else {
			return Ok(false);
		};
		if !ch.private {
			return Ok(true);
		}
		let known = if extension == "presence" {
			ch.policies.presence_write
		} else {
			ch.policies.broadcast_write
		};
		if let Some(k) = known {
			return Ok(k);
		}
		let (auth, policies) = (ch.auth.clone(), ch.policies);
		let pool = self.pool().await?;
		let policies = db::authorize_write(&pool, &auth, extension, policies)
			.await
			.map_err(|e| e.to_string())?;
		let allowed = if extension == "presence" {
			policies.presence_write
		} else {
			policies.broadcast_write
		}
		.unwrap_or(false);
		if let Some(ch) = self.channels.get_mut(topic) {
			ch.policies = policies;
		}
		Ok(allowed)
	}

	async fn broadcast(&mut self, m: Inbound) {
		let allowed = self.write_allowed(&m.topic, "broadcast").await;
		let Some(ch) = self.channels.get(&m.topic) else {
			return;
		};
		let (ack, key, except) = (
			ch.ack,
			ch.key.clone(),
			if ch.self_broadcast {
				None
			} else {
				Some(ch.sub)
			},
		);
		match allowed {
			Ok(true) => {}
			Ok(false) => {
				// Refused by the write policy. The pinned server says nothing; ours answers an
				// acknowledged broadcast.
				if ack {
					self.reply(&m, "error", json!({ "error": "unauthorized" }));
				}
				return;
			}
			Err(_) => {
				if ack {
					self.reply(&m, "error", json!({ "error": "unable_to_authorize" }));
				}
				return;
			}
		}
		let tenant = self.rt.tenant();
		let max = payload_limit(tenant.max_payload_size_in_kb);
		let size = match &m.payload {
			InboundPayload::Json(v) => v.to_string().len() as i64,
			InboundPayload::Bytes(b) => b.len() as i64,
			InboundPayload::UserBroadcast { payload, .. } => payload.len() as i64,
		};
		if size > max {
			if ack {
				self.reply(&m, "error", json!({ "error": "payload_size_exceeded" }));
			}
			return;
		}
		// The tenant's messages a second, counted on the same window the HTTP endpoint uses.
		// It was counted there only, so one socket could broadcast without limit through a
		// server every project on the host shares (QA round 11: 1,500 in a burst, all delivered
		// on a 500 a second plan). Over it, the channel is closed as the pinned server closes it.
		if self.rt.events.add(1, tenant.max_events_per_second) {
			let topic = m.topic.clone();
			self.shutdown(&topic, "Too many messages per second").await;
			return;
		}
		match &m.payload {
			InboundPayload::UserBroadcast {
				user_event,
				encoding,
				payload,
				..
			} => {
				hub::deliver_user(
					&self.rt,
					&key,
					except,
					hub::UserMessage {
						event: user_event,
						encoding: *encoding,
						payload,
						metadata: None,
						id: None,
					},
				);
			}
			InboundPayload::Json(v) => {
				hub::deliver_json(&self.rt, &key, except, v.clone());
			}
			InboundPayload::Bytes(_) => {}
		}
		if ack {
			self.reply(&m, "ok", json!({}));
		}
	}

	// --- presence -------------------------------------------------------------------------

	async fn presence(&mut self, m: Inbound) {
		let InboundPayload::Json(payload) = &m.payload else {
			return;
		};
		let event = payload
			.get("event")
			.and_then(Value::as_str)
			.unwrap_or_default()
			.to_ascii_lowercase();
		{
			let Some(ch) = self.channels.get_mut(&m.topic) else {
				return;
			};
			let now = Instant::now();
			match ch.presence_calls.1 {
				Some(reset) if now <= reset => {
					if ch.presence_calls.0 >= PRESENCE_CALLS {
						let topic = m.topic.clone();
						self.shutdown(&topic, "Client presence rate limit exceeded")
							.await;
						return;
					}
					ch.presence_calls.0 += 1;
				}
				_ => ch.presence_calls = (1, Some(now + PRESENCE_WINDOW)),
			}
		}
		match event.as_str() {
			"track" => {
				let body = payload.get("payload").cloned().unwrap_or_else(|| json!({}));
				let Value::Object(body) = body else {
					self.reply(
						&m,
						"error",
						json!({ "reason": "Presence track payload must be a map" }),
					);
					return;
				};
				match self.write_allowed(&m.topic, "presence").await {
					Ok(true) => {}
					_ => {
						self.reply(&m, "error", json!({}));
						return;
					}
				}
				// Kept in memory for the channel's life, so held to the payload limit a
				// broadcast is; over it the channel is shut, as the pinned server shuts it.
				if presence_too_large(&body, self.rt.tenant().max_payload_size_in_kb) {
					let topic = m.topic.clone();
					self.shutdown(&topic, "Track message size exceeded").await;
					return;
				}
				let Some(ch) = self.channels.get_mut(&m.topic) else {
					return;
				};
				if ch.presence_payload.as_ref() == Some(&body) {
					self.reply(&m, "ok", json!({}));
					return;
				}
				let enabling = !ch.presence_enabled;
				ch.presence_enabled = true;
				ch.presence_payload = Some(body.clone());
				let (key, sub, presence_key, join_ref) = (
					ch.key.clone(),
					ch.sub,
					ch.presence_key.clone(),
					ch.join_ref.clone(),
				);
				let can_read = can_read_presence(ch.private, &ch.policies);
				self.reply(&m, "ok", json!({}));
				// Enabling presence by tracking sends the state first, so the members already
				// there are seen.
				if enabling && can_read {
					let state = self.rt.presence_state(&key);
					let f = protocol::message(
						self.vsn,
						&join_ref,
						&None,
						&m.topic,
						"presence_state",
						state,
					);
					self.push(f);
				}
				let diff = self.rt.track(&key, sub, &presence_key, body);
				self.rt.broadcast_diff(&key, &diff);
			}
			"untrack" => {
				let Some(ch) = self.channels.get_mut(&m.topic) else {
					return;
				};
				ch.presence_payload = None;
				let (key, sub, presence_key) = (ch.key.clone(), ch.sub, ch.presence_key.clone());
				// The leave reaches this channel before the reply does, as the pinned server
				// sends it; everyone else on the topic gets it through the hub.
				let diff = self.rt.untrack(&key, sub, &presence_key);
				if !diff.is_empty() {
					let f =
						protocol::broadcast(self.vsn, &m.topic, "presence_diff", diff.payload());
					self.push(f);
				}
				self.rt.broadcast_diff_except(&key, &diff, Some(sub));
				self.reply(&m, "ok", json!({}));
			}
			_ => self.reply(&m, "error", json!({})),
		}
	}

	// --- outbound -------------------------------------------------------------------------

	async fn deliver(&mut self, o: Out) {
		match o {
			Out::Broadcast {
				join_topic,
				event,
				payload,
				..
			} => {
				// A presence diff carries other members' keys and payloads: a private channel
				// whose policy refuses presence read is sent none, as it is sent no state.
				let Some(ch) = self.channels.get(&join_topic) else {
					return;
				};
				if !may_receive(ch.private, &ch.policies, &event) {
					return;
				}
				let f = protocol::broadcast(self.vsn, &join_topic, &event, (*payload).clone());
				self.push(f);
			}
			Out::User {
				join_topic,
				event,
				encoding,
				payload,
				metadata,
				..
			} => {
				if !self.channels.contains_key(&join_topic) {
					return;
				}
				match self.vsn {
					Vsn::V2 => {
						if let Ok(f) = protocol::user_broadcast(
							&join_topic,
							&event,
							metadata.as_deref(),
							encoding,
							&payload,
						) {
							self.push(f);
						}
					}
					Vsn::V1 => {
						if let Some(v) = protocol::user_broadcast_as_json(
							&event,
							encoding,
							&payload,
							metadata.as_deref(),
						) {
							let f = protocol::broadcast(self.vsn, &join_topic, "broadcast", v);
							self.push(f);
						}
					}
				}
			}
			Out::Changes {
				join_topic,
				ids,
				data,
			} => {
				if !self.channels.contains_key(&join_topic) {
					return;
				}
				// `data` is JSON the change stream serialised once for every channel.
				let raw = format!("{{\"ids\":{},\"data\":{}}}", Value::from(ids), data);
				let f = protocol::broadcast_raw(self.vsn, &join_topic, "postgres_changes", &raw);
				self.push(f);
			}
			Out::System {
				join_topic,
				extension,
				status,
				message,
				stop,
			} => {
				if stop {
					self.shutdown(&join_topic, &message).await;
				} else if let Some(ch) = self.channels.get(&join_topic) {
					let (jr, name) = (ch.join_ref.clone(), ch.name.clone());
					self.system(&join_topic, &jr, &extension, &status, &message, &name);
				}
			}
			Out::Disconnect => {}
		}
	}
}

#[cfg(test)]
mod tests {
	use std::pin::Pin;
	use std::task::{Context, Poll};

	use super::*;

	/// A client that never reads: the socket never becomes writable again.
	struct Stalled;

	impl Sink<WsMessage> for Stalled {
		type Error = ();
		fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
			Poll::Pending
		}
		fn start_send(self: Pin<&mut Self>, _: WsMessage) -> Result<(), ()> {
			Ok(())
		}
		fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
			Poll::Pending
		}
		fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
			Poll::Pending
		}
	}

	#[tokio::test]
	async fn a_client_that_stops_reading_is_given_up_on() {
		let bytes = AtomicU64::new(0);
		let started = Instant::now();
		let written = write_frames(
			&mut Stalled,
			vec![Frame::Text("x".into())],
			&bytes,
			Duration::from_millis(50),
		)
		.await;
		assert!(!written);
		assert!(started.elapsed() < Duration::from_secs(5));
	}

	#[tokio::test]
	async fn a_client_that_reads_is_written_to() {
		let bytes = AtomicU64::new(0);
		let mut sink = futures_util::sink::drain();
		let frames = vec![Frame::Text("abc".into()), Frame::Binary(vec![1, 2])];
		assert!(write_frames(&mut sink, frames, &bytes, WRITE_TIMEOUT).await);
		assert_eq!(bytes.load(Ordering::Relaxed), 5);
	}

	#[test]
	fn the_timeouts_are_the_pinned_servers() {
		assert_eq!(IDLE_TIMEOUT, Duration::from_secs(60));
		assert_eq!(WRITE_TIMEOUT, Duration::from_secs(30));
	}

	#[test]
	fn a_presence_payload_is_held_to_the_payload_limit() {
		let mut small = Map::new();
		small.insert("user".into(), json!("ada"));
		assert!(!presence_too_large(&small, 1));
		let mut big = Map::new();
		big.insert("p".into(), json!("a".repeat(1600)));
		assert!(presence_too_large(&big, 1));
		// The same payload under the default 3,000 KB limit, and the attack's 60 MB over it.
		assert!(!presence_too_large(&big, 3000));
		let mut huge = Map::new();
		huge.insert("p".into(), json!("a".repeat(3_000_501)));
		assert!(presence_too_large(&huge, 3000));
	}

	#[test]
	fn a_frame_may_carry_the_default_payload_and_no_more_than_the_pinned_server_reads() {
		assert_eq!(MAX_FRAME_SIZE, 5_000_000);
		assert!(MAX_FRAME_SIZE as i64 > payload_limit(3000));
		assert_eq!(payload_limit(3000), 3_000_500);
	}

	#[test]
	fn a_private_channel_without_presence_read_gets_no_presence_diff() {
		let refused = Policies {
			broadcast_read: Some(true),
			presence_read: Some(false),
			..Policies::default()
		};
		let unasked = Policies {
			broadcast_read: Some(true),
			..Policies::default()
		};
		let allowed = Policies {
			broadcast_read: Some(true),
			presence_read: Some(true),
			..Policies::default()
		};
		assert!(!may_receive(true, &refused, "presence_diff"));
		assert!(!may_receive(true, &unasked, "presence_diff"));
		assert!(may_receive(true, &allowed, "presence_diff"));
		// Broadcasts are decided at join, by the broadcast read policy.
		assert!(may_receive(true, &refused, "broadcast"));
		// A public channel has no policies to consult.
		assert!(may_receive(false, &Policies::default(), "presence_diff"));
	}

	fn join(join_ref: Option<&str>, reference: Option<&str>) -> Inbound {
		Inbound {
			join_ref: join_ref.map(str::to_string),
			reference: reference.map(str::to_string),
			topic: "realtime:room".into(),
			event: "phx_join".into(),
			payload: InboundPayload::Json(json!({})),
		}
	}

	fn text(f: Frame) -> Value {
		match f {
			Frame::Text(t) => serde_json::from_str(&t).unwrap(),
			Frame::Binary(_) => panic!("a text frame was expected"),
		}
	}

	#[test]
	fn a_v1_join_without_a_join_ref_is_known_by_its_ref() {
		// A hand-written V1 client sends no join_ref: its join's ref stands in.
		assert_eq!(
			channel_join_ref(Vsn::V1, &join(None, Some("6"))).as_deref(),
			Some("6")
		);
		// realtime-js sends both, equal; a join_ref that is sent is kept.
		assert_eq!(
			channel_join_ref(Vsn::V1, &join(Some("6"), Some("6"))).as_deref(),
			Some("6")
		);
		// V2 has its own slot, and is left as the client sent it.
		assert_eq!(channel_join_ref(Vsn::V2, &join(None, Some("6"))), None);
		assert_eq!(
			channel_join_ref(Vsn::V2, &join(Some("3"), Some("6"))).as_deref(),
			Some("3")
		);
	}

	#[test]
	fn a_server_close_and_system_message_carry_the_join_ref() {
		let jr = Some("6".to_string());
		// V1: in `ref`, the only slot it has.
		let close = text(protocol::message(
			Vsn::V1,
			&jr,
			&jr,
			"realtime:room",
			"phx_close",
			json!({}),
		));
		assert_eq!(close["ref"], json!("6"));
		let system = text(system_frame(
			Vsn::V1,
			"realtime:room",
			&jr,
			json!({ "status": "error" }),
		));
		assert_eq!(system["event"], json!("system"));
		assert_eq!(system["ref"], json!("6"));
		// V2: in the join_ref slot, with `ref` null, as before.
		let system = text(system_frame(Vsn::V2, "realtime:room", &jr, json!({})));
		assert_eq!(system[0], json!("6"));
		assert_eq!(system[1], Value::Null);
		// Nothing else changes shape: presence_state in V1 still has a null ref.
		let state = text(protocol::message(
			Vsn::V1,
			&jr,
			&None,
			"realtime:room",
			"presence_state",
			json!({}),
		));
		assert_eq!(state["ref"], Value::Null);
	}

	#[test]
	fn presence_is_asked_for_by_enabled_or_by_a_presence_object_without_it() {
		assert!(presence_asked(
			&json!({ "presence": { "key": "a", "enabled": true } })
		));
		assert!(!presence_asked(
			&json!({ "presence": { "key": "a", "enabled": false } })
		));
		// No `enabled` at all: an older or hand-written client, taken as asking.
		assert!(presence_asked(&json!({ "presence": { "key": "a" } })));
		assert!(presence_asked(&json!({ "presence": {} })));
		assert!(!presence_asked(&json!({ "broadcast": { "self": true } })));
		assert!(!presence_asked(&Value::Null));
		assert!(!presence_asked(
			&json!({ "presence": { "key": "a", "enabled": null } })
		));
	}

	#[test]
	fn a_message_on_a_topic_the_server_closed_is_told_why() {
		let mut closed = Closed::default();
		closed.remember("realtime:room", "Too many messages per second");
		let reply = unmatched(closed.why("realtime:room"));
		assert_eq!(reply["reason"], json!("unmatched topic"));
		assert_eq!(
			reply["message"],
			json!(
				"channel closed by the server: Too many messages per second. Join it again, or open a new socket."
			)
		);
		// A topic never joined is answered as Phoenix answers it.
		assert_eq!(
			unmatched(closed.why("realtime:other")),
			json!({ "reason": "unmatched topic" })
		);
		// Joined (or left) again: forgotten.
		closed.forget("realtime:room");
		assert_eq!(closed.why("realtime:room"), None);
	}

	#[test]
	fn the_closed_topics_a_socket_remembers_are_bounded() {
		let mut closed = Closed::default();
		for i in 0..CLOSED_REMEMBERED + 10 {
			closed.remember(&format!("realtime:{i}"), "why");
		}
		assert_eq!(closed.0.len(), CLOSED_REMEMBERED);
		// The oldest went first.
		assert_eq!(closed.why("realtime:0"), None);
		assert_eq!(
			closed.why(&format!("realtime:{}", CLOSED_REMEMBERED + 9)),
			Some("why")
		);
		// A topic closed twice is remembered once, with the newer reason.
		closed.remember("realtime:x", "first");
		closed.remember("realtime:x", "second");
		assert_eq!(
			closed.0.iter().filter(|(t, _)| t == "realtime:x").count(),
			1
		);
		assert_eq!(closed.why("realtime:x"), Some("second"));
	}
}
