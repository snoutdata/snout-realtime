//! One WebSocket: the Phoenix socket protocol, and the channels joined on it.
//!
//! The socket is one task. It reads the client's frames, runs each channel's join, leave,
//! broadcast, presence and token refresh IN ORDER (so replies come back in the order asked),
//! and writes whatever the hub queues for it, serialised for this socket's `vsn`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::extract::ws::{Message as WsMessage, WebSocket};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Map, Value, json};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use uuid::Uuid;

use crate::App;
use crate::changes::{self, Binding};
use crate::db::{self, AuthContext, Policies};
use crate::hub::{self, Out, Sub, TenantRt, TopicKey};
use crate::jwt;
use crate::protocol::{self, Frame, Inbound, InboundPayload, Vsn};

/// How often a channel re-checks its token, at most (the pinned server's five minutes).
const CONFIRM_TOKEN_EVERY: Duration = Duration::from_secs(300);
/// Client presence calls allowed per window before the channel is shut (the pinned server's).
const PRESENCE_CALLS: u32 = 5;
const PRESENCE_WINDOW: Duration = Duration::from_secs(30);

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
	tx: UnboundedSender<Out>,
	channels: HashMap<String, Channel>,
	/// Frames waiting to go out.
	out: Vec<Frame>,
}

pub async fn serve(app: Arc<App>, ws: WebSocket, accepted: Accepted) {
	let (tx, mut rx) = unbounded_channel();
	let id = app.hub.next_id();
	accepted
		.rt
		.socket_opened(id, accepted.address.clone(), tx.clone());
	let mut s = Socket {
		app,
		rt: accepted.rt,
		id,
		vsn: accepted.vsn,
		token: accepted.token,
		headers: accepted.headers,
		tx,
		channels: HashMap::new(),
		out: Vec::new(),
	};
	let (mut sink, mut stream) = ws.split();
	loop {
		let next_confirm = s.channels.values().map(|c| c.confirm_at).min();
		let wait = async {
			match next_confirm {
				Some(at) => tokio::time::sleep_until(at.into()).await,
				None => std::future::pending::<()>().await,
			}
		};
		tokio::select! {
			incoming = stream.next() => {
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
					Some(Ok(WsMessage::Close(_))) | None | Some(Err(_)) => break,
					Some(Ok(_)) => {}
				}
			}
			out = rx.recv() => {
				match out {
					Some(Out::Disconnect) | None => break,
					Some(o) => s.deliver(o).await,
				}
			}
			_ = wait => s.confirm_tokens().await,
		}
		if !s.flush(&mut sink).await {
			break;
		}
	}
	s.close_all().await;
	s.rt.socket_closed(s.id);
	let _ = sink.close().await;
	drain(&mut rx);
}

fn drain(rx: &mut UnboundedReceiver<Out>) {
	while rx.try_recv().is_ok() {}
}

impl Socket {
	async fn flush(
		&mut self,
		sink: &mut futures_util::stream::SplitSink<WebSocket, WsMessage>,
	) -> bool {
		for frame in self.out.drain(..) {
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
			self.rt
				.counters
				.output_bytes
				.fetch_add(len as u64, Ordering::Relaxed);
			if sink.send(msg).await.is_err() {
				return false;
			}
		}
		true
	}

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
		let f = protocol::message(self.vsn, join_ref, &None, topic, "system", payload);
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
				if let Some(ch) = self.channels.remove(&m.topic) {
					self.cleanup(ch).await;
					self.reply(&m, "ok", json!({}));
					let join_ref = m.join_ref.clone();
					self.close_frame(&m.topic.clone(), &join_ref);
				} else {
					self.reply(&m, "ok", json!({}));
				}
			}
			_ if !self.channels.contains_key(&m.topic) => {
				self.reply(&m, "error", json!({ "reason": "unmatched topic" }));
			}
			"broadcast" => self.broadcast(m).await,
			"presence" => self.presence(m).await,
			"access_token" => self.access_token(m).await,
			_ => {}
		}
	}

	// --- join -----------------------------------------------------------------------------

	async fn join(&mut self, m: Inbound) {
		if let Some(old) = self.channels.remove(&m.topic) {
			// A second join on a joined topic replaces the first, as Phoenix does.
			let old_ref = old.join_ref.clone();
			self.cleanup(old).await;
			self.close_frame(&m.topic.clone(), &old_ref);
		}
		match self.try_join(&m).await {
			Ok((channel, response, after)) => {
				self.reply(&m, "ok", response);
				let name = channel.name.clone();
				let join_ref = channel.join_ref.clone();
				let presence = channel.presence_enabled
					&& (!channel.private || channel.policies.presence_read == Some(true));
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
			Err(reason) => self.reply(&m, "error", json!({ "reason": reason })),
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
		let presence_enabled = config
			.pointer("/presence/enabled")
			.and_then(Value::as_bool)
			.unwrap_or(false)
			|| tenant.presence_enabled;

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
			after.push(protocol::message(
				self.vsn,
				&m.join_ref,
				&None,
				&m.topic,
				"system",
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
			},
		);
		let exp = claims.get("exp").and_then(Value::as_i64).unwrap_or(0);
		let confirm_at = Instant::now()
			+ Duration::from_secs((exp - now_secs()).max(0) as u64).min(CONFIRM_TOKEN_EVERY);
		let channel = Channel {
			join_ref: m.join_ref.clone(),
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
			let database = tenant
				.database
				.clone()
				.ok_or("postgres_changes is not enabled for this project".to_string())?;
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
			if let (Some(changes), Some(database)) = (changes, self.rt.tenant().database.clone())
				&& let Ok(pool) = self.app.dbs.pool(&self.rt.id, &database).await
			{
				let ids: Vec<Uuid> = ch.bindings.iter().map(|b| b.subscription_id).collect();
				changes.unsubscribe(&pool, &ids).await;
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
			self.cleanup(ch).await;
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
		let max = tenant.max_payload_size_in_kb * 1000 + 500;
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
				let private = ch.private;
				let can_read = !private || ch.policies.presence_read == Some(true);
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
			} => {
				if !self.channels.contains_key(&join_topic) {
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
