//! The Socket.IO door: Engine.IO 4 sessions over long-polling and websocket (with the upgrade
//! between them), carrying Socket.IO 5, so `socket.io-client` connects unmodified.
//!
//! What it MEANS is the realtime handlers' (`handlers.rs`; https://docs.snoutdata.com/stack/socketio):
//! a namespace is connected with `auth.token` (the user's JWT, or the project key), and its
//! `connection` handler may refuse it or put it in rooms; an emit runs the handler for that event
//! and namespace, as the caller, and what it returns is the ack; a room is a Realtime topic, so
//! `realtime.emit()` from a handler and `realtime.send()` from any SQL reach it, and a channel of
//! the same name on the Phoenix door shares it.
//!
//! One session is one task, like a Phoenix socket: it reads the client's packets IN ORDER (so a
//! handler's ack comes back in the order the emits were sent), writes what the hub queues for it,
//! and pings. The transports only move packets: a polling GET drains the session's outbound queue,
//! a POST feeds its inbound one, and a websocket does both until the session ends.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Map, Value, json};
use tokio::sync::{Mutex as AsyncMutex, Notify, mpsc};

use crate::App;
use crate::db::AuthContext;
use crate::engineio::{self, Packet as EPacket};
use crate::handlers::{self, Call, Op};
use crate::hub::{self, Inbox, Out, Outbox, Sub, TenantRt, TopicKey};
use crate::inspect::{self, Event, Kind};
use crate::jwt;
use crate::protocol::Encoding;
use crate::socket::{self, Accepted, now_secs, payload_limit};
use crate::socketio::{self, Kind as SKind, Packet as SPacket};

/// Packets one session may have waiting to go out before it is dropped (as a Phoenix socket's).
const MAX_QUEUED: usize = hub::MAX_QUEUED;
const MAX_QUEUED_BYTES: usize = hub::MAX_QUEUED_BYTES;
/// Packets a client may have waiting to be read before its POST or its websocket waits.
const INBOUND: usize = 256;
/// How long a websocket upgrade may take, from the probe to the upgrade packet.
const UPGRADE_WITHIN: Duration = Duration::from_secs(10);
/// How long a disconnect handler may take, after the client is gone.
const DISCONNECT_WITHIN: Duration = Duration::from_secs(6);

/// The open sessions, by Engine.IO sid.
#[derive(Default)]
pub struct Sessions {
	map: std::sync::Mutex<HashMap<String, Arc<Handle>>>,
}

impl Sessions {
	fn get(&self, sid: &str) -> Option<Arc<Handle>> {
		self.lock().get(sid).cloned()
	}

	fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Arc<Handle>>> {
		self.map.lock().unwrap_or_else(|e| e.into_inner())
	}

	pub fn len(&self) -> usize {
		self.lock().len()
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}
}

/// What the transports hold of a session.
struct Handle {
	tenant: String,
	inbound: mpsc::Sender<In>,
	pipe: Pipe,
	/// One reader of the outbound queue at a time: a polling GET, or the websocket.
	reader: AsyncMutex<()>,
	upgraded: AtomicBool,
}

enum In {
	Packet(EPacket),
	Closed(String),
}

/// A session's outbound queue, bounded like a Phoenix socket's, read by whichever transport
/// holds it.
#[derive(Default)]
struct Pipe {
	queue: std::sync::Mutex<VecDeque<EPacket>>,
	bytes: AtomicUsize,
	notify: Notify,
	closed: AtomicBool,
	overflowed: AtomicBool,
}

impl Pipe {
	fn push(&self, p: EPacket) {
		if self.closed.load(Ordering::Acquire) {
			return;
		}
		let mut q = self.queue.lock().unwrap_or_else(|e| e.into_inner());
		let bytes = self.bytes.load(Ordering::Acquire);
		if !q.is_empty() && (q.len() >= MAX_QUEUED || bytes + p.len() > MAX_QUEUED_BYTES) {
			self.overflowed.store(true, Ordering::Release);
			return;
		}
		self.bytes.fetch_add(p.len(), Ordering::AcqRel);
		q.push_back(p);
		drop(q);
		self.notify.notify_one();
	}

	fn drain(&self) -> Vec<EPacket> {
		let mut q = self.queue.lock().unwrap_or_else(|e| e.into_inner());
		self.bytes.store(0, Ordering::Release);
		q.drain(..).collect()
	}

	fn close(&self) {
		self.closed.store(true, Ordering::Release);
		self.notify.notify_one();
	}

	fn is_closed(&self) -> bool {
		self.closed.load(Ordering::Acquire)
	}

	/// What is waiting, as soon as there is something, the session closes, or `wait` passes.
	/// Cancel-safe: nothing is taken off the queue until it is returned.
	async fn take(&self, wait: Duration) -> Vec<EPacket> {
		let deadline = tokio::time::Instant::now() + wait;
		loop {
			let out = self.drain();
			if !out.is_empty() || self.is_closed() {
				return out;
			}
			if tokio::time::timeout_at(deadline, self.notify.notified())
				.await
				.is_err()
			{
				return Vec::new();
			}
		}
	}
}

fn random_id() -> String {
	use base64::Engine as _;
	let mut bytes = [0u8; 15];
	getrandom::fill(&mut bytes).expect("the operating system has randomness");
	base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

fn error(code: u8, message: &str) -> Response {
	(
		StatusCode::BAD_REQUEST,
		axum::Json(json!({ "code": code, "message": message })),
	)
		.into_response()
}

fn text(body: String) -> Response {
	([(header::CONTENT_TYPE, "text/plain; charset=UTF-8")], body).into_response()
}

// --- the transports -----------------------------------------------------------------------

/// `GET /socket.io/`: a new session (polling or websocket), a poll, or an upgrade.
pub async fn get(
	State(app): State<Arc<App>>,
	headers: HeaderMap,
	Query(params): Query<HashMap<String, String>>,
	upgrade: Result<WebSocketUpgrade, axum::extract::ws::rejection::WebSocketUpgradeRejection>,
) -> Response {
	if params.get("EIO").map(String::as_str) != Some(engineio::PROTOCOL) {
		return error(5, "Unsupported protocol version");
	}
	let transport = params.get("transport").map(String::as_str);
	match (transport, params.get("sid")) {
		(Some("polling"), None) => {
			let (sid, _) = match open(&app, &headers, &params).await {
				Ok(s) => s,
				Err(r) => return *r,
			};
			text(EPacket::Open(engineio::handshake(&sid, true)).encode())
		}
		(Some("polling"), Some(sid)) => {
			let Some(handle) = known(&app, &headers, sid) else {
				return error(1, "Session ID unknown");
			};
			poll(&handle).await
		}
		(Some("websocket"), sid) => {
			let Ok(upgrade) = upgrade else {
				return error(3, "Bad request");
			};
			let upgrade = upgrade
				.read_buffer_size(4096)
				.write_buffer_size(4096)
				.max_frame_size(socket::MAX_FRAME_SIZE)
				.max_message_size(socket::MAX_FRAME_SIZE);
			match sid {
				None => {
					let (sid, handle) = match open(&app, &headers, &params).await {
						Ok(s) => s,
						Err(r) => return *r,
					};
					handle
						.pipe
						.push(EPacket::Open(engineio::handshake(&sid, false)));
					upgrade.on_upgrade(move |ws| websocket(ws, handle, false))
				}
				Some(sid) => {
					let Some(handle) = known(&app, &headers, sid) else {
						return error(1, "Session ID unknown");
					};
					if handle.upgraded.load(Ordering::Acquire) {
						return error(3, "Bad request");
					}
					upgrade.on_upgrade(move |ws| websocket(ws, handle, true))
				}
			}
		}
		_ => error(0, "Transport unknown"),
	}
}

/// `POST /socket.io/`: packets from a polling client.
pub async fn post(
	State(app): State<Arc<App>>,
	headers: HeaderMap,
	Query(params): Query<HashMap<String, String>>,
	body: Bytes,
) -> Response {
	let Some(handle) = params.get("sid").and_then(|sid| known(&app, &headers, sid)) else {
		return error(1, "Session ID unknown");
	};
	if handle.upgraded.load(Ordering::Acquire) {
		return error(3, "Bad request");
	}
	if body.len() > engineio::MAX_PAYLOAD {
		let _ = handle
			.inbound
			.send(In::Closed("a polling body over the limit".into()))
			.await;
		return (StatusCode::PAYLOAD_TOO_LARGE, "").into_response();
	}
	let Ok(body) = std::str::from_utf8(&body) else {
		return error(3, "Bad request");
	};
	let Ok(packets) = engineio::decode_payload(body) else {
		return error(3, "Bad request");
	};
	for p in packets {
		if handle.inbound.send(In::Packet(p)).await.is_err() {
			return error(1, "Session ID unknown");
		}
	}
	text("ok".into())
}

/// The session `sid` names, if it belongs to the project this request is for.
fn known(app: &App, headers: &HeaderMap, sid: &str) -> Option<Arc<Handle>> {
	let handle = app.sio.get(sid)?;
	(crate::api::external_id(headers).as_deref() == Some(handle.tenant.as_str())).then_some(handle)
}

async fn poll(handle: &Arc<Handle>) -> Response {
	if handle.upgraded.load(Ordering::Acquire) {
		return error(3, "Bad request");
	}
	// A second GET while one is waiting is a broken client; Engine.IO closes the session.
	let Ok(_reading) = handle.reader.try_lock() else {
		let _ = handle
			.inbound
			.send(In::Closed("two polls at once".into()))
			.await;
		return error(3, "Bad request");
	};
	let wait = Duration::from_millis(engineio::PING_INTERVAL_MS + 5_000);
	let packets = handle.pipe.take(wait).await;
	if packets.is_empty() {
		return text(if handle.pipe.is_closed() {
			EPacket::Close.encode()
		} else {
			EPacket::Noop.encode()
		});
	}
	text(engineio::encode_payload(&packets))
}

async fn websocket(ws: WebSocket, handle: Arc<Handle>, upgrading: bool) {
	let (mut sink, mut stream) = ws.split();
	if upgrading {
		// `2probe` → `3probe`, then a noop so a poll waiting on the old transport returns, then
		// the client's `5`. Anything else and the session stays on polling.
		let upgraded = tokio::time::timeout(UPGRADE_WITHIN, async {
			loop {
				match stream.next().await {
					Some(Ok(WsMessage::Text(t))) => match engineio::decode(t.as_str()) {
						Ok(EPacket::Ping(d)) if d == "probe" => {
							let pong = EPacket::Pong("probe".into()).encode();
							if sink.send(WsMessage::Text(pong.into())).await.is_err() {
								return false;
							}
							handle.pipe.push(EPacket::Noop);
						}
						Ok(EPacket::Upgrade) => return true,
						_ => return false,
					},
					Some(Ok(WsMessage::Ping(_) | WsMessage::Pong(_))) => {}
					_ => return false,
				}
			}
		})
		.await
		.unwrap_or(false);
		if !upgraded {
			let _ = sink.close().await;
			return;
		}
	}
	// Wait for a poll in flight to finish, then hold the queue for good.
	let _reading = handle.reader.lock().await;
	handle.upgraded.store(true, Ordering::Release);
	loop {
		tokio::select! {
			incoming = stream.next() => {
				let packet = match incoming {
					Some(Ok(WsMessage::Text(t))) => match engineio::decode(t.as_str()) {
						Ok(p) => p,
						Err(_) => continue,
					},
					Some(Ok(WsMessage::Binary(b))) => EPacket::Binary(b.to_vec()),
					Some(Ok(WsMessage::Ping(_) | WsMessage::Pong(_))) => continue,
					Some(Ok(WsMessage::Close(_))) | None => {
						let _ = handle.inbound.send(In::Closed("client closed".into())).await;
						break;
					}
					Some(Err(e)) => {
						let _ = handle.inbound.send(In::Closed(format!("connection error: {e}"))).await;
						break;
					}
				};
				if handle.inbound.send(In::Packet(packet)).await.is_err() {
					break;
				}
			}
			packets = handle.pipe.take(Duration::from_secs(3600)) => {
				let closing = packets.is_empty() && handle.pipe.is_closed();
				let mut ok = true;
				for p in packets {
					let msg = match p {
						EPacket::Binary(b) => WsMessage::Binary(b.into()),
						other => WsMessage::Text(other.encode().into()),
					};
					let sent = tokio::time::timeout(socket::WRITE_TIMEOUT, sink.send(msg)).await;
					if !matches!(sent, Ok(Ok(()))) {
						ok = false;
						break;
					}
				}
				if !ok {
					let _ = handle.inbound.send(In::Closed("the client could not be written to".into())).await;
					break;
				}
				if closing {
					let _ = tokio::time::timeout(socket::WRITE_TIMEOUT, sink.send(WsMessage::Text(EPacket::Close.encode().into()))).await;
					break;
				}
			}
		}
	}
	let _ = tokio::time::timeout(socket::WRITE_TIMEOUT, sink.close()).await;
}

/// A new session: admitted as a Phoenix socket is (the project, its key, the address cap), then
/// its task started.
async fn open(
	app: &Arc<App>,
	headers: &HeaderMap,
	params: &HashMap<String, String>,
) -> Result<(String, Arc<Handle>), Box<Response>> {
	let accepted = crate::api::admit(app, headers, params).await?;
	let sid = random_id();
	let (tx, rx) = mpsc::channel(INBOUND);
	let handle = Arc::new(Handle {
		tenant: accepted.rt.id.clone(),
		inbound: tx,
		pipe: Pipe::default(),
		reader: AsyncMutex::new(()),
		upgraded: AtomicBool::new(false),
	});
	app.sio.lock().insert(sid.clone(), handle.clone());
	tokio::spawn(run(app.clone(), sid.clone(), handle.clone(), accepted, rx));
	Ok((sid, handle))
}

// --- the session --------------------------------------------------------------------------

/// One connected namespace.
struct Nsp {
	/// The socket id the client sees, and `realtime.socket_id()`.
	sid: String,
	auth: AuthContext,
	/// Rooms, each a topic subscribed on its public and private keys.
	rooms: HashMap<String, [u64; 2]>,
}

struct Session {
	app: Arc<App>,
	rt: Arc<TenantRt>,
	id: u64,
	token: String,
	headers: Map<String, Value>,
	tx: Outbox,
	handle: Arc<Handle>,
	nsps: HashMap<String, Nsp>,
	/// Binary attachments still to arrive for a client packet that is refused anyway.
	skip_attachments: usize,
}

async fn run(
	app: Arc<App>,
	sid: String,
	handle: Arc<Handle>,
	accepted: Accepted,
	mut inbound: mpsc::Receiver<In>,
) {
	let (tx, mut rx) = hub::outbox();
	let id = app.hub.next_id();
	let seen = accepted
		.rt
		.socket_opened(id, accepted.address.clone(), tx.clone());
	accepted
		.rt
		.log
		.push(Event::new(Kind::Connect).socket(id).reason("Socket.IO"));
	let mut s = Session {
		app: app.clone(),
		rt: accepted.rt,
		id,
		token: accepted.token,
		headers: accepted.headers,
		tx,
		handle: handle.clone(),
		nsps: HashMap::new(),
		skip_attachments: 0,
	};
	let interval = Duration::from_millis(engineio::PING_INTERVAL_MS);
	let timeout = Duration::from_millis(engineio::PING_TIMEOUT_MS);
	let mut next_ping = Instant::now() + interval;
	let mut pong_by: Option<Instant> = None;
	let ended = loop {
		if handle.pipe.overflowed.load(Ordering::Acquire) {
			break "the client fell too far behind".to_string();
		}
		let wake = pong_by.unwrap_or(next_ping);
		tokio::select! {
			incoming = inbound.recv() => match incoming {
				Some(In::Packet(p)) => {
					seen.store(inspect::now_ms(), Ordering::Relaxed);
					match p {
						EPacket::Pong(_) => pong_by = None,
						EPacket::Message(m) => s.message(&m).await,
						EPacket::Binary(_) => s.skip_attachments = s.skip_attachments.saturating_sub(1),
						EPacket::Close => break "client closed".to_string(),
						_ => {}
					}
				}
				Some(In::Closed(reason)) => break reason,
				None => break "connection lost".to_string(),
			},
			out = rx.recv() => {
				if rx.overflowed() {
					break "the client fell too far behind".to_string();
				}
				match out {
					Some(Out::Disconnect) | None => {
						break "closed by the server (the project's Realtime settings were reset)".to_string();
					}
					Some(o) => s.deliver(o),
				}
			}
			_ = tokio::time::sleep_until(wake.into()) => {
				if pong_by.is_some() {
					break format!("no pong within {} s", timeout.as_secs());
				}
				handle.pipe.push(EPacket::Ping(String::new()));
				pong_by = Some(Instant::now() + timeout);
				next_ping = Instant::now() + interval;
			}
		}
	};
	let nsps: Vec<String> = s.nsps.keys().cloned().collect();
	for nsp in nsps {
		s.leave_nsp(&nsp, true).await;
	}
	handle.pipe.close();
	app.sio.lock().remove(&sid);
	s.rt.socket_closed(id);
	s.rt.log
		.push(Event::new(Kind::Disconnect).socket(id).reason(ended));
	drain(&mut rx);
}

fn drain(rx: &mut Inbox) {
	while rx.try_recv().is_some() {}
}

impl Session {
	fn send(&self, p: SPacket) {
		self.handle.pipe.push(EPacket::Message(p.encode()));
	}

	fn connect_error(&self, nsp: &str, message: &str, data: Option<Value>) {
		let mut body = json!({ "message": message });
		if let Some(d) = data {
			body["data"] = d;
		}
		self.send(SPacket::new(SKind::ConnectError, nsp, Some(body)));
	}

	fn ack(&self, nsp: &str, id: Option<u64>, value: Value) {
		if let Some(id) = id {
			let mut p = SPacket::new(SKind::Ack, nsp, Some(json!([value])));
			p.id = Some(id);
			self.send(p);
		}
	}

	async fn message(&mut self, text: &str) {
		let p = match socketio::decode(text) {
			Ok(p) => p,
			Err(e) => {
				tracing::debug!(error = %e, "an undecodable Socket.IO packet");
				return;
			}
		};
		match p.kind {
			SKind::Connect => self.connect(p).await,
			SKind::Disconnect => self.leave_nsp(&p.nsp.clone(), true).await,
			SKind::Event => self.event(p, text.len()).await,
			SKind::BinaryEvent => {
				// Refused (H12), and its attachments skipped as they arrive.
				self.skip_attachments = p.attachments;
				let error = handlers::Failure {
					code: "binary_not_supported".into(),
					message: "A handler takes JSON arguments; send bytes as base64".into(),
				};
				self.ack(&p.nsp, p.id, error.to_json());
			}
			_ => {}
		}
	}

	async fn connect(&mut self, p: SPacket) {
		let nsp = p.nsp.clone();
		if let Some(existing) = self.nsps.get(&nsp) {
			let sid = existing.sid.clone();
			self.send(SPacket::new(
				SKind::Connect,
				&nsp,
				Some(json!({ "sid": sid })),
			));
			return;
		}
		let tenant = self.rt.tenant();
		if tenant.database.is_none() {
			self.connect_error(
				&nsp,
				"Socket.IO runs this project's realtime handlers, and this project's plan does not include them",
				None,
			);
			return;
		}
		let auth_object = match p.data {
			Some(Value::Object(m)) => m,
			_ => Map::new(),
		};
		let token = match auth_object.get("token").and_then(Value::as_str) {
			Some(t) if !t.starts_with("sb_") => t.to_string(),
			_ => self.token.clone(),
		};
		let (counted, users) = self.rt.census(self.id);
		if !counted && users as i64 >= tenant.max_concurrent_users {
			self.connect_error(&nsp, "Too many connected users", None);
			return;
		}
		if self.rt.joins.add(1, tenant.max_joins_per_second) {
			self.connect_error(&nsp, "Too many connections per second", None);
			return;
		}
		let claims = match jwt::authorize(&token, &tenant.jwt_secret, now_secs()) {
			Ok(c) => c,
			Err(e) => {
				self.connect_error(&nsp, &e.message(), None);
				return;
			}
		};
		let auth = AuthContext {
			topic: nsp.clone(),
			role: claims
				.get("role")
				.and_then(Value::as_str)
				.unwrap_or_default()
				.to_string(),
			sub: claims
				.get("sub")
				.and_then(Value::as_str)
				.map(str::to_string),
			claims,
			headers: self.headers.clone(),
		};
		let sid = random_id();
		socket::ensure_messages(&self.app, &self.rt).await;
		// The `connection` handler: a refusal is the connect error; its rooms and emits follow
		// the connect, as they do after `io.on("connection")`.
		let mut ops = Vec::new();
		let list = handlers::current(&self.app, &self.rt).await;
		if let Some(handler) = handlers::find(&list, "connection", &nsp).cloned() {
			let mut payload = auth_object.clone();
			payload.remove("token");
			let payload = Value::Object(payload);
			let args = Value::Array(vec![payload.clone()]);
			let call = Call {
				auth: &auth,
				event: "connection",
				payload: &payload,
				args: &args,
				socket_id: Some(&sid),
			};
			match self.invoke(&handler, &call).await {
				Ok(o) => ops = o.ops,
				Err(f) => {
					self.connect_error(&nsp, &f.message, Some(json!({ "code": f.code })));
					return;
				}
			}
		}
		self.nsps.insert(
			nsp.clone(),
			Nsp {
				sid: sid.clone(),
				auth,
				rooms: HashMap::new(),
			},
		);
		self.send(SPacket::new(
			SKind::Connect,
			&nsp,
			Some(json!({ "sid": sid })),
		));
		self.rt.log.push(
			Event::new(Kind::Join)
				.socket(self.id)
				.channel(&format!("socket.io {nsp}")),
		);
		self.join(&nsp, &format!("socket:{sid}"));
		self.apply(&nsp, ops);
	}

	async fn event(&mut self, p: SPacket, size: usize) {
		let Some(n) = self.nsps.get(&p.nsp) else {
			return;
		};
		let auth = n.auth.clone();
		let sid = n.sid.clone();
		let tenant = self.rt.tenant();
		if self.rt.events.add(1, tenant.max_events_per_second) {
			self.server_disconnect(&p.nsp, "Too many messages per second")
				.await;
			return;
		}
		let Some((event, args)) = p.event() else {
			return;
		};
		if size as i64 > payload_limit(tenant.max_payload_size_in_kb) {
			let error = handlers::Failure {
				code: "payload_size_exceeded".into(),
				message: "The message is over this project's payload limit".into(),
			};
			self.ack(&p.nsp, p.id, error.to_json());
			return;
		}
		let list = handlers::current(&self.app, &self.rt).await;
		let Some(handler) = handlers::find(&list, event, &p.nsp).cloned() else {
			// No listener: a Socket.IO server drops it. One that waits for an ack is told, so it
			// does not wait for nothing.
			let error = handlers::Failure {
				code: "no_handler".into(),
				message: format!("No realtime handler is registered for {event} on {}", p.nsp),
			};
			self.ack(&p.nsp, p.id, error.to_json());
			return;
		};
		let payload = args.first().cloned().unwrap_or(Value::Null);
		let args = Value::Array(args.to_vec());
		let call = Call {
			auth: &auth,
			event,
			payload: &payload,
			args: &args,
			socket_id: Some(&sid),
		};
		match self.invoke(&handler, &call).await {
			Ok(o) => {
				self.ack(&p.nsp, p.id, o.result);
				self.apply(&p.nsp, o.ops);
			}
			Err(f) => {
				self.rt.log.push(
					Event::new(Kind::HandlerFailed)
						.socket(self.id)
						.channel(&p.nsp)
						.reason(format!(
							"{event} -> {}: {} {}",
							handler.function, f.code, f.message
						)),
				);
				self.ack(&p.nsp, p.id, f.to_json());
			}
		}
	}

	async fn invoke(
		&self,
		handler: &handlers::Handler,
		call: &Call<'_>,
	) -> Result<handlers::Outcome, handlers::Failure> {
		match handlers::pool(&self.app, &self.rt).await {
			Ok(pool) => handlers::invoke(&pool, handler, call).await,
			Err(message) => Err(handlers::Failure {
				code: "unavailable".into(),
				message,
			}),
		}
	}

	/// What a handler asked for, in order.
	fn apply(&mut self, nsp: &str, ops: Vec<Op>) {
		for op in ops {
			match op {
				Op::Join(room) => self.join(nsp, &room),
				Op::Leave(room) => self.leave(nsp, &room),
				Op::Emit {
					topic,
					event,
					payload,
					except_self,
				} => {
					let except = except_self.then_some(self.id);
					handlers::deliver_emit(&self.rt, &topic, &event, payload, except);
				}
			}
		}
	}

	fn join(&mut self, nsp: &str, room: &str) {
		let max = self.rt.tenant().max_channels_per_client;
		let Some(n) = self.nsps.get_mut(nsp) else {
			return;
		};
		if n.rooms.contains_key(room) {
			return;
		}
		// `socket:<sid>` is not counted against the project's channels per client.
		if n.rooms.len() as i64 > max.max(1) {
			self.rt.log.push(
				Event::new(Kind::JoinRefused)
					.socket(self.id)
					.channel(room)
					.reason("Too many rooms"),
			);
			return;
		}
		let presence_key: Arc<str> = Arc::from(n.sid.as_str());
		let mut ids = [0u64; 2];
		for (i, private) in [false, true].into_iter().enumerate() {
			ids[i] = self.app.hub.next_id();
			self.rt.subscribe(
				&TopicKey {
					name: room.to_string(),
					private,
				},
				Sub {
					id: ids[i],
					socket: self.id,
					join_topic: nsp.to_string(),
					tx: self.tx.clone(),
					replayed: Arc::new(HashSet::new()),
					presence_key: presence_key.clone(),
					joined_at: inspect::now_ms(),
				},
			);
		}
		n.rooms.insert(room.to_string(), ids);
		self.rt.joined(self.id);
	}

	fn leave(&mut self, nsp: &str, room: &str) {
		let Some(ids) = self.nsps.get_mut(nsp).and_then(|n| n.rooms.remove(room)) else {
			return;
		};
		for (i, private) in [false, true].into_iter().enumerate() {
			let key = TopicKey {
				name: room.to_string(),
				private,
			};
			self.rt.unsubscribe(&key, ids[i]);
		}
		self.rt.left(self.id);
	}

	/// The server ends one namespace and says so.
	async fn server_disconnect(&mut self, nsp: &str, reason: &str) {
		self.rt.log.push(
			Event::new(Kind::ChannelClosed)
				.socket(self.id)
				.channel(nsp)
				.reason(reason),
		);
		self.send(SPacket::new(SKind::Disconnect, nsp, None));
		self.leave_nsp(nsp, true).await;
	}

	/// Leave every room of a namespace, then run its `disconnect` handler (best effort).
	async fn leave_nsp(&mut self, nsp: &str, run_handler: bool) {
		let Some(n) = self.nsps.get(nsp) else {
			return;
		};
		let rooms: Vec<String> = n.rooms.keys().cloned().collect();
		for room in rooms {
			self.leave(nsp, &room);
		}
		let Some(n) = self.nsps.remove(nsp) else {
			return;
		};
		self.rt.log.push(
			Event::new(Kind::Leave)
				.socket(self.id)
				.channel(&format!("socket.io {nsp}")),
		);
		if !run_handler {
			return;
		}
		let list = handlers::current(&self.app, &self.rt).await;
		let Some(handler) = handlers::find(&list, "disconnect", nsp).cloned() else {
			return;
		};
		let payload = Value::Null;
		let args = json!([]);
		let call = Call {
			auth: &n.auth,
			event: "disconnect",
			payload: &payload,
			args: &args,
			socket_id: Some(&n.sid),
		};
		match tokio::time::timeout(DISCONNECT_WITHIN, self.invoke(&handler, &call)).await {
			Ok(Ok(o)) => {
				// Its emits still go out; its rooms are moot.
				let emits = o
					.ops
					.into_iter()
					.filter(|op| matches!(op, Op::Emit { .. }))
					.collect();
				self.apply(nsp, emits);
			}
			Ok(Err(f)) => self.rt.log.push(
				Event::new(Kind::HandlerFailed)
					.socket(self.id)
					.channel(nsp)
					.reason(format!(
						"disconnect -> {}: {} {}",
						handler.function, f.code, f.message
					)),
			),
			Err(_) => {}
		}
	}

	/// What the hub queued, as Socket.IO events on the namespace that holds the room.
	fn deliver(&self, out: Out) {
		match out {
			Out::Broadcast {
				join_topic,
				event,
				payload,
				..
			} => {
				if event != "broadcast" || !self.nsps.contains_key(&join_topic) {
					return;
				}
				let Some(name) = payload.get("event").and_then(Value::as_str) else {
					return;
				};
				let body = payload.get("payload").cloned().unwrap_or(Value::Null);
				self.send(SPacket::new(
					SKind::Event,
					&join_topic,
					Some(json!([name, body])),
				));
			}
			Out::User {
				join_topic,
				event,
				encoding,
				payload,
				..
			} => {
				if !self.nsps.contains_key(&join_topic) {
					return;
				}
				match encoding {
					Encoding::Json => {
						let body = serde_json::from_slice::<Value>(&payload).unwrap_or(Value::Null);
						self.send(SPacket::new(
							SKind::Event,
							&join_topic,
							Some(json!([event.as_ref(), body])),
						));
					}
					Encoding::Binary => {
						let (header, bytes) =
							socketio::binary_event(&join_topic, &event, payload.to_vec());
						self.send(header);
						self.handle.pipe.push(EPacket::Binary(bytes));
					}
				}
			}
			Out::Changes { .. } | Out::System { .. } | Out::Disconnect => {}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn a_pipe_hands_over_what_is_waiting_and_wakes_a_waiting_reader() {
		let pipe = Arc::new(Pipe::default());
		pipe.push(EPacket::Noop);
		assert_eq!(
			pipe.take(Duration::from_millis(10)).await,
			vec![EPacket::Noop]
		);
		assert!(pipe.take(Duration::from_millis(10)).await.is_empty());
		let reader = {
			let pipe = pipe.clone();
			tokio::spawn(async move { pipe.take(Duration::from_secs(5)).await })
		};
		tokio::time::sleep(Duration::from_millis(20)).await;
		pipe.push(EPacket::Message("x".into()));
		assert_eq!(reader.await.unwrap(), vec![EPacket::Message("x".into())]);
	}

	#[tokio::test]
	async fn a_closed_pipe_returns_at_once_and_takes_nothing_more() {
		let pipe = Pipe::default();
		pipe.close();
		assert!(pipe.take(Duration::from_secs(5)).await.is_empty());
		pipe.push(EPacket::Noop);
		assert!(pipe.drain().is_empty());
	}

	#[test]
	fn a_pipe_past_its_bound_overflows() {
		let pipe = Pipe::default();
		for _ in 0..MAX_QUEUED {
			pipe.push(EPacket::Noop);
		}
		assert!(!pipe.overflowed.load(Ordering::Acquire));
		pipe.push(EPacket::Noop);
		assert!(pipe.overflowed.load(Ordering::Acquire));
	}

	#[test]
	fn ids_are_long_random_and_url_safe() {
		let a = random_id();
		assert_eq!(a.len(), 20);
		assert!(
			a.chars()
				.all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
		);
		assert_ne!(a, random_id());
	}
}
