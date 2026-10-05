//! Who is subscribed to what, per project: the topics, the subscribers on each, presence, the
//! census of connected users, the rate windows and the counters `/metrics` reports.
//!
//! One process, no cluster: a broadcast is a walk over a topic's subscribers, each of which is a
//! socket's outbound queue. Nothing here touches a database or a socket; `socket.rs` and the
//! database streams call in.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use serde_json::Value;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use crate::inspect;
use crate::presence::{self, Diff, SubscriberId};
use crate::protocol::Encoding;
use crate::tenants::Tenant;

/// A topic inside one project: the channel's name and whether it is private. A public and a
/// private channel of the same name are different topics.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicKey {
	pub name: String,
	pub private: bool,
}

/// What a socket is asked to send. Serialised by the socket, for its own serializer.
#[derive(Debug, Clone)]
pub enum Out {
	/// A broadcast with a JSON payload: `[null, null, topic, event, payload]`.
	Broadcast {
		join_topic: String,
		event: String,
		payload: Arc<Value>,
		/// The payload's serialised length, measured once for every subscriber.
		size: usize,
	},
	/// A user broadcast whose payload is carried as it arrived (V2 binary kind 4).
	User {
		join_topic: String,
		event: Arc<str>,
		encoding: Encoding,
		payload: Arc<[u8]>,
		metadata: Option<Arc<Value>>,
		id: Option<Arc<str>>,
	},
	/// Database changes for this subscriber's bindings: `{ ids, data }`.
	Changes {
		join_topic: String,
		ids: Vec<i64>,
		data: Arc<str>,
	},
	/// A system message on one channel (a postgres_changes subscription's outcome, a shutdown).
	System {
		join_topic: String,
		extension: String,
		status: String,
		message: String,
		stop: bool,
	},
	/// Close the socket (the tenant was deleted).
	Disconnect,
}

impl Out {
	/// Roughly what holding this in a queue costs, in bytes. Shared payloads are counted on every
	/// queue holding them, which overstates the memory and never understates it.
	fn cost(&self) -> usize {
		const ENVELOPE: usize = 64;
		ENVELOPE
			+ match self {
				Out::Broadcast {
					join_topic,
					event,
					size,
					..
				} => join_topic.len() + event.len() + size,
				Out::User {
					join_topic,
					event,
					payload,
					..
				} => join_topic.len() + event.len() + payload.len(),
				Out::Changes {
					join_topic,
					ids,
					data,
				} => join_topic.len() + ids.len() * 8 + data.len(),
				Out::System {
					join_topic,
					message,
					..
				} => join_topic.len() + message.len(),
				Out::Disconnect => 0,
			}
	}
}

/// Messages one socket may have waiting to be written before it is dropped.
pub const MAX_QUEUED: usize = 8192;
/// Bytes one socket may have waiting to be written before it is dropped. One message larger
/// than this is still accepted into an empty queue.
pub const MAX_QUEUED_BYTES: usize = 16 << 20;

#[derive(Debug)]
struct QueueState {
	len: AtomicUsize,
	bytes: AtomicUsize,
	overflowed: AtomicBool,
	max_len: usize,
	max_bytes: usize,
}

/// The sending half of one socket's outbound queue. Bounded by count and by bytes: a socket
/// whose client stops reading would otherwise hold everything sent to it, at the database's
/// write speed, until the process (and every project on the host) ran out of memory. Past the
/// bound the queue refuses everything and the socket closes itself.
#[derive(Debug, Clone)]
pub struct Outbox {
	tx: UnboundedSender<Out>,
	state: Arc<QueueState>,
}

/// The receiving half, read by the socket's own task.
#[derive(Debug)]
pub struct Inbox {
	rx: UnboundedReceiver<Out>,
	state: Arc<QueueState>,
}

/// A socket's outbound queue at the standard bounds.
pub fn outbox() -> (Outbox, Inbox) {
	outbox_with(MAX_QUEUED, MAX_QUEUED_BYTES)
}

pub fn outbox_with(max_len: usize, max_bytes: usize) -> (Outbox, Inbox) {
	let (tx, rx) = unbounded_channel();
	let state = Arc::new(QueueState {
		len: AtomicUsize::new(0),
		bytes: AtomicUsize::new(0),
		overflowed: AtomicBool::new(false),
		max_len,
		max_bytes,
	});
	(
		Outbox {
			tx,
			state: state.clone(),
		},
		Inbox { rx, state },
	)
}

impl Outbox {
	/// Queue `out`; false when it was not (the socket is gone, or too far behind). `Disconnect`
	/// is never refused.
	pub fn send(&self, out: Out) -> bool {
		if matches!(out, Out::Disconnect) {
			return self.tx.send(out).is_ok();
		}
		let s = &self.state;
		if s.overflowed.load(Ordering::Acquire) {
			return false;
		}
		let cost = out.cost();
		let len = s.len.fetch_add(1, Ordering::AcqRel) + 1;
		let bytes = s.bytes.fetch_add(cost, Ordering::AcqRel) + cost;
		if len > s.max_len || (bytes > s.max_bytes && len > 1) {
			s.len.fetch_sub(1, Ordering::AcqRel);
			s.bytes.fetch_sub(cost, Ordering::AcqRel);
			s.overflowed.store(true, Ordering::Release);
			return false;
		}
		if self.tx.send(out).is_err() {
			s.len.fetch_sub(1, Ordering::AcqRel);
			s.bytes.fetch_sub(cost, Ordering::AcqRel);
			return false;
		}
		true
	}
}

impl Inbox {
	pub async fn recv(&mut self) -> Option<Out> {
		let out = self.rx.recv().await?;
		self.release(&out);
		Some(out)
	}

	pub fn try_recv(&mut self) -> Option<Out> {
		let out = self.rx.try_recv().ok()?;
		self.release(&out);
		Some(out)
	}

	fn release(&self, out: &Out) {
		if !matches!(out, Out::Disconnect) {
			self.state.len.fetch_sub(1, Ordering::AcqRel);
			self.state.bytes.fetch_sub(out.cost(), Ordering::AcqRel);
		}
	}

	/// Whether a send was refused for the bound: the socket has fallen too far behind.
	pub fn overflowed(&self) -> bool {
		self.state.overflowed.load(Ordering::Acquire)
	}

	/// Messages and bytes waiting.
	pub fn queued(&self) -> (usize, usize) {
		(
			self.state.len.load(Ordering::Acquire),
			self.state.bytes.load(Ordering::Acquire),
		)
	}
}

/// A subscriber on a topic: one channel on one socket.
#[derive(Debug, Clone)]
pub struct Sub {
	pub id: SubscriberId,
	pub socket: u64,
	pub join_topic: String,
	pub tx: Outbox,
	/// Message ids replayed at join, not to be delivered again live.
	pub replayed: Arc<HashSet<String>>,
	/// The presence key this channel tracks under (the client's, or one made for it).
	pub presence_key: Arc<str>,
	/// When it joined, in milliseconds since the epoch.
	pub joined_at: i64,
}

#[derive(Default)]
struct TopicRt {
	subs: Vec<Sub>,
	presence: presence::Topic,
	stats: inspect::Window,
}

/// Events per second over a sliding one-second window.
#[derive(Debug, Default)]
pub struct Rate {
	window: std::sync::Mutex<(Option<Instant>, u64)>,
}

impl Rate {
	/// Count `n` and report whether the second's total is over `limit`.
	pub fn add(&self, n: u64, limit: i64) -> bool {
		let mut w = self.window.lock().unwrap_or_else(|e| e.into_inner());
		let now = Instant::now();
		match w.0 {
			Some(start) if now.duration_since(start) < Duration::from_secs(1) => w.1 += n,
			_ => *w = (Some(now), n),
		}
		limit >= 0 && w.1 > limit as u64
	}
}

/// The counters `/metrics` reports per project, as running totals.
#[derive(Debug, Default)]
pub struct Counters {
	pub events: AtomicU64,
	pub presence_events: AtomicU64,
	pub db_events: AtomicU64,
	pub joins: AtomicU64,
	pub output_bytes: AtomicU64,
}

/// An open socket: the client's address, its queue, and when a frame last arrived from it.
type SocketEntry = (Option<String>, Outbox, Arc<AtomicI64>);

/// A user broadcast to deliver: its event, its payload as it arrived, and the metadata a stored
/// message carries (`{ id }`), whose id also keeps a replayed message from arriving twice.
pub struct UserMessage<'a> {
	pub event: &'a str,
	pub encoding: Encoding,
	pub payload: &'a [u8],
	pub metadata: Option<Value>,
	pub id: Option<String>,
}

/// One project's live state.
pub struct TenantRt {
	pub id: String,
	tenant: std::sync::RwLock<Arc<Tenant>>,
	topics: std::sync::Mutex<HashMap<TopicKey, TopicRt>>,
	/// Channels joined per socket. A socket is a connected user while it has at least one
	/// (not from its first join ATTEMPT, which would count a refused join, until the socket
	/// closed).
	census: std::sync::Mutex<HashMap<u64, usize>>,
	/// Open sockets.
	sockets: std::sync::Mutex<HashMap<u64, SocketEntry>>,
	pub joins: Rate,
	pub events: Rate,
	pub presence: Rate,
	pub counters: Counters,
	/// Connections coming and going, for the project's owner (`inspect.rs`).
	pub log: inspect::EventLog,
	/// Set up once per process: the realtime schema and the message partitions.
	pub prepared: tokio::sync::Mutex<bool>,
	/// The database streams, started on first need.
	pub streams: tokio::sync::Mutex<Streams>,
}

#[derive(Default)]
pub struct Streams {
	pub messages: Option<tokio::task::JoinHandle<()>>,
	pub changes: Option<Arc<crate::changes::Changes>>,
}

impl TenantRt {
	/// The tenant as last registered. Read at each check, so a raised limit applies to
	/// channels already joined.
	pub fn tenant(&self) -> Arc<Tenant> {
		self.tenant
			.read()
			.unwrap_or_else(|e| e.into_inner())
			.clone()
	}

	pub fn set_tenant(&self, tenant: Arc<Tenant>) {
		*self.tenant.write().unwrap_or_else(|e| e.into_inner()) = tenant;
	}

	fn topics(&self) -> std::sync::MutexGuard<'_, HashMap<TopicKey, TopicRt>> {
		self.topics.lock().unwrap_or_else(|e| e.into_inner())
	}

	pub fn subscribe(&self, key: &TopicKey, sub: Sub) {
		self.topics().entry(key.clone()).or_default().subs.push(sub);
	}

	/// Take a subscriber off a topic, with everything it tracked; the diff to broadcast.
	pub fn unsubscribe(&self, key: &TopicKey, id: SubscriberId) -> Diff {
		let mut topics = self.topics();
		let Some(topic) = topics.get_mut(key) else {
			return Diff {
				joins: Vec::new(),
				leaves: Vec::new(),
			};
		};
		topic.subs.retain(|s| s.id != id);
		let diff = topic.presence.remove(id);
		if topic.subs.is_empty() && topic.presence.is_empty() {
			topics.remove(key);
		}
		diff
	}

	/// Every subscriber on a topic (optionally not `except`), for a delivery.
	pub fn subscribers(&self, key: &TopicKey, except: Option<SubscriberId>) -> Vec<Sub> {
		self.topics()
			.get(key)
			.map(|t| {
				t.subs
					.iter()
					.filter(|s| Some(s.id) != except)
					.cloned()
					.collect()
			})
			.unwrap_or_default()
	}

	/// The subscribers of every topic of this project named `name` with a given privacy.
	pub fn presence_state(&self, key: &TopicKey) -> Value {
		self.topics()
			.get(key)
			.map(|t| t.presence.state())
			.unwrap_or_else(|| Value::Object(Default::default()))
	}

	pub fn track(
		&self,
		key: &TopicKey,
		id: SubscriberId,
		presence_key: &str,
		payload: serde_json::Map<String, Value>,
	) -> Diff {
		let mut topics = self.topics();
		let topic = topics.entry(key.clone()).or_default();
		topic
			.presence
			.track(id, presence_key, payload, presence::new_ref())
	}

	pub fn untrack(&self, key: &TopicKey, id: SubscriberId, presence_key: &str) -> Diff {
		self.topics()
			.get_mut(key)
			.map(|t| t.presence.untrack(id, presence_key))
			.unwrap_or(Diff {
				joins: Vec::new(),
				leaves: Vec::new(),
			})
	}

	pub fn tracked(
		&self,
		key: &TopicKey,
		id: SubscriberId,
		presence_key: &str,
	) -> Option<serde_json::Map<String, Value>> {
		self.topics()
			.get(key)
			.and_then(|t| t.presence.tracked(id, presence_key))
	}

	/// Send a presence diff to every subscriber of the topic.
	pub fn broadcast_diff(&self, key: &TopicKey, diff: &Diff) {
		self.broadcast_diff_except(key, diff, None);
	}

	/// The same, to everybody but `except` (who is sent it directly, in order with a reply).
	pub fn broadcast_diff_except(&self, key: &TopicKey, diff: &Diff, except: Option<SubscriberId>) {
		if diff.is_empty() {
			return;
		}
		let payload = diff.payload();
		let size = payload.to_string().len();
		let payload = Arc::new(payload);
		let subs = self.subscribers(key, except);
		self.record(key, |w| w.presence(subs.len()));
		self.counters
			.presence_events
			.fetch_add(subs.len() as u64, Ordering::Relaxed);
		for s in subs {
			s.tx.send(Out::Broadcast {
				join_topic: s.join_topic.clone(),
				event: "presence_diff".into(),
				payload: payload.clone(),
				size,
			});
		}
	}

	/// Is this socket already a connected user, and how many are there?
	pub fn census(&self, socket: u64) -> (bool, usize) {
		let c = self.census.lock().unwrap_or_else(|e| e.into_inner());
		(c.contains_key(&socket), c.len())
	}

	pub fn joined(&self, socket: u64) {
		*self
			.census
			.lock()
			.unwrap_or_else(|e| e.into_inner())
			.entry(socket)
			.or_default() += 1;
	}

	pub fn left(&self, socket: u64) {
		let mut c = self.census.lock().unwrap_or_else(|e| e.into_inner());
		if let Some(n) = c.get_mut(&socket) {
			*n -= 1;
			if *n == 0 {
				c.remove(&socket);
			}
		}
	}

	/// A socket opened; returns the clock its frames from the client are stamped on.
	pub fn socket_opened(
		&self,
		socket: u64,
		address: Option<String>,
		tx: Outbox,
	) -> Arc<AtomicI64> {
		let seen = Arc::new(AtomicI64::new(inspect::now_ms()));
		self.sockets
			.lock()
			.unwrap_or_else(|e| e.into_inner())
			.insert(socket, (address, tx, seen.clone()));
		seen
	}

	/// Count something on a topic's message window, if the topic is open.
	fn record(&self, key: &TopicKey, f: impl FnOnce(&mut inspect::Window)) {
		if let Some(t) = self.topics().get_mut(key) {
			f(&mut t.stats);
		}
	}

	/// What is open now: every channel (or the one named), who is on it, its presence and its
	/// last minute of messages. A client is named by its socket and presence key; the address
	/// it came from is not shown, because that is somebody's IP address.
	pub fn inspect(&self, channel: Option<&str>) -> Value {
		let now = inspect::now_ms();
		let seen: HashMap<u64, i64> = self
			.sockets
			.lock()
			.unwrap_or_else(|e| e.into_inner())
			.iter()
			.map(|(id, (_, _, at))| (*id, at.load(Ordering::Relaxed)))
			.collect();
		let users = self.census.lock().unwrap_or_else(|e| e.into_inner()).len();
		let topics = self.topics();
		let mut keys: Vec<&TopicKey> = topics
			.keys()
			.filter(|k| channel.is_none_or(|c| k.name == c))
			.collect();
		keys.sort_by(|a, b| (&a.name, a.private).cmp(&(&b.name, b.private)));
		let channels: Vec<Value> = keys
			.into_iter()
			.map(|k| {
				let t = &topics[k];
				let clients: Vec<Value> = t
					.subs
					.iter()
					.map(|s| {
						let last = seen.get(&s.socket).copied();
						serde_json::json!({
							"socket": s.socket,
							"presence_key": &*s.presence_key,
							"joined_at": s.joined_at,
							"last_seen_ms_ago": last.map(|at| (now - at).max(0)),
						})
					})
					.collect();
				serde_json::json!({
					"name": k.name,
					"private": k.private,
					"clients": clients,
					"presence": t.presence.state(),
					"messages": t.stats.summary(),
				})
			})
			.collect();
		drop(topics);
		let tenant = self.tenant();
		serde_json::json!({
			"at": now,
			"connections": seen.len(),
			"connected_users": users,
			"limits": {
				"max_events_per_second": tenant.max_events_per_second,
				"max_concurrent_users": tenant.max_concurrent_users,
				"max_channels_per_client": tenant.max_channels_per_client,
				"max_joins_per_second": tenant.max_joins_per_second,
			},
			"channels": channels,
		})
	}

	pub fn socket_closed(&self, socket: u64) {
		self.sockets
			.lock()
			.unwrap_or_else(|e| e.into_inner())
			.remove(&socket);
		self.census
			.lock()
			.unwrap_or_else(|e| e.into_inner())
			.remove(&socket);
	}

	/// Open sockets, and how many of them come from `address`.
	pub fn sockets_from(&self, address: &str) -> (usize, usize) {
		let s = self.sockets.lock().unwrap_or_else(|e| e.into_inner());
		(
			s.len(),
			s.values()
				.filter(|(a, _, _)| a.as_deref() == Some(address))
				.count(),
		)
	}

	pub fn connections(&self) -> usize {
		self.sockets.lock().unwrap_or_else(|e| e.into_inner()).len()
	}

	/// Close every socket (the tenant was deleted).
	pub fn disconnect_all(&self) {
		for (_, tx, _) in self
			.sockets
			.lock()
			.unwrap_or_else(|e| e.into_inner())
			.values()
		{
			tx.send(Out::Disconnect);
		}
	}
}

/// Every project this process has seen a socket or a registration for.
#[derive(Default)]
pub struct Hub {
	tenants: std::sync::Mutex<HashMap<String, Arc<TenantRt>>>,
	next: AtomicU64,
}

impl Hub {
	pub fn next_id(&self) -> u64 {
		self.next.fetch_add(1, Ordering::Relaxed) + 1
	}

	/// A project's live state, made on first use.
	pub fn tenant(&self, tenant: Arc<Tenant>) -> Arc<TenantRt> {
		let mut all = self.tenants.lock().unwrap_or_else(|e| e.into_inner());
		if let Some(rt) = all.get(&tenant.external_id) {
			rt.set_tenant(tenant);
			return rt.clone();
		}
		let rt = Arc::new(TenantRt {
			id: tenant.external_id.clone(),
			tenant: std::sync::RwLock::new(tenant.clone()),
			topics: Default::default(),
			census: Default::default(),
			sockets: Default::default(),
			joins: Rate::default(),
			events: Rate::default(),
			presence: Rate::default(),
			counters: Counters::default(),
			log: inspect::EventLog::default(),
			prepared: tokio::sync::Mutex::new(false),
			streams: tokio::sync::Mutex::new(Streams::default()),
		});
		all.insert(tenant.external_id.clone(), rt.clone());
		rt
	}

	pub fn get(&self, id: &str) -> Option<Arc<TenantRt>> {
		self.tenants
			.lock()
			.unwrap_or_else(|e| e.into_inner())
			.get(id)
			.cloned()
	}

	pub fn remove(&self, id: &str) -> Option<Arc<TenantRt>> {
		self.tenants
			.lock()
			.unwrap_or_else(|e| e.into_inner())
			.remove(id)
	}

	pub fn all(&self) -> Vec<Arc<TenantRt>> {
		self.tenants
			.lock()
			.unwrap_or_else(|e| e.into_inner())
			.values()
			.cloned()
			.collect()
	}
}

/// Deliver a user broadcast to a topic's subscribers, skipping `except` (the sender, unless it
/// asked for its own) and any that already had this message replayed.
pub fn deliver_user(
	rt: &TenantRt,
	key: &TopicKey,
	except: Option<SubscriberId>,
	message: UserMessage<'_>,
) -> usize {
	let subs = rt.subscribers(key, except);
	let event: Arc<str> = Arc::from(message.event);
	let encoding = message.encoding;
	let payload: Arc<[u8]> = Arc::from(message.payload);
	let metadata = message.metadata.map(Arc::new);
	let id: Option<Arc<str>> = message.id.map(Arc::from);
	let mut n = 0;
	for s in subs {
		if let Some(id) = &id
			&& s.replayed.contains(id.as_ref())
		{
			continue;
		}
		n += 1;
		s.tx.send(Out::User {
			join_topic: s.join_topic.clone(),
			event: event.clone(),
			encoding,
			payload: payload.clone(),
			metadata: metadata.clone(),
			id: id.clone(),
		});
	}
	rt.counters.events.fetch_add(n as u64, Ordering::Relaxed);
	rt.record(key, |w| w.broadcast(n));
	n
}

/// Deliver a JSON broadcast (`{type, event, payload}`) to a topic's subscribers.
pub fn deliver_json(
	rt: &TenantRt,
	key: &TopicKey,
	except: Option<SubscriberId>,
	payload: Value,
) -> usize {
	let subs = rt.subscribers(key, except);
	let size = payload.to_string().len();
	let payload = Arc::new(payload);
	let n = subs.len();
	for s in subs {
		s.tx.send(Out::Broadcast {
			join_topic: s.join_topic.clone(),
			event: "broadcast".into(),
			payload: payload.clone(),
			size,
		});
	}
	rt.counters.events.fetch_add(n as u64, Ordering::Relaxed);
	rt.record(key, |w| w.broadcast(n));
	n
}

#[cfg(test)]
mod tests {
	use super::*;

	fn change(bytes: usize) -> Out {
		Out::Changes {
			join_topic: "realtime:x".into(),
			ids: vec![1],
			data: Arc::from("a".repeat(bytes)),
		}
	}

	#[tokio::test]
	async fn a_queue_past_its_count_refuses_and_says_so() {
		let (tx, mut rx) = outbox_with(3, usize::MAX);
		for _ in 0..3 {
			assert!(tx.send(change(1)));
		}
		assert!(!rx.overflowed());
		assert!(!tx.send(change(1)));
		assert!(rx.overflowed());
		// Once over, it stays over: the socket is dropped, not slowed.
		assert!(rx.recv().await.is_some());
		assert!(!tx.send(change(1)));
		assert_eq!(rx.queued().0, 2);
	}

	#[tokio::test]
	async fn a_queue_past_its_bytes_refuses() {
		let (tx, rx) = outbox_with(usize::MAX, 10_000);
		assert!(tx.send(change(4_000)));
		assert!(tx.send(change(4_000)));
		assert!(!tx.send(change(4_000)));
		assert!(rx.overflowed());
	}

	#[tokio::test]
	async fn one_message_larger_than_the_bound_still_reaches_an_empty_queue() {
		let (tx, mut rx) = outbox_with(10, 1_000);
		assert!(tx.send(change(5_000)));
		assert!(!rx.overflowed());
		assert!(rx.recv().await.is_some());
		assert_eq!(rx.queued(), (0, 0));
		assert!(tx.send(change(5_000)));
	}

	#[tokio::test]
	async fn reading_makes_room() {
		let (tx, mut rx) = outbox_with(2, usize::MAX);
		for _ in 0..10 {
			assert!(tx.send(change(10)));
			assert!(tx.send(change(10)));
			assert!(rx.recv().await.is_some());
			assert!(rx.recv().await.is_some());
		}
		assert!(!rx.overflowed());
		assert_eq!(rx.queued(), (0, 0));
	}

	#[tokio::test]
	async fn a_disconnect_is_never_refused() {
		let (tx, mut rx) = outbox_with(1, usize::MAX);
		assert!(tx.send(change(1)));
		assert!(!tx.send(change(1)));
		assert!(tx.send(Out::Disconnect));
		assert!(matches!(rx.recv().await, Some(Out::Changes { .. })));
		assert!(matches!(rx.recv().await, Some(Out::Disconnect)));
	}

	#[test]
	fn a_queue_whose_socket_is_gone_refuses_without_counting() {
		let (tx, rx) = outbox_with(10, usize::MAX);
		drop(rx);
		assert!(!tx.send(change(1)));
		assert_eq!(tx.state.len.load(Ordering::Acquire), 0);
	}
}
