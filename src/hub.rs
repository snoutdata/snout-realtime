//! Who is subscribed to what, per project: the topics, the subscribers on each, presence, the
//! census of connected users, the rate windows and the counters `/metrics` reports.
//!
//! One process, no cluster: a broadcast is a walk over a topic's subscribers, each of which is a
//! socket's outbound queue. Nothing here touches a database or a socket; `socket.rs` and the
//! database streams call in.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde_json::Value;
use tokio::sync::mpsc::UnboundedSender;

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

/// A subscriber on a topic: one channel on one socket.
#[derive(Debug, Clone)]
pub struct Sub {
	pub id: SubscriberId,
	pub socket: u64,
	pub join_topic: String,
	pub tx: UnboundedSender<Out>,
	/// Message ids replayed at join, not to be delivered again live.
	pub replayed: Arc<HashSet<String>>,
}

#[derive(Default)]
struct TopicRt {
	subs: Vec<Sub>,
	presence: presence::Topic,
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

/// An open socket: the client's address, and its queue.
type SocketEntry = (Option<String>, UnboundedSender<Out>);

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
		let payload = Arc::new(diff.payload());
		let subs = self.subscribers(key, except);
		self.counters
			.presence_events
			.fetch_add(subs.len() as u64, Ordering::Relaxed);
		for s in subs {
			let _ = s.tx.send(Out::Broadcast {
				join_topic: s.join_topic.clone(),
				event: "presence_diff".into(),
				payload: payload.clone(),
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

	pub fn socket_opened(&self, socket: u64, address: Option<String>, tx: UnboundedSender<Out>) {
		self.sockets
			.lock()
			.unwrap_or_else(|e| e.into_inner())
			.insert(socket, (address, tx));
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
				.filter(|(a, _)| a.as_deref() == Some(address))
				.count(),
		)
	}

	pub fn connections(&self) -> usize {
		self.sockets.lock().unwrap_or_else(|e| e.into_inner()).len()
	}

	/// Close every socket (the tenant was deleted).
	pub fn disconnect_all(&self) {
		for (_, tx) in self
			.sockets
			.lock()
			.unwrap_or_else(|e| e.into_inner())
			.values()
		{
			let _ = tx.send(Out::Disconnect);
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
		let _ = s.tx.send(Out::User {
			join_topic: s.join_topic.clone(),
			event: event.clone(),
			encoding,
			payload: payload.clone(),
			metadata: metadata.clone(),
			id: id.clone(),
		});
	}
	rt.counters.events.fetch_add(n as u64, Ordering::Relaxed);
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
	let payload = Arc::new(payload);
	let n = subs.len();
	for s in subs {
		let _ = s.tx.send(Out::Broadcast {
			join_topic: s.join_topic.clone(),
			event: "broadcast".into(),
			payload: payload.clone(),
		});
	}
	rt.counters.events.fetch_add(n as u64, Ordering::Relaxed);
	n
}
