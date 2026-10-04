//! What a project's owner can see of its Realtime: the channels open now, who is on each, and a
//! short log of connections coming and going with the reason each one ended.
//!
//! Before this, a project had nowhere to look. "A friend disappeared from the game" could only
//! be answered by adding logging to the game and reproducing it with a bot, because the server
//! knew exactly what had happened and kept none of it. Everything here is in memory, bounded,
//! and per project: a restart forgets it, and it is never a substitute for the client's own
//! view, only the server's side of the same story.
//!
//! Served at `GET /socket/inspect` and `GET /socket/events` (so `/realtime/v1/inspect` and
//! `/realtime/v1/events` through the front door), to the project's `service_role` key only.

use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde_json::{Value, json};

/// How many connection events a project keeps. Old ones fall off the front.
pub const LOG_CAPACITY: usize = 1000;
/// The window message counts are reported over, in seconds.
pub const WINDOW_SECONDS: usize = 60;

pub fn now_ms() -> i64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map(|d| d.as_millis() as i64)
		.unwrap_or(0)
}

/// What happened.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
	/// A socket was accepted.
	Connect,
	/// A socket was refused at the door (a bad key, too many from one address).
	ConnectRefused,
	/// A channel was joined.
	Join,
	/// A join was refused, with the reason the client was sent.
	JoinRefused,
	/// The client left a channel (`phx_leave`).
	Leave,
	/// The server closed a channel and said why (a rate limit, an expired token).
	ChannelClosed,
	/// A socket ended, with how.
	Disconnect,
}

/// One entry in the log.
#[derive(Debug, Clone, Serialize)]
pub struct Event {
	/// Milliseconds since the epoch.
	pub at: i64,
	pub kind: Kind,
	/// The socket, as a number that is stable for its life and means nothing else.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub socket: Option<u64>,
	/// The channel's name, without `realtime:`.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub channel: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub presence_key: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub reason: Option<String>,
}

impl Event {
	pub fn new(kind: Kind) -> Self {
		Event {
			at: now_ms(),
			kind,
			socket: None,
			channel: None,
			presence_key: None,
			reason: None,
		}
	}

	pub fn socket(mut self, id: u64) -> Self {
		self.socket = Some(id);
		self
	}

	pub fn channel(mut self, name: &str) -> Self {
		self.channel = Some(name.to_string());
		self
	}

	pub fn presence_key(mut self, key: &str) -> Self {
		self.presence_key = Some(key.to_string());
		self
	}

	pub fn reason(mut self, reason: impl Into<String>) -> Self {
		self.reason = Some(reason.into());
		self
	}
}

/// A project's recent connection events, oldest first.
#[derive(Debug, Default)]
pub struct EventLog {
	ring: std::sync::Mutex<VecDeque<Event>>,
}

impl EventLog {
	pub fn push(&self, event: Event) {
		let mut ring = self.ring.lock().unwrap_or_else(|e| e.into_inner());
		if ring.len() >= LOG_CAPACITY {
			ring.pop_front();
		}
		ring.push_back(event);
	}

	/// Events at or after `since` (ms), optionally on one channel. A socket-wide event (a
	/// connect, a disconnect) is kept under a channel filter when that socket was on it, since
	/// "why did this player go" is usually answered by how their socket ended.
	pub fn since(&self, since: i64, channel: Option<&str>) -> Vec<Event> {
		let ring = self.ring.lock().unwrap_or_else(|e| e.into_inner());
		let Some(channel) = channel else {
			return ring.iter().filter(|e| e.at >= since).cloned().collect();
		};
		let sockets: std::collections::HashSet<u64> = ring
			.iter()
			.filter(|e| e.channel.as_deref() == Some(channel))
			.filter_map(|e| e.socket)
			.collect();
		ring.iter()
			.filter(|e| e.at >= since)
			.filter(|e| match &e.channel {
				Some(c) => c == channel,
				None => e.socket.is_some_and(|s| sockets.contains(&s)),
			})
			.cloned()
			.collect()
	}
}

/// Messages on one channel over the last minute, in one-second buckets.
#[derive(Debug, Default)]
pub struct Window {
	/// (second, broadcasts received, deliveries, presence diffs delivered), indexed by second.
	buckets: Vec<(i64, u64, u64, u64)>,
}

impl Window {
	fn bucket(&mut self, second: i64) -> &mut (i64, u64, u64, u64) {
		if self.buckets.is_empty() {
			self.buckets = vec![(i64::MIN, 0, 0, 0); WINDOW_SECONDS];
		}
		let slot = second.rem_euclid(WINDOW_SECONDS as i64) as usize;
		let b = &mut self.buckets[slot];
		if b.0 != second {
			*b = (second, 0, 0, 0);
		}
		b
	}

	/// One broadcast in, delivered to `delivered` subscribers.
	pub fn broadcast(&mut self, delivered: usize) {
		let b = self.bucket(now_ms() / 1000);
		b.1 += 1;
		b.2 += delivered as u64;
	}

	/// One presence diff, delivered to `delivered` subscribers.
	pub fn presence(&mut self, delivered: usize) {
		let b = self.bucket(now_ms() / 1000);
		b.3 += delivered as u64;
	}

	/// Totals over the window that ends now, and the busiest second's broadcasts in.
	pub fn summary(&self) -> Value {
		self.summary_at(now_ms() / 1000)
	}

	fn summary_at(&self, now: i64) -> Value {
		let from = now - WINDOW_SECONDS as i64 + 1;
		let (mut received, mut delivered, mut presence, mut peak) = (0, 0, 0, 0);
		for b in self.buckets.iter().filter(|b| b.0 >= from && b.0 <= now) {
			received += b.1;
			delivered += b.2;
			presence += b.3;
			peak = peak.max(b.1);
		}
		json!({
			"window_seconds": WINDOW_SECONDS,
			"broadcasts_received": received,
			"broadcasts_delivered": delivered,
			"presence_diffs_delivered": presence,
			"peak_broadcasts_per_second": peak,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn the_log_is_bounded_and_drops_the_oldest() {
		let log = EventLog::default();
		for i in 0..(LOG_CAPACITY + 5) {
			log.push(Event::new(Kind::Connect).socket(i as u64));
		}
		let all = log.since(0, None);
		assert_eq!(all.len(), LOG_CAPACITY);
		assert_eq!(all[0].socket, Some(5));
	}

	#[test]
	fn a_channel_filter_keeps_the_disconnect_of_a_socket_that_was_on_it() {
		let log = EventLog::default();
		log.push(Event::new(Kind::Connect).socket(1));
		log.push(
			Event::new(Kind::Join)
				.socket(1)
				.channel("room")
				.presence_key("ada"),
		);
		log.push(Event::new(Kind::Connect).socket(2));
		log.push(Event::new(Kind::Join).socket(2).channel("other"));
		log.push(
			Event::new(Kind::Disconnect)
				.socket(1)
				.reason("connection lost"),
		);
		log.push(
			Event::new(Kind::Disconnect)
				.socket(2)
				.reason("client closed"),
		);
		let room = log.since(0, Some("room"));
		let kinds: Vec<Kind> = room.iter().map(|e| e.kind).collect();
		assert_eq!(kinds, vec![Kind::Connect, Kind::Join, Kind::Disconnect]);
		assert_eq!(room[2].reason.as_deref(), Some("connection lost"));
	}

	#[test]
	fn since_drops_older_events() {
		let log = EventLog::default();
		let mut old = Event::new(Kind::Connect);
		old.at = 10;
		log.push(old);
		log.push(Event::new(Kind::Connect));
		assert_eq!(log.since(11, None).len(), 1);
	}

	#[test]
	fn one_broadcast_to_five_is_one_received_and_five_delivered() {
		let mut w = Window::default();
		w.bucket(100).1 += 1;
		w.bucket(100).2 += 5;
		w.bucket(130).1 += 3;
		w.bucket(130).2 += 15;
		// A bucket from more than a minute before is not counted.
		w.bucket(30).1 += 99;
		let s = w.summary_at(130);
		assert_eq!(s["broadcasts_received"], 4);
		assert_eq!(s["broadcasts_delivered"], 20);
		assert_eq!(s["peak_broadcasts_per_second"], 3);
	}

	#[test]
	fn a_reused_slot_starts_again() {
		let mut w = Window::default();
		w.bucket(5).1 += 7;
		// 65 lands in the same slot as 5 and replaces it.
		w.bucket(65).1 += 1;
		assert_eq!(w.summary_at(65)["broadcasts_received"], 1);
	}
}
