//! Presence: who is on a topic, in the shape the JavaScript client's presence state machine
//! reads (Phoenix Presence's).
//!
//! One `Topic` per project topic. A tracked entry is a (subscriber, key) pair holding one meta:
//! the client's own map plus a `phx_ref`, a random tag that names this version of the entry.
//! Several subscribers may track the same key (two tabs of one user), so a key maps to a LIST
//! of metas, and each one leaves on its own.
//!
//!  - `presence_state` is the whole topic, grouped: `{ key: { metas: [meta, ...] } }`.
//!  - `presence_diff` is what changed: `{ joins: {...}, leaves: {...} }`, grouped the same way.
//!  - Tracking again under a key already tracked (an update) is a LEAVE of the old meta and a
//!    JOIN of the new one, and the new one carries `phx_ref_prev`, the ref it replaced. The
//!    client builds its state from exactly that pair, which is why it is kept.

use std::collections::BTreeMap;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;

use serde_json::{Map, Value, json};

/// A subscriber, as the hub knows it: a socket and one channel on it.
pub type SubscriberId = u64;

#[derive(Debug, Clone)]
struct Entry {
	subscriber: SubscriberId,
	key: String,
	meta: Map<String, Value>,
}

/// One topic's presence.
#[derive(Debug, Default)]
pub struct Topic {
	/// In the order they were tracked, which is the order metas are listed in.
	entries: Vec<Entry>,
}

/// A change to broadcast.
#[derive(Debug, Clone, PartialEq)]
pub struct Diff {
	pub joins: Vec<(String, Map<String, Value>)>,
	pub leaves: Vec<(String, Map<String, Value>)>,
}

impl Diff {
	pub fn is_empty(&self) -> bool {
		self.joins.is_empty() && self.leaves.is_empty()
	}

	/// The `presence_diff` payload.
	pub fn payload(&self) -> Value {
		json!({ "joins": group(&self.joins), "leaves": group(&self.leaves) })
	}
}

fn group(items: &[(String, Map<String, Value>)]) -> Value {
	let mut by_key: BTreeMap<&str, Vec<Value>> = BTreeMap::new();
	for (key, meta) in items {
		by_key
			.entry(key)
			.or_default()
			.push(Value::Object(meta.clone()));
	}
	let mut out = Map::new();
	for (key, metas) in by_key {
		out.insert(key.to_string(), json!({ "metas": metas }));
	}
	Value::Object(out)
}

/// A fresh `phx_ref`: eight random bytes, base64, the tracker's own shape.
pub fn new_ref() -> String {
	let mut bytes = [0u8; 8];
	getrandom::fill(&mut bytes).expect("the operating system has randomness");
	STANDARD.encode(bytes)
}

impl Topic {
	pub fn is_empty(&self) -> bool {
		self.entries.is_empty()
	}

	/// The `presence_state` payload.
	pub fn state(&self) -> Value {
		let items: Vec<(String, Map<String, Value>)> = self
			.entries
			.iter()
			.map(|e| (e.key.clone(), e.meta.clone()))
			.collect();
		group(&items)
	}

	/// Track `payload` for this subscriber under `key`. An unchanged payload is no change.
	pub fn track(
		&mut self,
		subscriber: SubscriberId,
		key: &str,
		payload: Map<String, Value>,
		phx_ref: String,
	) -> Diff {
		let mut meta = payload;
		meta.insert("phx_ref".into(), Value::String(phx_ref));
		if let Some(existing) = self
			.entries
			.iter_mut()
			.find(|e| e.subscriber == subscriber && e.key == key)
		{
			let old = existing.meta.clone();
			let old_ref = old.get("phx_ref").cloned().unwrap_or(Value::Null);
			meta.insert("phx_ref_prev".into(), old_ref);
			existing.meta = meta.clone();
			return Diff {
				joins: vec![(key.to_string(), meta)],
				leaves: vec![(key.to_string(), old)],
			};
		}
		self.entries.push(Entry {
			subscriber,
			key: key.to_string(),
			meta: meta.clone(),
		});
		Diff {
			joins: vec![(key.to_string(), meta)],
			leaves: Vec::new(),
		}
	}

	/// Stop tracking this subscriber under `key`.
	pub fn untrack(&mut self, subscriber: SubscriberId, key: &str) -> Diff {
		let mut leaves = Vec::new();
		self.entries.retain(|e| {
			if e.subscriber == subscriber && e.key == key {
				leaves.push((e.key.clone(), e.meta.clone()));
				false
			} else {
				true
			}
		});
		Diff {
			joins: Vec::new(),
			leaves,
		}
	}

	/// Everything this subscriber tracked, gone (its channel closed).
	pub fn remove(&mut self, subscriber: SubscriberId) -> Diff {
		let mut leaves = Vec::new();
		self.entries.retain(|e| {
			if e.subscriber == subscriber {
				leaves.push((e.key.clone(), e.meta.clone()));
				false
			} else {
				true
			}
		});
		Diff {
			joins: Vec::new(),
			leaves,
		}
	}

	/// What this subscriber currently tracks under `key`, without its refs (to tell an
	/// unchanged track from a change).
	pub fn tracked(&self, subscriber: SubscriberId, key: &str) -> Option<Map<String, Value>> {
		self.entries
			.iter()
			.find(|e| e.subscriber == subscriber && e.key == key)
			.map(|e| {
				let mut m = e.meta.clone();
				m.remove("phx_ref");
				m.remove("phx_ref_prev");
				m
			})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn map(v: Value) -> Map<String, Value> {
		v.as_object().cloned().unwrap()
	}

	#[test]
	fn track_update_and_leave() {
		let mut t = Topic::default();
		let d = t.track(1, "alice", map(json!({"s": "here"})), "r1".into());
		assert_eq!(
			d.payload(),
			json!({"joins": {"alice": {"metas": [{"s": "here", "phx_ref": "r1"}]}}, "leaves": {}})
		);
		let d = t.track(1, "alice", map(json!({"s": "away"})), "r2".into());
		assert_eq!(
			d.payload()["joins"]["alice"]["metas"][0]["phx_ref_prev"],
			"r1"
		);
		assert_eq!(d.payload()["leaves"]["alice"]["metas"][0]["phx_ref"], "r1");
		t.track(2, "alice", map(json!({"s": "tab two"})), "r3".into());
		assert_eq!(t.state()["alice"]["metas"].as_array().unwrap().len(), 2);
		let d = t.remove(1);
		assert_eq!(d.leaves.len(), 1);
		assert_eq!(t.state()["alice"]["metas"][0]["phx_ref"], "r3");
	}

	#[test]
	fn a_ref_is_twelve_base64_characters() {
		assert_eq!(new_ref().len(), 12);
	}
}
