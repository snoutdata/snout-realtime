//! The wire in both vocabularies (the own layout): a client may join `live:<n>` and
//! speak `changes`, `signal` and `roster`, or join `realtime:<n>` and speak `postgres_changes`,
//! `broadcast` and `presence`, and either way it is the same channel.
//!
//! Translated at the socket's edge and nowhere else: a frame from a client is put into the old
//! vocabulary before the socket reads it, and a frame to a client is put back into the new one
//! only when that client joined the topic in the new one. Everything between (the hub, presence,
//! changes, policies) speaks one vocabulary, the old one, so a channel joined both ways is one
//! channel.

use std::collections::HashSet;

use serde_json::{Map, Value};

use crate::layout::{self, Vocabulary};
use crate::protocol::{Frame, Inbound, InboundPayload};

/// The join `config` keys, old then new.
const CONFIG_KEYS: [(&str, &str); 3] = [
	("postgres_changes", "changes"),
	("broadcast", "signal"),
	("presence", "roster"),
];

/// A message from a client, in the old vocabulary, and the vocabulary its topic was in (`None` for
/// a topic with neither prefix, such as `phoenix`).
pub fn hear(mut m: Inbound) -> (Inbound, Option<Vocabulary>) {
	let Some((name, spoken)) = layout::split_topic(&m.topic) else {
		return (m, None);
	};
	if spoken == Vocabulary::Old {
		return (m, Some(spoken));
	}
	m.topic = layout::topic_in(name, Vocabulary::Old);
	m.event = layout::event_to_old(&m.event).0.to_string();
	if let InboundPayload::Json(payload) = &mut m.payload {
		to_old(&m.event, payload);
	}
	(m, Some(spoken))
}

/// A payload's own names, new to old: a join's `config` keys, and the `type` a broadcast or
/// presence message carries.
fn to_old(event: &str, payload: &mut Value) {
	let Value::Object(map) = payload else {
		return;
	};
	if event == "phx_join"
		&& let Some(Value::Object(config)) = map.get_mut("config")
	{
		rename_keys(config, |key| {
			CONFIG_KEYS
				.iter()
				.find(|(_, new)| *new == key)
				.map(|(old, _)| *old)
		});
	}
	if let Some(Value::String(kind)) = map.get_mut("type") {
		let old = layout::event_to_old(kind).0.to_string();
		*kind = old;
	}
}

fn rename_keys(map: &mut Map<String, Value>, rename: impl Fn(&str) -> Option<&'static str>) {
	let keys: Vec<String> = map.keys().cloned().collect();
	for key in keys {
		if let Some(to) = rename(&key)
			&& !map.contains_key(to)
			&& let Some(value) = map.remove(&key)
		{
			map.insert(to.to_string(), value);
		}
	}
}

/// A frame to a client, in the new vocabulary when its topic is one the client joined in it
/// (`new_topics`, by the topic's old form); otherwise as it is.
pub fn speak(frame: Frame, new_topics: &HashSet<String>) -> Frame {
	if new_topics.is_empty() {
		return frame;
	}
	match frame {
		Frame::Text(text) => Frame::Text(speak_text(text, new_topics)),
		Frame::Binary(bytes) => Frame::Binary(speak_binary(bytes, new_topics)),
	}
}

fn speak_text(text: String, new_topics: &HashSet<String>) -> String {
	let Ok(mut value) = serde_json::from_str::<Value>(&text) else {
		return text;
	};
	// V2 is `[join_ref, ref, topic, event, payload]`; V1 an object with the same fields.
	let topic = match &value {
		Value::Array(items) if items.len() >= 5 => items[2].as_str(),
		Value::Object(map) => map.get("topic").and_then(Value::as_str),
		_ => None,
	};
	let Some(topic) = topic.filter(|t| new_topics.contains(*t)) else {
		return text;
	};
	let Some((bare, _)) = layout::split_topic(topic) else {
		return text;
	};
	let topic = Value::String(layout::topic_in(bare, Vocabulary::New));
	let renamed = |event: &mut Value| {
		if let Some(e) = event.as_str() {
			*event = Value::String(layout::event_in(e, Vocabulary::New).to_string());
		}
	};
	match &mut value {
		Value::Array(items) => {
			items[2] = topic;
			renamed(&mut items[3]);
			to_new(&mut items[4]);
		}
		Value::Object(map) => {
			map.insert("topic".into(), topic);
			if let Some(event) = map.get_mut("event") {
				renamed(event);
			}
			if let Some(payload) = map.get_mut("payload") {
				to_new(payload);
			}
		}
		_ => {}
	}
	value.to_string()
}

/// A payload's own names, old to new: the `type` of a broadcast or presence message, the
/// `extension` a `system` message names, and the extension keys of a join's reply.
fn to_new(payload: &mut Value) {
	let Value::Object(map) = payload else {
		return;
	};
	for field in ["type", "extension"] {
		if let Some(Value::String(kind)) = map.get_mut(field) {
			let new = layout::event_in(kind, Vocabulary::New).to_string();
			*kind = new;
		}
	}
	if let Some(Value::Object(response)) = map.get_mut("response") {
		rename_keys(response, |key| {
			CONFIG_KEYS
				.iter()
				.find(|(old, _)| *old == key)
				.map(|(_, new)| *new)
		});
	}
}

/// The binary kinds a server sends (protocol.rs): 1 reply and 4 user broadcast carry a topic, 2
/// broadcast a topic and an event. Rebuilt with the new names; anything unreadable is sent as is.
fn speak_binary(bytes: Vec<u8>, new_topics: &HashSet<String>) -> Vec<u8> {
	speak_binary_checked(&bytes, new_topics).unwrap_or(bytes)
}

fn speak_binary_checked(bytes: &[u8], new_topics: &HashSet<String>) -> Option<Vec<u8>> {
	let kind = *bytes.first()?;
	// Where the length bytes are, how many there are, and which of them are the topic and event.
	let (lengths, topic_at, event_at) = match kind {
		1 => (4, 2, None),
		2 => (2, 0, Some(1)),
		4 => (3, 0, None),
		_ => return None,
	};
	// Kind 4 has an encoding byte after its lengths, before the strings.
	let header = 1 + lengths + usize::from(kind == 4);
	let sizes: Vec<usize> = bytes
		.get(1..1 + lengths)?
		.iter()
		.map(|b| *b as usize)
		.collect();
	let mut fields = Vec::with_capacity(lengths);
	let mut at = header;
	for size in &sizes {
		fields.push(bytes.get(at..at + size)?.to_vec());
		at += size;
	}
	let rest = bytes.get(at..)?;
	let topic = std::str::from_utf8(&fields[topic_at]).ok()?;
	if !new_topics.contains(topic) {
		return None;
	}
	let (bare, _) = layout::split_topic(topic)?;
	fields[topic_at] = layout::topic_in(bare, Vocabulary::New).into_bytes();
	if let Some(e) = event_at {
		let event = std::str::from_utf8(&fields[e]).ok()?;
		fields[e] = layout::event_in(event, Vocabulary::New).as_bytes().to_vec();
	}
	let mut out = Vec::with_capacity(bytes.len());
	out.push(kind);
	for field in &fields {
		out.push(u8::try_from(field.len()).ok()?);
	}
	if kind == 4 {
		out.push(bytes[1 + lengths]);
	}
	for field in &fields {
		out.extend_from_slice(field);
	}
	out.extend_from_slice(rest);
	Some(out)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::protocol::{self, Encoding, Vsn};
	use serde_json::json;

	fn inbound(topic: &str, event: &str, payload: Value) -> Inbound {
		Inbound {
			join_ref: Some("1".into()),
			reference: Some("2".into()),
			topic: topic.into(),
			event: event.into(),
			payload: InboundPayload::Json(payload),
		}
	}

	fn joined(topic: &str) -> HashSet<String> {
		HashSet::from([topic.to_string()])
	}

	#[test]
	fn a_new_join_is_heard_as_the_old_one() {
		let (m, spoken) = hear(inbound(
			"live:room",
			"phx_join",
			json!({ "config": { "changes": [{ "event": "*" }], "signal": { "self": true }, "roster": { "key": "a" }, "private": false } }),
		));
		assert_eq!(spoken, Some(Vocabulary::New));
		assert_eq!(m.topic, "realtime:room");
		assert_eq!(m.event, "phx_join");
		assert_eq!(
			m.payload,
			InboundPayload::Json(json!({ "config": {
				"postgres_changes": [{ "event": "*" }],
				"broadcast": { "self": true },
				"presence": { "key": "a" },
				"private": false
			} }))
		);
	}

	#[test]
	fn every_new_event_is_heard_as_the_old_one() {
		for (old, new) in layout::EVENTS {
			let (m, _) = hear(inbound("live:r", new, json!({ "type": new })));
			assert_eq!(m.event, old);
			assert_eq!(m.payload, InboundPayload::Json(json!({ "type": old })));
		}
	}

	#[test]
	fn an_old_message_and_the_phoenix_topic_are_untouched() {
		let old = inbound("realtime:room", "broadcast", json!({ "type": "broadcast" }));
		assert_eq!(hear(old.clone()), (old, Some(Vocabulary::Old)));
		let beat = inbound("phoenix", "heartbeat", json!({}));
		assert_eq!(hear(beat.clone()), (beat, None));
		// A new-vocabulary EVENT on an old topic is a customer's own name, not ours.
		let custom = inbound("realtime:room", "signal", json!({}));
		assert_eq!(hear(custom.clone()).0, custom);
	}

	#[test]
	fn a_binary_user_broadcast_push_is_heard_on_the_old_topic() {
		let m = Inbound {
			join_ref: Some("1".into()),
			reference: Some("2".into()),
			topic: "live:room".into(),
			event: "broadcast".into(),
			payload: InboundPayload::UserBroadcast {
				user_event: "move".into(),
				encoding: Encoding::Json,
				payload: b"{}".to_vec(),
				metadata: json!({}),
			},
		};
		let (heard, _) = hear(m);
		assert_eq!(heard.topic, "realtime:room");
		assert_eq!(heard.event, "broadcast");
	}

	fn text(f: Frame) -> Value {
		match f {
			Frame::Text(t) => serde_json::from_str(&t).unwrap_or(Value::Null),
			Frame::Binary(_) => Value::Null,
		}
	}

	#[test]
	fn every_event_is_spoken_back_in_the_new_vocabulary_on_both_serializers() {
		for (old, new) in layout::EVENTS {
			let v2 = protocol::broadcast(Vsn::V2, "realtime:r", old, json!({ "type": old }));
			assert_eq!(
				text(speak(v2, &joined("realtime:r"))),
				json!([null, null, "live:r", new, { "type": new }])
			);
			let v1 = protocol::broadcast(Vsn::V1, "realtime:r", old, json!({ "type": old }));
			assert_eq!(
				text(speak(v1, &joined("realtime:r"))),
				json!({ "event": new, "payload": { "type": new }, "ref": null, "topic": "live:r" })
			);
		}
	}

	#[test]
	fn a_raw_change_a_join_reply_and_a_system_message_are_spoken_new() {
		let raw =
			protocol::broadcast_raw(Vsn::V2, "realtime:r", "postgres_changes", r#"{"ids":[1]}"#);
		assert_eq!(
			text(speak(raw, &joined("realtime:r"))),
			json!([null, null, "live:r", "changes", { "ids": [1] }])
		);
		let reply = protocol::reply(
			Vsn::V2,
			&Some("1".into()),
			&Some("1".into()),
			"realtime:r",
			"ok",
			json!({ "postgres_changes": [] }),
		);
		assert_eq!(
			text(speak(reply, &joined("realtime:r"))),
			json!(["1", "1", "live:r", "phx_reply", { "status": "ok", "response": { "changes": [] } }])
		);
		let system = protocol::message(
			Vsn::V2,
			&Some("1".into()),
			&None,
			"realtime:r",
			"system",
			json!({ "extension": "postgres_changes", "status": "ok" }),
		);
		assert_eq!(
			text(speak(system, &joined("realtime:r"))),
			json!(["1", null, "live:r", "system", { "extension": "changes", "status": "ok" }])
		);
	}

	#[test]
	fn a_client_that_joined_the_old_way_is_answered_the_old_way() {
		let f = protocol::broadcast(
			Vsn::V2,
			"realtime:r",
			"broadcast",
			json!({ "type": "broadcast" }),
		);
		assert_eq!(speak(f.clone(), &HashSet::new()), f);
		assert_eq!(speak(f.clone(), &joined("realtime:other")), f);
	}

	#[test]
	fn a_binary_user_broadcast_is_spoken_on_the_new_topic() {
		let f = protocol::user_broadcast(
			"realtime:r",
			"move",
			Some(&json!({"id":1})),
			Encoding::Json,
			b"{}",
		)
		.unwrap_or(Frame::Binary(vec![]));
		let want = protocol::user_broadcast(
			"live:r",
			"move",
			Some(&json!({"id":1})),
			Encoding::Json,
			b"{}",
		)
		.unwrap_or(Frame::Binary(vec![1]));
		assert_eq!(speak(f, &joined("realtime:r")), want);
	}

	#[test]
	fn binary_replies_and_broadcasts_are_rebuilt() {
		// Kind 2: topic len, event len, topic, event, payload.
		let mut b = vec![2, 10, 9];
		b.extend_from_slice(b"realtime:rbroadcast{}");
		let mut want = vec![2, 6, 6];
		want.extend_from_slice(b"live:rsignal{}");
		assert_eq!(
			speak(Frame::Binary(b), &joined("realtime:r")),
			Frame::Binary(want)
		);
		// Kind 1: join_ref, ref, topic, status, payload.
		let mut b = vec![1, 1, 1, 10, 2];
		b.extend_from_slice(b"12realtime:rok{}");
		let mut want = vec![1, 1, 1, 6, 2];
		want.extend_from_slice(b"12live:rok{}");
		assert_eq!(
			speak(Frame::Binary(b), &joined("realtime:r")),
			Frame::Binary(want)
		);
	}
}
