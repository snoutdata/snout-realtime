//! The wire: Phoenix channel messages as the JavaScript client speaks them, in both serializers.
//!
//! A socket chooses its serializer with the `vsn` query parameter:
//!
//!  - **1.0.0**: every message is a JSON object `{topic, event, payload, ref}` in a text frame.
//!    Replies carry no `join_ref`.
//!  - **2.0.0** (the client's default since 2.x): a JSON array
//!    `[join_ref, ref, topic, event, payload]` in a text frame, plus three BINARY frame kinds for
//!    broadcasts whose payload is carried as bytes rather than re-encoded JSON:
//!
//!    | kind | byte 0 | layout after it |
//!    |---|---|---|
//!    | push (client to server) | 0 | join_ref len, ref len, topic len, event len, then those bytes, then the payload |
//!    | reply (server to client) | 1 | join_ref len, ref len, topic len, status len, then those, then the payload |
//!    | broadcast (server to client) | 2 | topic len, event len, then those, then the payload |
//!    | user broadcast push (client to server) | 3 | join_ref len, ref len, topic len, user event len, metadata len, encoding (0 binary, 1 JSON), then those, then the user payload |
//!    | user broadcast (server to client) | 4 | topic len, user event len, metadata len, encoding, then those, then the user payload |
//!
//! Every length is one byte, so each of those strings is at most 255 bytes.

use serde_json::{Map, Value, json};

/// Which serializer a socket speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Vsn {
	V1,
	V2,
}

impl Vsn {
	/// From the `vsn` query parameter. Anything but 2.x is the first serializer, as Phoenix does.
	pub fn from_param(value: Option<&str>) -> Vsn {
		match value {
			Some(v) if v.starts_with("2.") => Vsn::V2,
			_ => Vsn::V1,
		}
	}
}

/// How a user broadcast's payload was carried.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
	Binary,
	Json,
}

/// A message from a client.
#[derive(Debug, Clone, PartialEq)]
pub struct Inbound {
	pub join_ref: Option<String>,
	pub reference: Option<String>,
	pub topic: String,
	pub event: String,
	pub payload: InboundPayload,
}

#[derive(Debug, Clone, PartialEq)]
pub enum InboundPayload {
	Json(Value),
	/// A V2 push whose payload is raw bytes (kind 0).
	Bytes(Vec<u8>),
	/// A V2 user broadcast push (kind 3): the event is always `broadcast`.
	UserBroadcast {
		user_event: String,
		encoding: Encoding,
		payload: Vec<u8>,
		metadata: Value,
	},
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct DecodeError(pub String);

fn string_or_null(value: Option<&Value>) -> Option<String> {
	match value {
		Some(Value::String(s)) => Some(s.clone()),
		Some(Value::Number(n)) => Some(n.to_string()),
		_ => None,
	}
}

/// A text frame.
pub fn decode_text(vsn: Vsn, text: &str) -> Result<Inbound, DecodeError> {
	let value: Value =
		serde_json::from_str(text).map_err(|e| DecodeError(format!("not JSON: {e}")))?;
	match (vsn, value) {
		(Vsn::V2, Value::Array(items)) if items.len() >= 5 => Ok(Inbound {
			join_ref: string_or_null(items.first()),
			reference: string_or_null(items.get(1)),
			topic: items[2].as_str().unwrap_or_default().to_string(),
			event: items[3].as_str().unwrap_or_default().to_string(),
			payload: InboundPayload::Json(items[4].clone()),
		}),
		(Vsn::V2, other) => Err(DecodeError(format!("expected V2 array, got: {other}"))),
		(Vsn::V1, Value::Object(map)) => Ok(Inbound {
			join_ref: string_or_null(map.get("join_ref")),
			reference: string_or_null(map.get("ref")),
			topic: map
				.get("topic")
				.and_then(Value::as_str)
				.unwrap_or_default()
				.to_string(),
			event: map
				.get("event")
				.and_then(Value::as_str)
				.unwrap_or_default()
				.to_string(),
			payload: InboundPayload::Json(map.get("payload").cloned().unwrap_or(Value::Null)),
		}),
		(Vsn::V1, other) => Err(DecodeError(format!(
			"expected a message object, got: {other}"
		))),
	}
}

struct Reader<'a> {
	bytes: &'a [u8],
	at: usize,
}

impl<'a> Reader<'a> {
	fn byte(&mut self) -> Result<usize, DecodeError> {
		let b = *self
			.bytes
			.get(self.at)
			.ok_or_else(|| DecodeError("truncated frame".into()))?;
		self.at += 1;
		Ok(b as usize)
	}

	fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
		let end = self
			.at
			.checked_add(n)
			.filter(|end| *end <= self.bytes.len())
			.ok_or_else(|| DecodeError("truncated frame".into()))?;
		let out = &self.bytes[self.at..end];
		self.at = end;
		Ok(out)
	}

	fn text(&mut self, n: usize) -> Result<String, DecodeError> {
		String::from_utf8(self.take(n)?.to_vec())
			.map_err(|_| DecodeError("a header field is not UTF-8".into()))
	}

	fn rest(&self) -> Vec<u8> {
		self.bytes[self.at..].to_vec()
	}
}

/// A binary frame (V2 only).
pub fn decode_binary(bytes: &[u8]) -> Result<Inbound, DecodeError> {
	let mut r = Reader { bytes, at: 0 };
	match r.byte()? {
		0 => {
			let (jr, rf, tp, ev) = (r.byte()?, r.byte()?, r.byte()?, r.byte()?);
			let join_ref = r.text(jr)?;
			let reference = r.text(rf)?;
			let topic = r.text(tp)?;
			let event = r.text(ev)?;
			Ok(Inbound {
				join_ref: Some(join_ref),
				reference: Some(reference),
				topic,
				event,
				payload: InboundPayload::Bytes(r.rest()),
			})
		}
		3 => {
			let (jr, rf, tp, ue, md, enc) = (
				r.byte()?,
				r.byte()?,
				r.byte()?,
				r.byte()?,
				r.byte()?,
				r.byte()?,
			);
			let join_ref = r.text(jr)?;
			let reference = r.text(rf)?;
			let topic = r.text(tp)?;
			let user_event = r.text(ue)?;
			let metadata = if md > 0 {
				serde_json::from_slice(r.take(md)?)
					.map_err(|e| DecodeError(format!("metadata is not JSON: {e}")))?
			} else {
				Value::Object(Map::new())
			};
			Ok(Inbound {
				join_ref: Some(join_ref),
				reference: Some(reference),
				topic,
				event: "broadcast".into(),
				payload: InboundPayload::UserBroadcast {
					user_event,
					encoding: if enc == 0 {
						Encoding::Binary
					} else {
						Encoding::Json
					},
					payload: r.rest(),
					metadata,
				},
			})
		}
		kind => Err(DecodeError(format!("unknown binary message kind {kind}"))),
	}
}

/// A frame to send.
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
	Text(String),
	Binary(Vec<u8>),
}

fn nullable(value: &Option<String>) -> Value {
	value
		.as_ref()
		.map_or(Value::Null, |s| Value::String(s.clone()))
}

/// A reply to a client's message (`phx_reply`).
pub fn reply(
	vsn: Vsn,
	join_ref: &Option<String>,
	reference: &Option<String>,
	topic: &str,
	status: &str,
	response: Value,
) -> Frame {
	let payload = json!({ "status": status, "response": response });
	message(vsn, join_ref, reference, topic, "phx_reply", payload)
}

/// A server push on a channel (`presence_state`, `system`, `phx_close`, ...).
pub fn message(
	vsn: Vsn,
	join_ref: &Option<String>,
	reference: &Option<String>,
	topic: &str,
	event: &str,
	payload: Value,
) -> Frame {
	match vsn {
		Vsn::V2 => Frame::Text(
			Value::Array(vec![
				nullable(join_ref),
				nullable(reference),
				Value::String(topic.into()),
				Value::String(event.into()),
				payload,
			])
			.to_string(),
		),
		Vsn::V1 => {
			// V1 carries no join_ref.
			let mut map = Map::new();
			map.insert("event".into(), Value::String(event.into()));
			map.insert("payload".into(), payload);
			map.insert("ref".into(), nullable(reference));
			map.insert("topic".into(), Value::String(topic.into()));
			Frame::Text(Value::Object(map).to_string())
		}
	}
}

/// A broadcast fanned out to every subscriber: no refs.
pub fn broadcast(vsn: Vsn, topic: &str, event: &str, payload: Value) -> Frame {
	message(vsn, &None, &None, topic, event, payload)
}

/// `broadcast` with a payload that is already JSON text, so a change sent to a thousand sockets
/// is serialised once, not parsed and serialised again for each. `payload` must be valid JSON.
pub fn broadcast_raw(vsn: Vsn, topic: &str, event: &str, payload: &str) -> Frame {
	let topic = Value::from(topic);
	let event = Value::from(event);
	Frame::Text(match vsn {
		Vsn::V2 => format!("[null,null,{topic},{event},{payload}]"),
		Vsn::V1 => {
			format!("{{\"event\":{event},\"payload\":{payload},\"ref\":null,\"topic\":{topic}}}")
		}
	})
}

/// A user broadcast in V2's binary form (kind 4): the payload is sent as the bytes it arrived as.
pub fn user_broadcast(
	topic: &str,
	user_event: &str,
	metadata: Option<&Value>,
	encoding: Encoding,
	payload: &[u8],
) -> Result<Frame, DecodeError> {
	let metadata = metadata.map(Value::to_string).unwrap_or_default();
	for (what, s) in [
		("topic", topic),
		("user_event", user_event),
		("metadata", metadata.as_str()),
	] {
		if s.len() > 255 {
			return Err(DecodeError(format!(
				"{what} must be at most 255 bytes, but is {}",
				s.len()
			)));
		}
	}
	let mut out =
		Vec::with_capacity(5 + topic.len() + user_event.len() + metadata.len() + payload.len());
	out.push(4);
	out.push(topic.len() as u8);
	out.push(user_event.len() as u8);
	out.push(metadata.len() as u8);
	out.push(if encoding == Encoding::Json { 1 } else { 0 });
	out.extend_from_slice(topic.as_bytes());
	out.extend_from_slice(user_event.as_bytes());
	out.extend_from_slice(metadata.as_bytes());
	out.extend_from_slice(payload);
	Ok(Frame::Binary(out))
}

/// A user broadcast for a V1 socket, which cannot carry bytes: JSON only, re-encoded as the
/// regular `broadcast` event, with its metadata as `meta`. A binary payload cannot be
/// delivered to V1 at all.
pub fn user_broadcast_as_json(
	user_event: &str,
	encoding: Encoding,
	payload: &[u8],
	metadata: Option<&Value>,
) -> Option<Value> {
	if encoding != Encoding::Json {
		return None;
	}
	let inner: Value = serde_json::from_slice(payload).ok()?;
	let mut out = json!({ "type": "broadcast", "event": user_event, "payload": inner });
	if let Some(m) = metadata {
		out["meta"] = m.clone();
	}
	Some(out)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn v2_text_round_trip() {
		let m = decode_text(
			Vsn::V2,
			r#"["1","2","realtime:room","phx_join",{"config":{}}]"#,
		)
		.unwrap();
		assert_eq!(m.join_ref.as_deref(), Some("1"));
		assert_eq!(m.reference.as_deref(), Some("2"));
		assert_eq!(m.topic, "realtime:room");
		assert_eq!(m.event, "phx_join");
		let Frame::Text(t) = reply(
			Vsn::V2,
			&m.join_ref,
			&m.reference,
			&m.topic,
			"ok",
			json!({}),
		) else {
			panic!()
		};
		assert_eq!(
			t,
			r#"["1","2","realtime:room","phx_reply",{"status":"ok","response":{}}]"#
		);
	}

	#[test]
	fn a_raw_payload_frames_as_the_parsed_one_does() {
		let payload = r#"{"ids":[7],"data":{"table":"t","record":{"a":"x \"y\""}}}"#;
		for vsn in [Vsn::V1, Vsn::V2] {
			let (Frame::Text(raw), Frame::Text(parsed)) = (
				broadcast_raw(vsn, "realtime:r", "postgres_changes", payload),
				broadcast(
					vsn,
					"realtime:r",
					"postgres_changes",
					serde_json::from_str(payload).unwrap(),
				),
			) else {
				panic!()
			};
			assert_eq!(
				serde_json::from_str::<Value>(&raw).unwrap(),
				serde_json::from_str::<Value>(&parsed).unwrap()
			);
		}
	}

	#[test]
	fn v1_object_and_reply_without_join_ref() {
		let m = decode_text(
			Vsn::V1,
			r#"{"topic":"phoenix","event":"heartbeat","payload":{},"ref":"7"}"#,
		)
		.unwrap();
		assert_eq!(m.reference.as_deref(), Some("7"));
		let Frame::Text(t) = reply(
			Vsn::V1,
			&m.join_ref,
			&m.reference,
			&m.topic,
			"ok",
			json!({}),
		) else {
			panic!()
		};
		let v: Value = serde_json::from_str(&t).unwrap();
		assert_eq!(
			v,
			json!({"topic":"phoenix","event":"phx_reply","ref":"7","payload":{"status":"ok","response":{}}})
		);
	}

	#[test]
	fn v2_user_broadcast_push_decodes() {
		let mut b = vec![3, 1, 1, 4, 5, 0, 1];
		b.extend_from_slice(b"12room");
		b.extend_from_slice(b"hello");
		b.extend_from_slice(br#"{"a":1}"#);
		let m = decode_binary(&b).unwrap();
		assert_eq!(m.event, "broadcast");
		assert_eq!(m.topic, "room");
		let InboundPayload::UserBroadcast {
			user_event,
			encoding,
			payload,
			..
		} = m.payload
		else {
			panic!()
		};
		assert_eq!(user_event, "hello");
		assert_eq!(encoding, Encoding::Json);
		assert_eq!(payload, br#"{"a":1}"#);
	}

	#[test]
	fn truncated_binary_is_refused_not_panicking() {
		for n in 0..6 {
			assert!(decode_binary(&[3, 9, 9, 9, 9, 9][..n]).is_err());
		}
		assert!(decode_binary(&[0, 200, 0, 0, 0]).is_err());
	}

	#[test]
	fn user_broadcast_encodes_kind_four() {
		let Frame::Binary(b) =
			user_broadcast("realtime:r", "e", None, Encoding::Json, b"{}").unwrap()
		else {
			panic!()
		};
		assert_eq!(&b[..5], &[4, 10, 1, 0, 1]);
		assert_eq!(&b[5..], b"realtime:re{}");
	}
}
