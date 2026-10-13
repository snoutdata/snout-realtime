//! Engine.IO 4: the transport under Socket.IO. Pure: packets in and out, the polling payload, the
//! handshake. The sessions that use it are `sio.rs`.
//!
//! A packet is a type digit and its data. Over a websocket each is one frame (binary data is a
//! binary frame with no type); over long-polling they are joined by U+001E, and binary data is
//! `b` followed by base64.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde_json::json;

/// The protocol revision this server speaks (`EIO=4`).
pub const PROTOCOL: &str = "4";
/// How often the server pings, and how long it waits for the pong.
pub const PING_INTERVAL_MS: u64 = 25_000;
pub const PING_TIMEOUT_MS: u64 = 20_000;
/// The largest polling body read from a client, in bytes.
pub const MAX_PAYLOAD: usize = 1_000_000;
const SEPARATOR: char = '\u{1e}';

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Packet {
	Open(String),
	Close,
	Ping(String),
	Pong(String),
	Message(String),
	Binary(Vec<u8>),
	Upgrade,
	Noop,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct DecodeError(pub &'static str);

/// The open packet's body for a new session.
pub fn handshake(sid: &str, upgrades: bool) -> String {
	json!({
		"sid": sid,
		"upgrades": if upgrades { vec!["websocket"] } else { vec![] },
		"pingInterval": PING_INTERVAL_MS,
		"pingTimeout": PING_TIMEOUT_MS,
		"maxPayload": MAX_PAYLOAD,
	})
	.to_string()
}

impl Packet {
	/// The packet as text: a websocket text frame, or one item of a polling payload. Binary data
	/// is base64 here, which is only what polling wants; `ws_frame` sends it as bytes.
	pub fn encode(&self) -> String {
		match self {
			Packet::Open(d) => format!("0{d}"),
			Packet::Close => "1".into(),
			Packet::Ping(d) => format!("2{d}"),
			Packet::Pong(d) => format!("3{d}"),
			Packet::Message(d) => format!("4{d}"),
			Packet::Binary(b) => format!("b{}", STANDARD.encode(b)),
			Packet::Upgrade => "5".into(),
			Packet::Noop => "6".into(),
		}
	}

	/// The size the packet costs in a queue, roughly.
	pub fn len(&self) -> usize {
		match self {
			Packet::Open(d) | Packet::Ping(d) | Packet::Pong(d) | Packet::Message(d) => d.len() + 1,
			Packet::Binary(b) => b.len(),
			_ => 1,
		}
	}

	pub fn is_empty(&self) -> bool {
		false
	}
}

/// One text packet, from a websocket text frame or a polling item.
pub fn decode(text: &str) -> Result<Packet, DecodeError> {
	let mut chars = text.chars();
	let kind = chars.next().ok_or(DecodeError("an empty packet"))?;
	let rest = chars.as_str();
	Ok(match kind {
		'0' => Packet::Open(rest.into()),
		'1' => Packet::Close,
		'2' => Packet::Ping(rest.into()),
		'3' => Packet::Pong(rest.into()),
		'4' => Packet::Message(rest.into()),
		'5' => Packet::Upgrade,
		'6' => Packet::Noop,
		'b' => Packet::Binary(
			STANDARD
				.decode(rest)
				.map_err(|_| DecodeError("binary data that is not base64"))?,
		),
		_ => return Err(DecodeError("an unknown packet type")),
	})
}

/// A polling request's body: one or more packets.
pub fn decode_payload(body: &str) -> Result<Vec<Packet>, DecodeError> {
	body.split(SEPARATOR).map(decode).collect()
}

/// A polling response's body.
pub fn encode_payload(packets: &[Packet]) -> String {
	let mut out = String::new();
	for (i, p) in packets.iter().enumerate() {
		if i > 0 {
			out.push(SEPARATOR);
		}
		out.push_str(&p.encode());
	}
	out
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn packets_round_trip() {
		for p in [
			Packet::Open("{}".into()),
			Packet::Close,
			Packet::Ping("probe".into()),
			Packet::Pong(String::new()),
			Packet::Message("40".into()),
			Packet::Binary(vec![0, 1, 255]),
			Packet::Upgrade,
			Packet::Noop,
		] {
			assert_eq!(decode(&p.encode()), Ok(p));
		}
	}

	#[test]
	fn a_polling_payload_splits_on_the_record_separator() {
		let body = "4hello\u{1e}2\u{1e}bAQI=";
		let packets = decode_payload(body).unwrap();
		assert_eq!(
			packets,
			vec![
				Packet::Message("hello".into()),
				Packet::Ping(String::new()),
				Packet::Binary(vec![1, 2]),
			]
		);
		assert_eq!(encode_payload(&packets), body);
	}

	#[test]
	fn garbage_is_an_error_not_a_panic() {
		assert!(decode("").is_err());
		assert!(decode("9").is_err());
		assert!(decode("b!!").is_err());
		assert!(decode_payload("4a\u{1e}").is_err());
	}

	#[test]
	fn the_handshake_offers_the_upgrade_only_on_polling() {
		let v: serde_json::Value = serde_json::from_str(&handshake("abc", true)).unwrap();
		assert_eq!(v["upgrades"], json!(["websocket"]));
		assert_eq!(v["pingInterval"], json!(25000));
		let v: serde_json::Value = serde_json::from_str(&handshake("abc", false)).unwrap();
		assert_eq!(v["upgrades"], json!([]));
	}
}
