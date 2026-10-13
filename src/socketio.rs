//! Socket.IO 5 packets, carried in Engine.IO message packets (`4` then the packet). Pure.
//!
//! As they look on the wire, with that `4` in front:
//! `<type>[<attachments>-][<namespace>,][<ack id>][<JSON>]`: `40{"token":"..."}` connects to `/`
//! with auth, `42["send",{"a":1}]` is an event, `4212["send",{}]` one that wants ack 12, and
//! `4312[{"ok":true}]` that ack. A namespace other than `/` comes first: `42/chat,["send"]`.

use serde_json::{Value, json};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
	Connect,
	Disconnect,
	Event,
	Ack,
	ConnectError,
	BinaryEvent,
	BinaryAck,
}

impl Kind {
	fn digit(self) -> char {
		match self {
			Kind::Connect => '0',
			Kind::Disconnect => '1',
			Kind::Event => '2',
			Kind::Ack => '3',
			Kind::ConnectError => '4',
			Kind::BinaryEvent => '5',
			Kind::BinaryAck => '6',
		}
	}

	fn from_digit(c: char) -> Option<Kind> {
		Some(match c {
			'0' => Kind::Connect,
			'1' => Kind::Disconnect,
			'2' => Kind::Event,
			'3' => Kind::Ack,
			'4' => Kind::ConnectError,
			'5' => Kind::BinaryEvent,
			'6' => Kind::BinaryAck,
			_ => return None,
		})
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct Packet {
	pub kind: Kind,
	pub nsp: String,
	pub id: Option<u64>,
	pub data: Option<Value>,
	/// Binary attachments that follow as their own Engine.IO packets.
	pub attachments: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct DecodeError(pub &'static str);

/// The most attachments one packet may announce.
const MAX_ATTACHMENTS: usize = 16;

impl Packet {
	pub fn new(kind: Kind, nsp: &str, data: Option<Value>) -> Packet {
		Packet {
			kind,
			nsp: nsp.to_string(),
			id: None,
			data,
			attachments: 0,
		}
	}

	pub fn encode(&self) -> String {
		let mut out = String::new();
		out.push(self.kind.digit());
		if matches!(self.kind, Kind::BinaryEvent | Kind::BinaryAck) {
			out.push_str(&self.attachments.to_string());
			out.push('-');
		}
		if self.nsp != "/" && !self.nsp.is_empty() {
			out.push_str(&self.nsp);
			out.push(',');
		}
		if let Some(id) = self.id {
			out.push_str(&id.to_string());
		}
		if let Some(data) = &self.data {
			out.push_str(&data.to_string());
		}
		out
	}

	/// For an event: its name and arguments.
	pub fn event(&self) -> Option<(&str, &[Value])> {
		let Some(Value::Array(list)) = &self.data else {
			return None;
		};
		let (name, args) = list.split_first()?;
		Some((name.as_str()?, args))
	}
}

pub fn decode(text: &str) -> Result<Packet, DecodeError> {
	let mut rest = text;
	let first = rest.chars().next().ok_or(DecodeError("an empty packet"))?;
	let kind = Kind::from_digit(first).ok_or(DecodeError("an unknown packet type"))?;
	rest = &rest[1..];
	let mut attachments = 0;
	if matches!(kind, Kind::BinaryEvent | Kind::BinaryAck) {
		let dash = rest
			.find('-')
			.ok_or(DecodeError("a binary packet without its attachment count"))?;
		attachments = rest[..dash]
			.parse::<usize>()
			.map_err(|_| DecodeError("an attachment count that is not a number"))?;
		if attachments > MAX_ATTACHMENTS {
			return Err(DecodeError("too many attachments"));
		}
		rest = &rest[dash + 1..];
	}
	let mut nsp = "/".to_string();
	if rest.starts_with('/') {
		match rest.find(',') {
			Some(comma) => {
				nsp = rest[..comma].to_string();
				rest = &rest[comma + 1..];
			}
			None => {
				nsp = rest.to_string();
				rest = "";
			}
		}
	}
	let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
	let id = if digits > 0 {
		if digits > 15 {
			return Err(DecodeError("an ack id that is too long"));
		}
		let id = rest[..digits]
			.parse::<u64>()
			.map_err(|_| DecodeError("an ack id that is not a number"))?;
		rest = &rest[digits..];
		Some(id)
	} else {
		None
	};
	let data = if rest.is_empty() {
		None
	} else {
		Some(
			serde_json::from_str::<Value>(rest)
				.map_err(|_| DecodeError("data that is not JSON"))?,
		)
	};
	let packet = Packet {
		kind,
		nsp,
		id,
		data,
		attachments,
	};
	match packet.kind {
		Kind::Event | Kind::BinaryEvent if packet.event().is_none() => {
			Err(DecodeError("an event without a name"))
		}
		Kind::Connect if !matches!(packet.data, None | Some(Value::Object(_))) => {
			Err(DecodeError("a connect whose auth is not an object"))
		}
		Kind::Ack | Kind::BinaryAck if !matches!(packet.data, Some(Value::Array(_))) => {
			Err(DecodeError("an ack without its arguments"))
		}
		_ => Ok(packet),
	}
}

/// An event to send with one binary argument: the header packet, then the bytes as an
/// attachment.
pub fn binary_event(nsp: &str, event: &str, bytes: Vec<u8>) -> (Packet, Vec<u8>) {
	let mut p = Packet::new(
		Kind::BinaryEvent,
		nsp,
		Some(json!([event, { "_placeholder": true, "num": 0 }])),
	);
	p.attachments = 1;
	(p, bytes)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn the_packets_a_client_sends_decode() {
		let p = decode("0").unwrap();
		assert_eq!((p.kind, p.nsp.as_str(), p.data), (Kind::Connect, "/", None));

		let p = decode("0{\"token\":\"t\"}").unwrap();
		assert_eq!(p.data, Some(json!({ "token": "t" })));

		let p = decode("0/chat,{\"token\":\"t\"}").unwrap();
		assert_eq!(p.nsp, "/chat");

		let p = decode("0/chat,").unwrap();
		assert_eq!((p.nsp.as_str(), p.data), ("/chat", None));

		let p = decode("212[\"send\",{\"a\":1},2]").unwrap();
		assert_eq!(p.id, Some(12));
		let (name, args) = p.event().unwrap();
		assert_eq!(name, "send");
		assert_eq!(args, &[json!({ "a": 1 }), json!(2)]);

		let p = decode("2/chat,7[\"x\"]").unwrap();
		assert_eq!((p.nsp.as_str(), p.id), ("/chat", Some(7)));

		let p = decode("51-[\"up\",{\"_placeholder\":true,\"num\":0}]").unwrap();
		assert_eq!((p.kind, p.attachments), (Kind::BinaryEvent, 1));

		assert_eq!(decode("1").unwrap().kind, Kind::Disconnect);
		assert_eq!(decode("3/chat,1[\"ok\"]").unwrap().id, Some(1));
	}

	#[test]
	fn what_the_server_sends_encodes_as_the_reference_client_expects() {
		assert_eq!(
			Packet::new(Kind::Connect, "/", Some(json!({ "sid": "x" }))).encode(),
			"0{\"sid\":\"x\"}"
		);
		assert_eq!(
			Packet::new(Kind::Event, "/chat", Some(json!(["m", 1]))).encode(),
			"2/chat,[\"m\",1]"
		);
		let mut ack = Packet::new(Kind::Ack, "/", Some(json!([{ "ok": true }])));
		ack.id = Some(5);
		assert_eq!(ack.encode(), "35[{\"ok\":true}]");
		let (header, bytes) = binary_event("/", "file", vec![1, 2]);
		assert_eq!(
			header.encode(),
			"51-[\"file\",{\"_placeholder\":true,\"num\":0}]"
		);
		assert_eq!(bytes, vec![1, 2]);
		assert_eq!(
			Packet::new(Kind::ConnectError, "/", Some(json!({ "message": "no" }))).encode(),
			"4{\"message\":\"no\"}"
		);
	}

	#[test]
	fn malformed_packets_are_errors() {
		for bad in [
			"",
			"9",
			"2",
			"2{}",
			"2[1]",
			"0[]",
			"3",
			"5-[\"x\"]",
			"5x-[\"x\"]",
			"599-[\"x\"]",
			"2[\"x\"",
			"21234567890123456[\"x\"]",
		] {
			assert!(decode(bad).is_err(), "{bad:?} should not decode");
		}
	}
}
