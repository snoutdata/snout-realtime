//! pgoutput, protocol version 1, text format: the logical decoding output Postgres ships with,
//! decoded as far as a row stream needs (begin, commit, relation, insert, update, delete).
//!
//! Tuple values arrive as TEXT (the stream is started without `binary 'true'`), so a value is
//! exactly what `select col::text` would print, and turning it into JSON is the caller's
//! business with the column's type in hand.

use bytes::{Buf, Bytes};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
	pub name: String,
	pub type_oid: u32,
	pub type_modifier: i32,
	/// Part of the replica identity (the key a DELETE or UPDATE's old row carries).
	pub key: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relation {
	pub id: u32,
	pub schema: String,
	pub name: String,
	pub columns: Vec<Column>,
}

/// One column's value in a tuple.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
	Null,
	/// A TOASTed value that did not change and was not sent.
	Unchanged,
	Text(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
	Begin {
		final_lsn: u64,
		commit_time: i64,
		xid: u32,
	},
	Commit {
		lsn: u64,
		end_lsn: u64,
		commit_time: i64,
	},
	Relation(Relation),
	Insert {
		relation: u32,
		new: Vec<Value>,
	},
	/// `old` is the key or the whole old row, when the replica identity sends one.
	Update {
		relation: u32,
		old: Option<Old>,
		new: Vec<Value>,
	},
	Delete {
		relation: u32,
		old: Old,
	},
	/// Truncate, type, origin, message: nothing a row stream acts on.
	Other(u8),
}

/// The old row a change carries: only its key (`K`: the other columns are sent as null), or the
/// whole row (`O`, replica identity full).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Old {
	pub row: Vec<Value>,
	pub key_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("pgoutput: {0}")]
pub struct DecodeError(pub String);

fn short(what: &str) -> DecodeError {
	DecodeError(format!("truncated {what}"))
}

fn need(b: &Bytes, n: usize, what: &str) -> Result<(), DecodeError> {
	if b.remaining() < n {
		Err(short(what))
	} else {
		Ok(())
	}
}

fn cstr(b: &mut Bytes) -> Result<String, DecodeError> {
	let end = b
		.iter()
		.position(|c| *c == 0)
		.ok_or_else(|| short("string"))?;
	let s = String::from_utf8(b.split_to(end).to_vec())
		.map_err(|_| DecodeError("a name is not UTF-8".into()))?;
	b.advance(1);
	Ok(s)
}

fn tuple(b: &mut Bytes) -> Result<Vec<Value>, DecodeError> {
	need(b, 2, "tuple")?;
	let n = b.get_i16().max(0) as usize;
	let mut out = Vec::with_capacity(n);
	for _ in 0..n {
		need(b, 1, "tuple value")?;
		match b.get_u8() {
			b'n' => out.push(Value::Null),
			b'u' => out.push(Value::Unchanged),
			b't' | b'b' => {
				need(b, 4, "value length")?;
				let len = b.get_i32().max(0) as usize;
				need(b, len, "value")?;
				out.push(Value::Text(
					String::from_utf8_lossy(&b.split_to(len)).into_owned(),
				));
			}
			other => return Err(DecodeError(format!("unknown tuple value kind {other}"))),
		}
	}
	Ok(out)
}

pub fn decode(mut b: Bytes) -> Result<Message, DecodeError> {
	need(&b, 1, "message")?;
	let kind = b.get_u8();
	match kind {
		b'B' => {
			need(&b, 20, "begin")?;
			Ok(Message::Begin {
				final_lsn: b.get_u64(),
				commit_time: b.get_i64(),
				xid: b.get_u32(),
			})
		}
		b'C' => {
			need(&b, 25, "commit")?;
			let _flags = b.get_u8();
			Ok(Message::Commit {
				lsn: b.get_u64(),
				end_lsn: b.get_u64(),
				commit_time: b.get_i64(),
			})
		}
		b'R' => {
			need(&b, 4, "relation")?;
			let id = b.get_u32();
			let schema = cstr(&mut b)?;
			let name = cstr(&mut b)?;
			need(&b, 3, "relation")?;
			let _identity = b.get_u8();
			let n = b.get_i16().max(0) as usize;
			let mut columns = Vec::with_capacity(n);
			for _ in 0..n {
				need(&b, 1, "column")?;
				let flags = b.get_u8();
				let name = cstr(&mut b)?;
				need(&b, 8, "column")?;
				columns.push(Column {
					name,
					type_oid: b.get_u32(),
					type_modifier: b.get_i32(),
					key: flags & 1 == 1,
				});
			}
			Ok(Message::Relation(Relation {
				id,
				schema,
				name,
				columns,
			}))
		}
		b'I' => {
			need(&b, 5, "insert")?;
			let relation = b.get_u32();
			let _n = b.get_u8();
			Ok(Message::Insert {
				relation,
				new: tuple(&mut b)?,
			})
		}
		b'U' => {
			need(&b, 5, "update")?;
			let relation = b.get_u32();
			let mut marker = b.get_u8();
			let mut old = None;
			if marker == b'K' || marker == b'O' {
				old = Some(Old {
					row: tuple(&mut b)?,
					key_only: marker == b'K',
				});
				need(&b, 1, "update")?;
				marker = b.get_u8();
			}
			if marker != b'N' {
				return Err(DecodeError(format!(
					"an update without a new tuple ({marker})"
				)));
			}
			Ok(Message::Update {
				relation,
				old,
				new: tuple(&mut b)?,
			})
		}
		b'D' => {
			need(&b, 5, "delete")?;
			let relation = b.get_u32();
			let marker = b.get_u8();
			Ok(Message::Delete {
				relation,
				old: Old {
					row: tuple(&mut b)?,
					key_only: marker == b'K',
				},
			})
		}
		other => Ok(Message::Other(other)),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use bytes::BufMut;

	#[test]
	fn a_relation_then_an_insert() {
		let mut r = bytes::BytesMut::new();
		r.put_u8(b'R');
		r.put_u32(42);
		r.put_slice(b"realtime\0messages\0");
		r.put_u8(b'd');
		r.put_i16(2);
		r.put_u8(1);
		r.put_slice(b"id\0");
		r.put_u32(2950);
		r.put_i32(-1);
		r.put_u8(0);
		r.put_slice(b"topic\0");
		r.put_u32(25);
		r.put_i32(-1);
		let Message::Relation(rel) = decode(r.freeze()).unwrap() else {
			panic!()
		};
		assert_eq!(
			(
				rel.schema.as_str(),
				rel.name.as_str(),
				rel.columns.len(),
				rel.columns[0].key
			),
			("realtime", "messages", 2, true)
		);

		let mut i = bytes::BytesMut::new();
		i.put_u8(b'I');
		i.put_u32(42);
		i.put_u8(b'N');
		i.put_i16(2);
		i.put_u8(b't');
		i.put_i32(3);
		i.put_slice(b"abc");
		i.put_u8(b'n');
		assert_eq!(
			decode(i.freeze()).unwrap(),
			Message::Insert {
				relation: 42,
				new: vec![Value::Text("abc".into()), Value::Null]
			}
		);
	}

	#[test]
	fn garbage_is_an_error_not_a_panic() {
		for bytes in [&b"R"[..], b"I\0\0", b"U\0\0\0\x01X", b"B\0"] {
			let _ = decode(Bytes::copy_from_slice(bytes));
		}
		assert!(decode(Bytes::from_static(b"I\0\0\0\x01N\0\x01t\0\0\0\x09ab")).is_err());
	}
}
