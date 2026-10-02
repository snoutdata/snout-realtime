//! A logical replication connection: the part of the Postgres wire protocol tokio-postgres does
//! not speak (a `replication=database` session, `START_REPLICATION`, and the CopyBoth stream
//! that follows).
//!
//! Small on purpose: connect and authenticate (SCRAM-SHA-256, MD5 or cleartext, as the server
//! asks), run a simple query and read its rows as text, start streaming, then read XLogData and
//! keepalives and send standby status updates. Everything is decoded with `postgres-protocol`,
//! the same codec tokio-postgres uses, so no byte layout is re-derived here except the four
//! replication sub-messages the protocol documentation defines:
//!
//! | CopyData starting | Direction | Body |
//! |---|---|---|
//! | `w` XLogData | server | start LSN u64, end LSN u64, send time i64, then the plugin's message |
//! | `k` keepalive | server | end LSN u64, send time i64, reply requested u8 |
//! | `r` standby status | client | written, flushed, applied LSN u64 each, time i64, reply u8 |

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, BytesMut};
use fallible_iterator::FallibleIterator;
use postgres_protocol::authentication::sasl::{ChannelBinding, SCRAM_SHA_256, ScramSha256};
use postgres_protocol::message::backend::{ErrorFields, Message};
use postgres_protocol::message::frontend;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug, thiserror::Error)]
pub enum ReplicationError {
	#[error("connecting: {0}")]
	Io(#[from] std::io::Error),
	#[error("{0}")]
	Protocol(String),
	/// An ErrorResponse, with its SQLSTATE.
	#[error("{code}: {message}")]
	Server { code: String, message: String },
}

fn protocol(what: &str) -> ReplicationError {
	ReplicationError::Protocol(what.to_string())
}

/// Microseconds since 2000-01-01, the protocol's clock.
fn pg_now() -> i64 {
	const PG_EPOCH_OFFSET_SECS: u64 = 946_684_800;
	let now = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap_or(Duration::ZERO);
	(now.as_micros() as i64) - (PG_EPOCH_OFFSET_SECS as i64) * 1_000_000
}

/// Where to connect, and as whom.
#[derive(Debug, Clone)]
pub struct Target {
	pub host: String,
	pub port: u16,
	pub user: String,
	pub password: String,
	pub database: String,
	pub application_name: String,
}

/// An event from the stream.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
	/// A message from the output plugin, ending at `end_lsn`.
	Data {
		start_lsn: u64,
		end_lsn: u64,
		data: bytes::Bytes,
	},
	/// The server's heartbeat. When `reply` is set it wants a status update now.
	Keepalive { end_lsn: u64, reply: bool },
}

enum Backend {
	Message(Message),
	CopyBoth,
}

pub struct Connection {
	stream: TcpStream,
	read: BytesMut,
	write: BytesMut,
}

fn server_error(fields: ErrorFields<'_>) -> ReplicationError {
	let mut code = String::new();
	let mut message = String::new();
	let mut fields = fields;
	while let Ok(Some(field)) = fields.next() {
		match field.type_() {
			b'C' => code = String::from_utf8_lossy(field.value_bytes()).into_owned(),
			b'M' => message = String::from_utf8_lossy(field.value_bytes()).into_owned(),
			_ => {}
		}
	}
	ReplicationError::Server { code, message }
}

impl Connection {
	/// Connect, authenticate, and wait until the server is ready for a query.
	pub async fn connect(target: &Target) -> Result<Connection, ReplicationError> {
		let stream = TcpStream::connect((target.host.as_str(), target.port)).await?;
		stream.set_nodelay(true)?;
		let mut conn = Connection {
			stream,
			read: BytesMut::with_capacity(64 * 1024),
			write: BytesMut::new(),
		};
		let params = [
			("user", target.user.as_str()),
			("database", target.database.as_str()),
			("replication", "database"),
			("application_name", target.application_name.as_str()),
		];
		frontend::startup_message(params.iter().copied(), &mut conn.write)
			.map_err(|e| protocol(&e.to_string()))?;
		conn.flush().await?;
		conn.authenticate(target).await?;
		Ok(conn)
	}

	async fn flush(&mut self) -> Result<(), ReplicationError> {
		self.stream.write_all(&self.write).await?;
		self.write.clear();
		Ok(())
	}

	/// The next backend message, reading as much as it takes.
	async fn message(&mut self) -> Result<Message, ReplicationError> {
		match self.backend().await? {
			Backend::Message(m) => Ok(m),
			Backend::CopyBoth => Err(protocol("an unexpected CopyBothResponse")),
		}
	}

	/// The next backend message, or CopyBothResponse ('W'), which postgres-protocol does not
	/// decode (it is only ever sent to a replication session).
	async fn backend(&mut self) -> Result<Backend, ReplicationError> {
		loop {
			if self.read.len() >= 5 && self.read[0] == b'W' {
				let len =
					u32::from_be_bytes([self.read[1], self.read[2], self.read[3], self.read[4]])
						as usize;
				if self.read.len() > len {
					let _ = self.read.split_to(len + 1);
					return Ok(Backend::CopyBoth);
				}
			} else if let Some(m) =
				Message::parse(&mut self.read).map_err(|e| protocol(&e.to_string()))?
			{
				return Ok(Backend::Message(m));
			}
			if self.stream.read_buf(&mut self.read).await? == 0 {
				return Err(protocol("the server closed the connection"));
			}
		}
	}

	async fn authenticate(&mut self, target: &Target) -> Result<(), ReplicationError> {
		loop {
			match self.message().await? {
				Message::AuthenticationOk => {}
				Message::AuthenticationCleartextPassword => {
					frontend::password_message(target.password.as_bytes(), &mut self.write)
						.map_err(|e| protocol(&e.to_string()))?;
					self.flush().await?;
				}
				Message::AuthenticationMd5Password(body) => {
					let hash = postgres_protocol::authentication::md5_hash(
						target.user.as_bytes(),
						target.password.as_bytes(),
						body.salt(),
					);
					frontend::password_message(hash.as_bytes(), &mut self.write)
						.map_err(|e| protocol(&e.to_string()))?;
					self.flush().await?;
				}
				Message::AuthenticationSasl(body) => {
					let mut mechanisms = body.mechanisms();
					let mut scram = false;
					while let Some(m) = mechanisms.next().map_err(|e| protocol(&e.to_string()))? {
						scram |= m == SCRAM_SHA_256;
					}
					if !scram {
						return Err(protocol(
							"the server offers no SASL mechanism this client speaks",
						));
					}
					let mut state =
						ScramSha256::new(target.password.as_bytes(), ChannelBinding::unsupported());
					frontend::sasl_initial_response(
						SCRAM_SHA_256,
						state.message(),
						&mut self.write,
					)
					.map_err(|e| protocol(&e.to_string()))?;
					self.flush().await?;
					let body = match self.message().await? {
						Message::AuthenticationSaslContinue(body) => body,
						Message::ErrorResponse(body) => return Err(server_error(body.fields())),
						_ => return Err(protocol("expected SASL continue")),
					};
					state
						.update(body.data())
						.map_err(|e| protocol(&e.to_string()))?;
					frontend::sasl_response(state.message(), &mut self.write)
						.map_err(|e| protocol(&e.to_string()))?;
					self.flush().await?;
					// A wrong password is an ErrorResponse here, and it used to be reported as
					// "expected SASL final", which named the protocol rather than the problem.
					let body = match self.message().await? {
						Message::AuthenticationSaslFinal(body) => body,
						Message::ErrorResponse(body) => return Err(server_error(body.fields())),
						_ => return Err(protocol("expected SASL final")),
					};
					state
						.finish(body.data())
						.map_err(|e| protocol(&e.to_string()))?;
				}
				Message::ErrorResponse(body) => return Err(server_error(body.fields())),
				Message::ReadyForQuery(_) => return Ok(()),
				_ => {}
			}
		}
	}

	/// A simple query; each row's columns as text. Replication commands answer this way too.
	pub async fn simple_query(
		&mut self,
		sql: &str,
	) -> Result<Vec<Vec<Option<String>>>, ReplicationError> {
		frontend::query(sql, &mut self.write).map_err(|e| protocol(&e.to_string()))?;
		self.flush().await?;
		let mut rows = Vec::new();
		let mut failure = None;
		loop {
			match self.message().await? {
				Message::DataRow(row) => {
					let mut out = Vec::new();
					let buffer = row.buffer();
					let mut ranges = row.ranges();
					while let Some(range) = ranges.next().map_err(|e| protocol(&e.to_string()))? {
						out.push(range.map(|r| String::from_utf8_lossy(&buffer[r]).into_owned()));
					}
					rows.push(out);
				}
				Message::ErrorResponse(body) => failure = Some(server_error(body.fields())),
				Message::ReadyForQuery(_) => break,
				_ => {}
			}
		}
		match failure {
			Some(e) => Err(e),
			None => Ok(rows),
		}
	}

	/// Send `START_REPLICATION ...` and wait for the stream to open.
	pub async fn start(&mut self, command: &str) -> Result<(), ReplicationError> {
		frontend::query(command, &mut self.write).map_err(|e| protocol(&e.to_string()))?;
		self.flush().await?;
		loop {
			match self.backend().await? {
				Backend::CopyBoth => return Ok(()),
				Backend::Message(Message::ErrorResponse(body)) => {
					let e = server_error(body.fields());
					// The server follows an error with ReadyForQuery; drain it so the
					// connection stays usable for another command.
					while !matches!(self.message().await?, Message::ReadyForQuery(_)) {}
					return Err(e);
				}
				_ => {}
			}
		}
	}

	/// The next event from an open stream.
	pub async fn next(&mut self) -> Result<Event, ReplicationError> {
		loop {
			match self.message().await? {
				Message::CopyData(body) => {
					let mut data = body.into_bytes();
					if data.is_empty() {
						continue;
					}
					match data.get_u8() {
						b'w' if data.len() >= 24 => {
							let start_lsn = data.get_u64();
							let end_lsn = data.get_u64();
							let _sent = data.get_i64();
							return Ok(Event::Data {
								start_lsn,
								end_lsn,
								data,
							});
						}
						b'k' if data.len() >= 17 => {
							let end_lsn = data.get_u64();
							let _sent = data.get_i64();
							let reply = data.get_u8() == 1;
							return Ok(Event::Keepalive { end_lsn, reply });
						}
						_ => continue,
					}
				}
				Message::ErrorResponse(body) => return Err(server_error(body.fields())),
				Message::CopyDone => {
					return Err(protocol("the server ended the replication stream"));
				}
				_ => {}
			}
		}
	}

	/// Tell the server everything up to `lsn` is done with, so it can recycle the WAL.
	pub async fn ack(&mut self, lsn: u64) -> Result<(), ReplicationError> {
		let mut body = BytesMut::with_capacity(34);
		body.put_u8(b'r');
		body.put_u64(lsn);
		body.put_u64(lsn);
		body.put_u64(lsn);
		body.put_i64(pg_now());
		body.put_u8(0);
		// CopyData: 'd', length (including itself), body.
		self.write.put_u8(b'd');
		self.write.put_i32(4 + body.len() as i32);
		self.write.extend_from_slice(&body);
		self.flush().await
	}
}

/// `0/16B3748` as a number.
pub fn parse_lsn(text: &str) -> Option<u64> {
	let (hi, lo) = text.split_once('/')?;
	Some((u64::from_str_radix(hi, 16).ok()? << 32) | u64::from_str_radix(lo, 16).ok()?)
}

/// A number as `0/16B3748`.
pub fn format_lsn(lsn: u64) -> String {
	format!("{:X}/{:X}", lsn >> 32, lsn & 0xffff_ffff)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn lsn_round_trips() {
		assert_eq!(parse_lsn("0/16B3748"), Some(0x16B3748));
		assert_eq!(parse_lsn("1/0"), Some(1 << 32));
		assert_eq!(format_lsn(0x1_0000_00FF), "1/FF");
		assert_eq!(parse_lsn("nonsense"), None);
	}
}
