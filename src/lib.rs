//! snout-realtime: broadcast, presence and database changes over WebSockets, with the
//! project's own row-level security deciding who sees what.
//!
//! One process per host, many projects (tenants). A project is found from the `Host` a socket
//! arrives with, its tokens are verified with its own secret, and everything a subscriber may
//! see from the database is decided by running the check AS that subscriber.

pub mod api;
pub mod changes;
pub mod config;
pub mod db;
pub mod hub;
pub mod inspect;
pub mod jwt;
pub mod messages;
pub mod pgoutput;
pub mod presence;
pub mod protocol;
pub mod replication;
pub mod socket;
pub mod tenants;

/// Everything a request needs.
pub struct App {
	pub config: config::Config,
	pub registry: tenants::Registry,
	pub hub: hub::Hub,
	pub dbs: db::Databases,
}

/// Every hour, for each project this process has set up: the days ahead get their message
/// partitions, and partitions past replay's 72 hours are dropped (what the pinned server's
/// janitor does).
pub async fn upkeep(app: std::sync::Arc<App>) {
	let mut every = tokio::time::interval(std::time::Duration::from_secs(3600));
	// The first tick is immediate, and preparing a project has just done it.
	every.tick().await;
	loop {
		every.tick().await;
		for rt in app.hub.all() {
			if !*rt.prepared.lock().await {
				continue;
			}
			let tenant = rt.tenant();
			let Some(database) = tenant.database.clone() else {
				continue;
			};
			let Ok(pool) = app.dbs.pool(&tenant.external_id, &database).await else {
				continue;
			};
			if let Ok(client) = pool.get().await {
				let _ = db::create_partitions(&client).await;
			}
			let _ = db::drop_old_partitions(&pool).await;
		}
	}
}

/// The entry points the fuzz targets drive: every parser of bytes a
/// client or the database sends. Each must return, whatever it is given; a panic, a hang or an
/// allocation blow-up is the failure.
pub mod fuzz {
	/// A client's frame, as text in both serializers and as a V2 binary frame.
	pub fn frame(data: &[u8]) {
		let _ = crate::protocol::decode_binary(data);
		if let Ok(text) = std::str::from_utf8(data) {
			let _ = crate::protocol::decode_text(crate::protocol::Vsn::V1, text);
			let _ = crate::protocol::decode_text(crate::protocol::Vsn::V2, text);
		}
	}

	/// A logical decoding message from the project's database.
	pub fn pgoutput(data: &[u8]) {
		let _ = crate::pgoutput::decode(bytes::Bytes::copy_from_slice(data));
	}

	/// A token, against a project secret.
	pub fn token(text: &str) {
		let _ = crate::jwt::authorize(text, "a-fuzzing-secret-of-reasonable-length", 1_800_000_000);
	}
}
