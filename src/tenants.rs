//! The tenants: one per project, registered by the host agent over the tenant API, kept in the
//! metadata database.
//!
//! **In a schema of its own (`snout_realtime`).** The host agent lists
//! the tenants on every reconcile and registers any this server does not know (`planRealtime`
//! in `packages/snoutpod/src/host/realtime.ts`), so a swap to this server re-registers every
//! project within one tick, and a swap back finds the previous server's rows as it left them: the
//! rollback costs nothing.
//!
//! The project's JWT secret and its database password are encrypted at rest with `DB_ENC_KEY`
//! (AES-256-GCM, the key a SHA-256 of the variable, a fresh 96-bit nonce per value).

use std::collections::HashMap;
use std::sync::Arc;

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;

use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::RwLock;
use tokio_postgres::NoTls;

/// What a tenant registration says, as `tenantPayload` sends it.
#[derive(Debug, Clone, Deserialize)]
pub struct TenantBody {
	pub name: Option<String>,
	pub external_id: String,
	pub jwt_secret: String,
	#[serde(default)]
	pub max_concurrent_users: Option<i64>,
	#[serde(default)]
	pub max_channels_per_client: Option<i64>,
	#[serde(default)]
	pub max_events_per_second: Option<i64>,
	#[serde(default)]
	pub max_joins_per_second: Option<i64>,
	#[serde(default)]
	pub max_presence_events_per_second: Option<i64>,
	#[serde(default)]
	pub max_payload_size_in_kb: Option<i64>,
	#[serde(default)]
	pub private_only: Option<bool>,
	#[serde(default)]
	pub presence_enabled: Option<bool>,
	#[serde(default)]
	pub extensions: Vec<Extension>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Extension {
	#[serde(rename = "type")]
	pub kind: String,
	pub settings: Value,
}

/// The project's database, from the `postgres_cdc_rls` extension's settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Database {
	pub host: String,
	pub port: u16,
	pub name: String,
	pub user: String,
	pub password: String,
	pub publication: String,
	pub slot_name: String,
	pub poll_interval_ms: u64,
	pub poll_max_record_bytes: i64,
	pub poll_max_changes: i64,
}

/// One tenant, decrypted, as the server uses it.
#[derive(Debug, Clone)]
pub struct Tenant {
	pub external_id: String,
	pub name: String,
	pub jwt_secret: String,
	pub max_concurrent_users: i64,
	pub max_channels_per_client: i64,
	pub max_events_per_second: i64,
	pub max_joins_per_second: i64,
	pub max_presence_events_per_second: i64,
	pub max_payload_size_in_kb: i64,
	pub private_only: bool,
	pub presence_enabled: bool,
	/// `None` when the tenant was registered without `postgres_cdc_rls`: it may not serve
	/// postgres_changes at all, whatever a client asks (`RealtimeTenant.postgresChanges`).
	pub database: Option<Database>,
}

impl Tenant {
	/// The row the tenant API answers with. No secret in it.
	pub fn public_json(&self) -> Value {
		json!({
			"external_id": self.external_id,
			"name": self.name,
			"max_concurrent_users": self.max_concurrent_users,
			"max_channels_per_client": self.max_channels_per_client,
			"max_events_per_second": self.max_events_per_second,
			"max_joins_per_second": self.max_joins_per_second,
			"max_presence_events_per_second": self.max_presence_events_per_second,
			"max_payload_size_in_kb": self.max_payload_size_in_kb,
			"private_only": self.private_only,
			"presence_enabled": self.presence_enabled,
			"postgres_changes": self.database.is_some(),
		})
	}
}

#[derive(Debug, thiserror::Error)]
pub enum TenantError {
	#[error("{0}")]
	Invalid(String),
	#[error("the metadata database: {0}")]
	Store(String),
}

/// Encryption of the secret columns.
#[derive(Clone)]
pub struct Sealer {
	cipher: Aes256Gcm,
}

impl Sealer {
	pub fn new(key: &str) -> Sealer {
		let digest = Sha256::digest(key.as_bytes());
		Sealer {
			cipher: Aes256Gcm::new_from_slice(&digest).expect("a SHA-256 digest is a 256-bit key"),
		}
	}

	pub fn seal(&self, plaintext: &str) -> String {
		let mut nonce = [0u8; 12];
		getrandom::fill(&mut nonce).expect("the operating system has randomness");
		let sealed = self
			.cipher
			.encrypt(Nonce::from_slice(&nonce), plaintext.as_bytes())
			.expect("AES-GCM encryption does not fail");
		let mut out = nonce.to_vec();
		out.extend_from_slice(&sealed);
		STANDARD.encode(out)
	}

	pub fn open(&self, sealed: &str) -> Result<String, TenantError> {
		let bytes = STANDARD
			.decode(sealed)
			.map_err(|_| TenantError::Store("a sealed value is not base64".into()))?;
		if bytes.len() < 13 {
			return Err(TenantError::Store("a sealed value is too short".into()));
		}
		let (nonce, body) = bytes.split_at(12);
		let plain = self
			.cipher
			.decrypt(Nonce::from_slice(nonce), body)
			.map_err(|_| {
				TenantError::Store(
					"DB_ENC_KEY does not open a stored secret: was it changed?".into(),
				)
			})?;
		String::from_utf8(plain)
			.map_err(|_| TenantError::Store("a stored secret is not UTF-8".into()))
	}
}

const MIGRATION: &str = "
create schema if not exists snout_realtime;
create table if not exists snout_realtime.tenants (
	external_id text primary key,
	name text not null,
	jwt_secret text not null,
	settings jsonb not null,
	database jsonb,
	inserted_at timestamptz not null default now(),
	updated_at timestamptz not null default now()
);
";

fn as_i64(v: &Value, key: &str, default: i64) -> i64 {
	v.get(key)
		.and_then(|x| {
			x.as_i64()
				.or_else(|| x.as_str().and_then(|s| s.parse().ok()))
		})
		.unwrap_or(default)
}

fn as_string(v: &Value, key: &str) -> Option<String> {
	v.get(key).and_then(|x| match x {
		Value::String(s) => Some(s.clone()),
		Value::Number(n) => Some(n.to_string()),
		_ => None,
	})
}

/// Parse and check a registration: the defaults the pinned server applies, and the database
/// settings when the tenant may serve postgres_changes.
pub fn from_body(body: &TenantBody) -> Result<Tenant, TenantError> {
	if body.external_id.trim().is_empty() {
		return Err(TenantError::Invalid("external_id can't be blank".into()));
	}
	if body.jwt_secret.is_empty() {
		return Err(TenantError::Invalid("jwt_secret can't be blank".into()));
	}
	let database = match body
		.extensions
		.iter()
		.find(|e| e.kind == "postgres_cdc_rls")
	{
		None => None,
		Some(ext) => {
			let s = &ext.settings;
			let need = |k: &str| {
				as_string(s, k)
					.ok_or_else(|| TenantError::Invalid(format!("postgres_cdc_rls needs {k}")))
			};
			Some(Database {
				host: need("db_host")?,
				port: as_i64(s, "db_port", 5432) as u16,
				name: need("db_name")?,
				user: need("db_user")?,
				password: need("db_password")?,
				publication: need("publication")?,
				slot_name: need("slot_name")?,
				poll_interval_ms: as_i64(s, "poll_interval_ms", 100).max(1) as u64,
				poll_max_record_bytes: as_i64(s, "poll_max_record_bytes", 1_048_576),
				poll_max_changes: as_i64(s, "poll_max_changes", 100),
			})
		}
	};
	Ok(Tenant {
		name: body
			.name
			.clone()
			.unwrap_or_else(|| body.external_id.clone()),
		external_id: body.external_id.clone(),
		jwt_secret: body.jwt_secret.clone(),
		max_concurrent_users: body.max_concurrent_users.unwrap_or(200),
		max_channels_per_client: body.max_channels_per_client.unwrap_or(100),
		max_events_per_second: body.max_events_per_second.unwrap_or(100),
		max_joins_per_second: body.max_joins_per_second.unwrap_or(100),
		max_presence_events_per_second: body.max_presence_events_per_second.unwrap_or(1000),
		max_payload_size_in_kb: body.max_payload_size_in_kb.unwrap_or(3000),
		private_only: body.private_only.unwrap_or(false),
		presence_enabled: body.presence_enabled.unwrap_or(false),
		database,
	})
}

/// The tenants, stored, with a read-through cache (a socket connect must not be a query).
pub struct Registry {
	url: String,
	sealer: Sealer,
	cache: RwLock<HashMap<String, Arc<Tenant>>>,
}

impl Registry {
	pub fn new(url: &str, key: &str) -> Registry {
		Registry {
			url: url.to_string(),
			sealer: Sealer::new(key),
			cache: RwLock::new(HashMap::new()),
		}
	}

	async fn client(&self) -> Result<tokio_postgres::Client, TenantError> {
		let (client, connection) = tokio_postgres::connect(&self.url, NoTls)
			.await
			.map_err(|e| TenantError::Store(e.to_string()))?;
		tokio::spawn(async move {
			let _ = connection.await;
		});
		Ok(client)
	}

	pub async fn migrate(&self) -> Result<(), TenantError> {
		self.client()
			.await?
			.batch_execute(MIGRATION)
			.await
			.map_err(|e| TenantError::Store(e.to_string()))
	}

	fn settings_json(t: &Tenant) -> Value {
		json!({
			"name": t.name,
			"max_concurrent_users": t.max_concurrent_users,
			"max_channels_per_client": t.max_channels_per_client,
			"max_events_per_second": t.max_events_per_second,
			"max_joins_per_second": t.max_joins_per_second,
			"max_presence_events_per_second": t.max_presence_events_per_second,
			"max_payload_size_in_kb": t.max_payload_size_in_kb,
			"private_only": t.private_only,
			"presence_enabled": t.presence_enabled,
		})
	}

	fn database_json(&self, d: &Database) -> Value {
		json!({
			"host": d.host, "port": d.port, "name": d.name, "user": d.user,
			"password": self.sealer.seal(&d.password),
			"publication": d.publication, "slot_name": d.slot_name,
			"poll_interval_ms": d.poll_interval_ms, "poll_max_record_bytes": d.poll_max_record_bytes,
			"poll_max_changes": d.poll_max_changes,
		})
	}

	fn tenant_of_row(
		&self,
		external_id: String,
		jwt_secret: &str,
		settings: &Value,
		database: Option<&Value>,
	) -> Result<Tenant, TenantError> {
		let database = match database {
			None | Some(Value::Null) => None,
			Some(d) => Some(Database {
				host: as_string(d, "host").unwrap_or_default(),
				port: as_i64(d, "port", 5432) as u16,
				name: as_string(d, "name").unwrap_or_default(),
				user: as_string(d, "user").unwrap_or_default(),
				password: self
					.sealer
					.open(&as_string(d, "password").unwrap_or_default())?,
				publication: as_string(d, "publication").unwrap_or_default(),
				slot_name: as_string(d, "slot_name").unwrap_or_default(),
				poll_interval_ms: as_i64(d, "poll_interval_ms", 100) as u64,
				poll_max_record_bytes: as_i64(d, "poll_max_record_bytes", 1_048_576),
				poll_max_changes: as_i64(d, "poll_max_changes", 100),
			}),
		};
		Ok(Tenant {
			name: as_string(settings, "name").unwrap_or_else(|| external_id.clone()),
			jwt_secret: self.sealer.open(jwt_secret)?,
			max_concurrent_users: as_i64(settings, "max_concurrent_users", 200),
			max_channels_per_client: as_i64(settings, "max_channels_per_client", 100),
			max_events_per_second: as_i64(settings, "max_events_per_second", 100),
			max_joins_per_second: as_i64(settings, "max_joins_per_second", 100),
			max_presence_events_per_second: as_i64(
				settings,
				"max_presence_events_per_second",
				1000,
			),
			max_payload_size_in_kb: as_i64(settings, "max_payload_size_in_kb", 3000),
			private_only: settings
				.get("private_only")
				.and_then(Value::as_bool)
				.unwrap_or(false),
			presence_enabled: settings
				.get("presence_enabled")
				.and_then(Value::as_bool)
				.unwrap_or(false),
			external_id,
			database,
		})
	}

	/// Create or replace one tenant (the agent upserts on every registration).
	pub async fn put(&self, tenant: Tenant) -> Result<Arc<Tenant>, TenantError> {
		let client = self.client().await?;
		let database = tenant.database.as_ref().map(|d| self.database_json(d));
		client
			.execute(
				"insert into snout_realtime.tenants (external_id, name, jwt_secret, settings, database)
				 values ($1, $2, $3, $4, $5)
				 on conflict (external_id) do update set name = excluded.name, jwt_secret = excluded.jwt_secret,
				   settings = excluded.settings, database = excluded.database, updated_at = now()",
				&[&tenant.external_id, &tenant.name, &self.sealer.seal(&tenant.jwt_secret), &Self::settings_json(&tenant), &database],
			)
			.await
			.map_err(|e| TenantError::Store(e.to_string()))?;
		let tenant = Arc::new(tenant);
		self.cache
			.write()
			.await
			.insert(tenant.external_id.clone(), tenant.clone());
		Ok(tenant)
	}

	/// One tenant, from the cache or the store.
	pub async fn get(&self, external_id: &str) -> Result<Option<Arc<Tenant>>, TenantError> {
		if let Some(t) = self.cache.read().await.get(external_id) {
			return Ok(Some(t.clone()));
		}
		let client = self.client().await?;
		let row = client
			.query_opt(
				"select jwt_secret, settings, database from snout_realtime.tenants where external_id = $1",
				&[&external_id],
			)
			.await
			.map_err(|e| TenantError::Store(e.to_string()))?;
		let Some(row) = row else { return Ok(None) };
		let secret: String = row.get(0);
		let settings: Value = row.get(1);
		let database: Option<Value> = row.get(2);
		let tenant = Arc::new(self.tenant_of_row(
			external_id.to_string(),
			&secret,
			&settings,
			database.as_ref(),
		)?);
		self.cache
			.write()
			.await
			.insert(external_id.to_string(), tenant.clone());
		Ok(Some(tenant))
	}

	/// Forget one. True if it existed.
	pub async fn delete(&self, external_id: &str) -> Result<bool, TenantError> {
		self.cache.write().await.remove(external_id);
		let client = self.client().await?;
		let n = client
			.execute(
				"delete from snout_realtime.tenants where external_id = $1",
				&[&external_id],
			)
			.await
			.map_err(|e| TenantError::Store(e.to_string()))?;
		Ok(n > 0)
	}

	/// Every tenant, in the tenant API's shape.
	pub async fn list(&self) -> Result<Vec<Value>, TenantError> {
		let client = self.client().await?;
		let rows = client
			.query("select external_id, jwt_secret, settings, database from snout_realtime.tenants order by external_id", &[])
			.await
			.map_err(|e| TenantError::Store(e.to_string()))?;
		let mut out = Vec::with_capacity(rows.len());
		for row in rows {
			let id: String = row.get(0);
			let secret: String = row.get(1);
			let settings: Value = row.get(2);
			let database: Option<Value> = row.get(3);
			out.push(
				self.tenant_of_row(id, &secret, &settings, database.as_ref())?
					.public_json(),
			);
		}
		Ok(out)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn sealing_round_trips_and_a_wrong_key_says_so() {
		let s = Sealer::new("one key");
		let sealed = s.seal("the secret");
		assert_ne!(sealed, s.seal("the secret"), "a fresh nonce each time");
		assert_eq!(s.open(&sealed).unwrap(), "the secret");
		assert!(Sealer::new("another key").open(&sealed).is_err());
	}

	#[test]
	fn the_agents_registration_parses() {
		let body: TenantBody = serde_json::from_value(json!({
			"name": "abc", "external_id": "abc", "jwt_secret": "s",
			"max_concurrent_users": 200, "max_channels_per_client": 100, "max_events_per_second": 100,
			"extensions": [{ "type": "postgres_cdc_rls", "settings": {
				"db_host": "snoutpod-abc", "db_name": "abc", "db_user": "project_admin", "db_password": "pw",
				"db_port": "5432", "region": "snoutpod", "poll_interval_ms": 100, "poll_max_record_bytes": 1048576,
				"publication": "snoutdata_realtime", "slot_name": "realtime_slot", "ssl_enforced": false
			}}]
		}))
		.unwrap();
		let t = from_body(&body).unwrap();
		let d = t.database.unwrap();
		assert_eq!(
			(d.port, d.publication.as_str(), d.poll_max_changes),
			(5432, "snoutdata_realtime", 100)
		);
		assert_eq!(t.max_joins_per_second, 100);
	}

	#[test]
	fn a_tenant_without_the_extension_serves_no_changes() {
		let body: TenantBody = serde_json::from_value(
			json!({"external_id": "x", "jwt_secret": "s", "extensions": []}),
		)
		.unwrap();
		assert!(from_body(&body).unwrap().database.is_none());
	}
}
