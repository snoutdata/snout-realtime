//! Configuration, from the environment. The names are the ones a realtime server on SnoutData
//! Cloud is already given, so moving a host to this server is an image change and nothing else.
//!
//! | Variable | Default | Secret | What |
//! |---|---|---|---|
//! | `PORT` | `4000` | no | The one port: sockets, the tenant API, `/metrics`, `/` |
//! | `HOST` | `0.0.0.0` | no | Where it listens |
//! | `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME` | port `5432` | the password | The metadata database that holds the tenants |
//! | `API_JWT_SECRET` | none, required | yes | Signs the tenant API's bearer token |
//! | `METRICS_JWT_SECRET` | none, required | yes | Signs `/metrics`' bearer token |
//! | `DB_ENC_KEY` | none, required | yes | Encrypts each tenant's JWT secret and database password at rest |
//! | `SNOUT_REALTIME_MAX_SOCKETS_PER_ADDRESS` | `100` | no | Sockets one client address may hold per project; `0` for no cap |
//!
//! `SECRET_KEY_BASE`, `APP_NAME`, `SEED_SELF_HOST`, `RUN_JANITOR`, `DNS_NODES`,
//! `ERL_AFLAGS` and `DB_SSL` are accepted and ignored: they configure a BEAM cluster and a
//! Phoenix endpoint this server does not have. There is no default secret anywhere: a
//! missing one is a refusal to start.

use std::env;

#[derive(Debug, Clone)]
pub struct Config {
	pub host: String,
	pub port: u16,
	pub metadata_url: String,
	pub api_jwt_secret: String,
	pub metrics_jwt_secret: String,
	pub encryption_key: String,
	pub max_sockets_per_address: usize,
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ConfigError(pub String);

fn required(name: &str) -> Result<String, ConfigError> {
	match env::var(name) {
		Ok(v) if !v.is_empty() => Ok(v),
		_ => Err(ConfigError(format!(
			"{name} is required and has no default"
		))),
	}
}

fn or(name: &str, default: &str) -> String {
	env::var(name)
		.ok()
		.filter(|v| !v.is_empty())
		.unwrap_or_else(|| default.to_string())
}

impl Config {
	pub fn from_env() -> Result<Config, ConfigError> {
		let port = or("PORT", "4000")
			.parse()
			.map_err(|_| ConfigError("PORT is not a port number".into()))?;
		let db_port = or("DB_PORT", "5432");
		let user = required("DB_USER")?;
		let password = required("DB_PASSWORD")?;
		let host = required("DB_HOST")?;
		let name = required("DB_NAME")?;
		let metadata_url = format!(
			"postgres://{}:{}@{}:{}/{}",
			percent_encoding::utf8_percent_encode(&user, percent_encoding::NON_ALPHANUMERIC),
			percent_encoding::utf8_percent_encode(&password, percent_encoding::NON_ALPHANUMERIC),
			host,
			db_port,
			percent_encoding::utf8_percent_encode(&name, percent_encoding::NON_ALPHANUMERIC),
		);
		Ok(Config {
			host: or("HOST", "0.0.0.0"),
			port,
			metadata_url,
			api_jwt_secret: required("API_JWT_SECRET")?,
			metrics_jwt_secret: required("METRICS_JWT_SECRET")?,
			encryption_key: required("DB_ENC_KEY")?,
			max_sockets_per_address: or("SNOUT_REALTIME_MAX_SOCKETS_PER_ADDRESS", "100")
				.parse()
				.map_err(|_| {
					ConfigError("SNOUT_REALTIME_MAX_SOCKETS_PER_ADDRESS is not a number".into())
				})?,
		})
	}
}
