//! A project's own database: the realtime schema, the daily message partitions, authorisation
//! AS the subscriber, replay, and postgres_changes subscriptions.
//!
//! **This server never decides who may read or write a topic, or see a row; the project's
//! database does.** Authorisation is the pinned server's method, statement for statement: in a
//! transaction that is always rolled back, a probe row per extension is inserted into
//! `realtime.messages` for the topic, the connection BECOMES the subscriber
//! (`set_config('role', ...)` plus the claims and headers the customer's policies read), and the
//! customer's SELECT policies decide which probe rows come back (read) or their INSERT policies
//! decide which a subscriber may add (write).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod, Runtime};
use serde_json::{Map, Value};
use tokio::sync::Mutex;
use tokio_postgres::NoTls;

use crate::tenants::Database;

const SCHEMA: &str = include_str!("../migrations/0001_realtime.sql");
const CHANGES: &str = include_str!("../migrations/0002_changes.sql");

#[derive(Debug, thiserror::Error)]
pub enum DbError {
	/// The project database cannot be reached.
	#[error("unavailable: {0}")]
	Unavailable(String),
	/// A statement failed; the SQLSTATE and the message.
	#[error("{code}: {message}")]
	Sql { code: String, message: String },
}

impl DbError {
	pub fn code(&self) -> &str {
		match self {
			DbError::Sql { code, .. } => code,
			DbError::Unavailable(_) => "",
		}
	}
}

impl DbError {
	pub fn from_pg(e: tokio_postgres::Error) -> DbError {
		sql_error(e)
	}
}

fn sql_error(e: tokio_postgres::Error) -> DbError {
	match e.as_db_error() {
		Some(db) => DbError::Sql {
			code: db.code().code().to_string(),
			message: db.message().to_string(),
		},
		None => DbError::Unavailable(e.to_string()),
	}
}

fn pool_error(e: deadpool_postgres::PoolError) -> DbError {
	match e {
		deadpool_postgres::PoolError::Backend(e) => sql_error(e),
		other => DbError::Unavailable(other.to_string()),
	}
}

/// Who is asking, and about which topic: what the customer's policies read.
#[derive(Debug, Clone)]
pub struct AuthContext {
	pub topic: String,
	pub role: String,
	pub sub: Option<String>,
	pub claims: Map<String, Value>,
	pub headers: Map<String, Value>,
}

/// What a subscriber may do on a private topic, per extension. `None` is "not asked yet".
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Policies {
	pub broadcast_read: Option<bool>,
	pub broadcast_write: Option<bool>,
	pub presence_read: Option<bool>,
	pub presence_write: Option<bool>,
}

/// One broadcast kept in realtime.messages, for replay.
#[derive(Debug, Clone)]
pub struct Stored {
	pub id: String,
	pub event: String,
	pub payload: Value,
}

/// The project databases this process holds a pool for.
#[derive(Default)]
pub struct Databases {
	pools: Mutex<HashMap<String, (Database, Pool)>>,
}

impl Databases {
	/// The pool for one project, made on first use, and made again if its settings changed.
	pub async fn pool(&self, tenant: &str, db: &Database) -> Result<Pool, DbError> {
		let mut pools = self.pools.lock().await;
		if let Some((settings, pool)) = pools.get(tenant)
			&& settings == db
		{
			return Ok(pool.clone());
		}
		let pool = new_pool(db, 4)?;
		pools.insert(tenant.to_string(), (db.clone(), pool.clone()));
		Ok(pool)
	}

	/// Drop a project's pool (the tenant was deleted or its database moved).
	pub async fn forget(&self, tenant: &str) {
		self.pools.lock().await.remove(tenant);
	}
}

/// A pool of at most `size` connections to one database, each made when first asked for.
pub fn new_pool(db: &Database, size: usize) -> Result<Pool, DbError> {
	let mut config = tokio_postgres::Config::new();
	config
		.host(&db.host)
		.port(db.port)
		.dbname(&db.name)
		.user(&db.user)
		.password(&db.password)
		.application_name("snout_realtime")
		.connect_timeout(Duration::from_secs(10));
	let manager = Manager::from_config(
		config,
		NoTls,
		ManagerConfig {
			recycling_method: RecyclingMethod::Fast,
		},
	);
	Pool::builder(manager)
		.max_size(size)
		.runtime(Runtime::Tokio1)
		.wait_timeout(Some(Duration::from_secs(10)))
		.create_timeout(Some(Duration::from_secs(10)))
		.build()
		.map_err(|e| DbError::Unavailable(e.to_string()))
}

/// Make a project ready: its realtime schema (only when it has none: one the pinned server set up
/// already has the same tables, and is left as it is), snout-realtime's own functions (made in
/// every project, and made again when their file changes), and today's message partitions.
/// Run once per project per process, before anything else, one process at a time per project.
pub async fn prepare(pool: &Pool) -> Result<(), DbError> {
	let mut client = pool.get().await.map_err(pool_error)?;
	let tx = client.transaction().await.map_err(sql_error)?;
	tx.execute(
		"select pg_advisory_xact_lock(hashtext('snout_realtime.prepare'))",
		&[],
	)
	.await
	.map_err(sql_error)?;
	let has_schema: bool = tx
		.query_one("select to_regclass('realtime.messages') is not null", &[])
		.await
		.map_err(sql_error)?
		.get(0);
	if !has_schema {
		tx.batch_execute(SCHEMA).await.map_err(sql_error)?;
	}
	tx.batch_execute(
		"create schema if not exists snout_realtime;
		 create table if not exists snout_realtime.migrations (
			name text primary key, sha256 text not null, applied_at timestamptz not null default now())",
	)
	.await
	.map_err(sql_error)?;
	let hash = sha256_hex(CHANGES);
	let current: Option<String> = tx
		.query_opt(
			"select sha256 from snout_realtime.migrations where name = 'changes'",
			&[],
		)
		.await
		.map_err(sql_error)?
		.map(|r| r.get(0));
	if current.as_deref() != Some(hash.as_str()) {
		tx.batch_execute(CHANGES).await.map_err(sql_error)?;
		tx.execute(
			"insert into snout_realtime.migrations (name, sha256) values ('changes', $1)
			 on conflict (name) do update set sha256 = excluded.sha256, applied_at = now()",
			&[&hash],
		)
		.await
		.map_err(sql_error)?;
	}
	tx.commit().await.map_err(sql_error)?;
	create_partitions(&client).await
}

fn sha256_hex(text: &str) -> String {
	use sha2::{Digest, Sha256};
	Sha256::digest(text.as_bytes())
		.iter()
		.map(|b| format!("{b:02x}"))
		.collect()
}

/// `realtime.messages_YYYY_MM_DD` from yesterday to three days ahead, as the pinned server
/// keeps them, so `realtime.send()` always has a partition to land in.
pub async fn create_partitions(client: &deadpool_postgres::Object) -> Result<(), DbError> {
	client
		.batch_execute(
			"do $$
			declare d date;
			begin
				for d in select generate_series(current_date - 1, current_date + 3, interval '1 day')::date loop
					begin
						execute format('create table if not exists realtime.%I partition of realtime.messages for values from (%L) to (%L)',
							'messages_' || to_char(d, 'YYYY_MM_DD'), d, d + 1);
					exception when duplicate_table then null;
					end;
				end loop;
			end $$",
		)
		.await
		.map_err(sql_error)
}

/// Drop partitions older than 72 hours (the pinned server's retention for replay).
pub async fn drop_old_partitions(pool: &Pool) -> Result<(), DbError> {
	let client = pool.get().await.map_err(pool_error)?;
	client
		.batch_execute(
			"do $$
			declare r record;
			begin
				for r in select c.relname from pg_inherits i join pg_class p on i.inhparent = p.oid join pg_class c on i.inhrelid = c.oid
					join pg_namespace n on n.oid = c.relnamespace
					where p.relname = 'messages' and n.nspname = 'realtime' and c.relname ~ '^messages_\\d{4}_\\d{2}_\\d{2}$'
				loop
					if to_date(substr(r.relname, 10), 'YYYY_MM_DD') < (now() - interval '72 hours')::date then
						execute format('drop table if exists realtime.%I', r.relname);
					end if;
				end loop;
			end $$",
		)
		.await
		.map_err(sql_error)
}

pub fn quote_ident(name: &str) -> String {
	format!("\"{}\"", name.replace('"', "\"\""))
}

const SET_CONFIG: &str =
	"select set_config('role', $1, true), set_config('realtime.topic', $2, true),
	set_config('request.jwt.claims', $3, true), set_config('request.jwt.claim.sub', $4, true),
	set_config('request.jwt.claim.role', $5, true), set_config('request.headers', $6, true)";

/// Which extensions a read or a write check covers: presence too, unless presence is off for
/// this channel.
fn extensions(presence: bool) -> &'static [&'static str] {
	if presence {
		&["broadcast", "presence"]
	} else {
		&["broadcast"]
	}
}

async fn become_subscriber(
	tx: &deadpool_postgres::Transaction<'_>,
	ctx: &AuthContext,
) -> Result<(), DbError> {
	let claims = Value::Object(ctx.claims.clone()).to_string();
	let headers = Value::Object(ctx.headers.clone()).to_string();
	let sub = ctx.sub.clone().unwrap_or_default();
	tx.execute(
		SET_CONFIG,
		&[&ctx.role, &ctx.topic, &claims, &sub, &ctx.role, &headers],
	)
	.await
	.map_err(sql_error)?;
	Ok(())
}

/// Read authorisation: which probe rows the subscriber's SELECT policies let it see.
pub async fn authorize_read(
	pool: &Pool,
	ctx: &AuthContext,
	presence: bool,
	mut policies: Policies,
) -> Result<Policies, DbError> {
	let mut client = pool.get().await.map_err(pool_error)?;
	let tx = client.transaction().await.map_err(sql_error)?;
	let mut ids = HashMap::new();
	for ext in extensions(presence) {
		let row = tx
			.query_one(
				"insert into realtime.messages (topic, extension) values ($1, $2) returning id::text",
				&[&ctx.topic, ext],
			)
			.await
			.map_err(sql_error)?;
		ids.insert(*ext, row.get::<_, String>(0));
	}
	become_subscriber(&tx, ctx).await?;
	let wanted: Vec<String> = ids.values().cloned().collect();
	let seen: Vec<String> = tx
		.query(
			"select id::text from realtime.messages where topic = $1 and id::text = any($2)",
			&[&ctx.topic, &wanted],
		)
		.await
		.map_err(sql_error)?
		.into_iter()
		.map(|r| r.get(0))
		.collect();
	tx.rollback().await.map_err(sql_error)?;
	let can = |ext: &str| ids.get(ext).map(|id| seen.contains(id)).unwrap_or(false);
	policies.broadcast_read = Some(can("broadcast"));
	policies.presence_read = Some(can("presence"));
	Ok(policies)
}

/// Write authorisation: which probe rows the subscriber's INSERT policies let it add. The
/// pinned server probes BOTH extensions whichever is being used; ours probes only the one
/// asked for (probing the other logs a row-level security error in the customer's Postgres).
pub async fn authorize_write(
	pool: &Pool,
	ctx: &AuthContext,
	extension: &str,
	mut policies: Policies,
) -> Result<Policies, DbError> {
	let mut client = pool.get().await.map_err(pool_error)?;
	let tx = client.transaction().await.map_err(sql_error)?;
	become_subscriber(&tx, ctx).await?;
	tx.batch_execute("savepoint probe")
		.await
		.map_err(sql_error)?;
	let allowed = match tx
		.execute(
			"insert into realtime.messages (topic, extension) values ($1, $2)",
			&[&ctx.topic, &extension],
		)
		.await
	{
		Ok(_) => true,
		Err(e) if e.as_db_error().map(|d| d.code().code()) == Some("42501") => false,
		Err(e) => return Err(sql_error(e)),
	};
	tx.rollback().await.map_err(sql_error)?;
	match extension {
		"presence" => policies.presence_write = Some(allowed),
		_ => policies.broadcast_write = Some(allowed),
	}
	Ok(policies)
}

/// The last `limit` private broadcasts on a topic since `since` (milliseconds), oldest first.
/// Ordered by insertion with a tie-break, so several sent in one transaction come back in the
/// order they were sent.
pub async fn replay(
	pool: &Pool,
	topic: &str,
	since_ms: i64,
	limit: i64,
) -> Result<Vec<Stored>, DbError> {
	let client = pool.get().await.map_err(pool_error)?;
	let limit = limit.clamp(1, 25);
	let rows = client
		.query(
			"select id::text, event, payload from (
				select id, event, payload, inserted_at, ctid from realtime.messages
				where topic = $1 and private is true and extension = 'broadcast'
				  and inserted_at >= to_timestamp($2::float8 / 1000) at time zone 'utc'
				  and inserted_at < (now() at time zone 'utc') + interval '1 minute'
				order by inserted_at desc, ctid desc limit $3
			) m order by inserted_at, ctid",
			&[&topic, &(since_ms as f64), &limit],
		)
		.await
		.map_err(sql_error)?;
	Ok(rows
		.into_iter()
		.map(|r| Stored {
			id: r.get(0),
			event: r.get::<_, Option<String>>(1).unwrap_or_default(),
			payload: r.get::<_, Option<Value>>(2).unwrap_or(Value::Null),
		})
		.collect())
}

/// Shared handle type.
pub type Shared = Arc<Databases>;
