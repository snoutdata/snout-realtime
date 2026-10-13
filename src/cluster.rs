//! A sharded project: one database spread over several Postgres nodes by snout-lepis, whose shape
//! is the `lepis` catalog on the HOME node (the database the tenant is registered with).
//!
//! A sharded table's rows live on whichever node owns their key, so its changes are in THAT
//! node's WAL. For such a project the changes stream (`changes.rs`) is one stream per node, all
//! delivering to the same subscribers:
//!
//!  - The home node's stream is the one every project has, for every published table.
//!  - Every other node has its own, for its SHARDED tables only. A reference table is written on
//!    every node at once (two-phase commit) and only home's copy is streamed; a global table is on
//!    home alone.
//!  - A subscription's rows (`realtime.subscription`) are written on every node, and a change is
//!    decided on the node it came from, as the subscriber, by that node's copy of the table's
//!    policies. Authorisation is unchanged: the same check, in the database the row is in.
//!  - The node list is followed: `LISTEN lepis_epoch` on home, read again on each notification
//!    and every 30 s. A node's stream is opened before anybody waiting is told the stream is ready,
//!    and the epoch is acknowledged in `lepis.router` once the new nodes' streams are open, so a
//!    cutover onto a brand-new node waits for them (best effort: skipped when this role may not
//!    write there).
//!  - The publication is the home node's: a node's copy is made to hold the same sharded tables
//!    (`mirror_publication`), when this role may.
//!
//! What Lepis does to move rows must not reach a subscriber as if somebody had changed them:
//!
//!  - Rows copied onto a node arrive through logical replication, and their transactions carry
//!    an ORIGIN; in a sharded project those are skipped on every node.
//!  - Rows deleted from a node that no longer owns them (the cleanup after a split or a move)
//!    fail that node's ownership fence, the `lepis_owns` CHECK on the table; a DELETE whose old
//!    key fails it is not sent (`disowned`).
//!
//! Changes from different nodes are not ordered against each other, as a read across shards sees
//! each shard at its own moment (L9); each node's are in its commit order.
//!
//! A project without a `lepis` schema meets none of this: one catalog probe when its stream opens,
//! and one every 30 s while it is open, so a project that BECOMES sharded is followed.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use deadpool_postgres::Pool;
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot, watch};

use crate::changes::{Change, Changes};
use crate::db::{self, quote_ident};
use crate::hub::TenantRt;
use crate::pgoutput::Relation;
use crate::tenants::Database;

/// How often the catalog is read again when no notification has come.
const POLL: Duration = Duration::from_secs(30);
/// How often the acknowledgement in `lepis.router` is written again (a cutover counts it while it
/// is under 15 s old).
const HEARTBEAT: Duration = Duration::from_secs(5);

/// One node, as this server reaches it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Member {
	pub id: i32,
	pub name: String,
	/// The `peer_host` label when the node has one (how the other nodes reach it; the `host`
	/// column is the router's view, `127.0.0.1` for a Cloud home node), else `host`.
	pub host: String,
	pub port: u16,
	pub dbname: String,
	pub home: bool,
}

impl Member {
	/// The tenant's database settings, pointed at this node. Lepis gives every node the same
	/// roles with the same password verifiers (L11), so the tenant's login works on each.
	pub fn database(&self, home: &Database) -> Database {
		Database {
			host: self.host.clone(),
			port: self.port,
			name: self.dbname.clone(),
			..home.clone()
		}
	}
}

/// A cluster's nodes and sharded tables, at one epoch.
#[derive(Debug, Clone, Default)]
pub struct Shape {
	pub epoch: i64,
	/// Every node not removed, the home node among them.
	pub members: Vec<Member>,
	sharded: HashSet<(String, String)>,
}

impl Shape {
	pub fn new(epoch: i64, members: Vec<Member>, sharded: &[(&str, &str)]) -> Shape {
		Shape {
			epoch,
			members,
			sharded: sharded
				.iter()
				.map(|(s, t)| (s.to_string(), t.to_string()))
				.collect(),
		}
	}

	pub fn is_sharded(&self, schema: &str, table: &str) -> bool {
		self.sharded
			.contains(&(schema.to_string(), table.to_string()))
	}
}

/// Whether the database is a Lepis cluster's home node. Reads `pg_namespace`, which needs no
/// privilege on the schema.
pub async fn is_cluster(client: &tokio_postgres::Client) -> Result<bool, tokio_postgres::Error> {
	Ok(client
		.query_one(
			"select exists (select from pg_namespace where nspname = 'lepis')",
			&[],
		)
		.await?
		.get(0))
}

const SHAPE_SQL: &str = "select (select epoch from lepis.cluster where id = 1),
	(select coalesce(json_agg(json_build_object('id', id, 'name', name,
		'host', coalesce(labels ->> 'peer_host', host), 'port', port, 'dbname', dbname,
		'home', kind = 'home') order by id), '[]') from lepis.node where state <> 'removed'),
	(select coalesce(json_agg(json_build_array(schema_name, table_name)), '[]')
		from lepis.relation where kind = 'sharded')";

/// The catalog's shape, in one statement (one snapshot).
pub async fn read(client: &tokio_postgres::Client) -> Result<Shape, String> {
	let row = client
		.query_one(SHAPE_SQL, &[])
		.await
		.map_err(|e| match e.as_db_error() {
			Some(db) => format!("{}: {}", db.code().code(), db.message()),
			None => e.to_string(),
		})?;
	let epoch: Option<i64> = row.get(0);
	let members: Vec<Member> =
		serde_json::from_value(row.get::<_, Value>(1)).map_err(|e| e.to_string())?;
	let pairs: Vec<(String, String)> =
		serde_json::from_value(row.get::<_, Value>(2)).map_err(|e| e.to_string())?;
	Ok(Shape {
		epoch: epoch.unwrap_or(0),
		members,
		sharded: pairs.into_iter().collect(),
	})
}

/// The shape, when the database is a cluster's home node and its catalog can be read. A catalog
/// this role may not read is a warning, and the project is streamed from home alone, as before.
pub(crate) async fn probe(client: &tokio_postgres::Client, tenant: &str) -> Option<Shape> {
	match is_cluster(client).await {
		Ok(true) => {}
		Ok(false) => return None,
		Err(e) => {
			tracing::debug!(tenant, error = %e, "looking for a lepis catalog");
			return None;
		}
	}
	match read(client).await {
		Ok(shape) if shape.members.iter().any(|m| !m.home) => Some(shape),
		Ok(_) => None,
		Err(e) => {
			tracing::warn!(tenant, error = %e, "the project is sharded but its lepis catalog cannot be read; changes come from the home node only (grant usage on schema lepis and select on its tables to the role Realtime connects as)");
			None
		}
	}
}

/// The rows of a batch of DELETEs (by position from 0) that this node deleted because it no
/// longer owns them: their old key fails the table's `lepis_owns` fence as it is now. A table
/// with no fence has none; an old row without the key passes.
pub(crate) async fn disowned(
	client: &tokio_postgres::Client,
	relation: &Relation,
	changes: &[Change],
) -> Result<HashSet<usize>, tokio_postgres::Error> {
	let fence: Option<String> = client
		.query_opt(
			"select pg_get_expr(conbin, conrelid) from pg_constraint
			 where conrelid = $1::oid and conname = 'lepis_owns' and contype = 'c'",
			&[&relation.id],
		)
		.await?
		.and_then(|row| row.get(0));
	let Some(fence) = fence else {
		return Ok(HashSet::new());
	};
	let old: Vec<Option<&Value>> = changes.iter().map(|c| c.old.as_ref()).collect();
	// The fence names the table's columns unqualified; they resolve to the record's, which is the
	// innermost FROM item.
	let sql = format!(
		"select coalesce(array_agg(lepis_o::int), '{{}}') from unnest($1::jsonb[]) with ordinality as lepis_u(lepis_j, lepis_o)
		 where exists (select from jsonb_populate_record(null::{}.{}, lepis_j) where ({fence}) is false)",
		quote_ident(&relation.schema),
		quote_ident(&relation.name)
	);
	let positions: Vec<i32> = client.query_one(&sql, &[&old]).await?.get(0);
	Ok(positions
		.into_iter()
		.filter(|p| *p >= 1)
		.map(|p| p as usize - 1)
		.collect())
}

const PUBLISHED: &str =
	"select schemaname::text, tablename::text from pg_publication_tables where pubname = $1";

/// Make a node's publication hold the sharded tables home's holds, and no other sharded table.
/// The statements run, in order. Needs the rights `alter publication` needs on the node (its
/// owner and the tables' owner, or a superuser); the error names the statement it could not run.
pub async fn mirror_publication(
	home: &tokio_postgres::Client,
	node: &tokio_postgres::Client,
	publication: &str,
	shape: &Shape,
) -> Result<Vec<String>, String> {
	let sharded = |rows: Vec<tokio_postgres::Row>| -> BTreeSet<(String, String)> {
		rows.into_iter()
			.map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
			.filter(|(s, t)| shape.is_sharded(s, t))
			.collect()
	};
	let wanted = sharded(
		home.query(PUBLISHED, &[&publication])
			.await
			.map_err(|e| e.to_string())?,
	);
	let exists: bool = node
		.query_one(
			"select exists (select from pg_publication where pubname = $1)",
			&[&publication],
		)
		.await
		.map_err(|e| e.to_string())?
		.get(0);
	let have = if exists {
		sharded(
			node.query(PUBLISHED, &[&publication])
				.await
				.map_err(|e| e.to_string())?,
		)
	} else {
		BTreeSet::new()
	};
	let mut add = Vec::new();
	for (s, t) in wanted.difference(&have) {
		let present: bool = node
			.query_one(
				"select to_regclass(format('%I.%I', $1::text, $2::text)) is not null",
				&[s, t],
			)
			.await
			.map_err(|e| e.to_string())?
			.get(0);
		if present {
			add.push(format!("{}.{}", quote_ident(s), quote_ident(t)));
		}
	}
	let drop: Vec<String> = have
		.difference(&wanted)
		.map(|(s, t)| format!("{}.{}", quote_ident(s), quote_ident(t)))
		.collect();
	let name = quote_ident(publication);
	let mut statements = Vec::new();
	if !exists {
		statements.push(if add.is_empty() {
			format!("create publication {name}")
		} else {
			format!("create publication {name} for table {}", add.join(", "))
		});
	} else {
		if !add.is_empty() {
			statements.push(format!(
				"alter publication {name} add table {}",
				add.join(", ")
			));
		}
		if !drop.is_empty() {
			statements.push(format!(
				"alter publication {name} drop table {}",
				drop.join(", ")
			));
		}
	}
	for sql in &statements {
		node.batch_execute(sql)
			.await
			.map_err(|e| match e.as_db_error() {
				Some(db) => format!("{sql}: {}", db.message()),
				None => format!("{sql}: {e}"),
			})?;
	}
	Ok(statements)
}

const ACK_SQL: &str = "insert into lepis.router (id, epoch, seen_at) values ($1, $2, now())
	on conflict (id) do update set epoch = excluded.epoch, seen_at = now()";

/// This server's row in `lepis.router`: a cutover waits for every live row to reach its epoch,
/// which here means "the new nodes' streams are open". Given up for the follower's life the first
/// time the role may not write there, since a cutover only waits for rows that exist.
struct Acks {
	id: String,
	on: bool,
}

impl Acks {
	fn new() -> Acks {
		let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "local".into());
		Acks {
			id: format!("snout-realtime/{host}/{}", std::process::id()),
			on: true,
		}
	}

	async fn ack(&mut self, client: &tokio_postgres::Client, epoch: i64, tenant: &str) {
		if !self.on || epoch == 0 {
			return;
		}
		if let Err(e) = client.execute(ACK_SQL, &[&self.id, &epoch]).await {
			self.on = false;
			tracing::info!(tenant, error = %e, "not acknowledging lepis epochs; a cutover onto a new node may run before its stream opens");
		}
	}
}

/// Ends the task it holds when dropped.
struct Aborting(tokio::task::JoinHandle<()>);

impl Drop for Aborting {
	fn drop(&mut self) {
		self.0.abort();
	}
}

/// The follower of one sharded project and the node streams it keeps. Held by the home stream:
/// dropped, every one of them ends, and their temporary slots go with their connections.
pub(crate) struct Follower {
	_task: Aborting,
}

/// Start following a cluster whose shape was just read, and return once every node's stream is
/// open, or after 10 s.
pub(crate) async fn follow(changes: Arc<Changes>, rt: Arc<TenantRt>, first: Shape) -> Follower {
	let (tx, rx) = oneshot::channel();
	let task = tokio::spawn(run(changes, rt, first, tx));
	let _ = tokio::time::timeout(Duration::from_secs(10), rx).await;
	Follower {
		_task: Aborting(task),
	}
}

/// One node's stream, as the follower keeps it.
struct Node {
	member: Member,
	database: Database,
	pool: Pool,
	ready: Arc<watch::Sender<bool>>,
	prepared: bool,
	task: Option<Aborting>,
}

async fn listen(
	db: &Database,
) -> Result<(tokio_postgres::Client, mpsc::UnboundedReceiver<()>), tokio_postgres::Error> {
	let mut config = tokio_postgres::Config::new();
	config
		.host(&db.host)
		.port(db.port)
		.dbname(&db.name)
		.user(&db.user)
		.password(&db.password)
		.application_name("snout_realtime_cluster")
		.connect_timeout(Duration::from_secs(10));
	let (client, mut connection) = config.connect(tokio_postgres::NoTls).await?;
	let (tx, rx) = mpsc::unbounded_channel();
	tokio::spawn(async move {
		loop {
			match futures_util::future::poll_fn(|cx| connection.poll_message(cx)).await {
				Some(Ok(tokio_postgres::AsyncMessage::Notification(_))) => {
					if tx.send(()).is_err() {
						break;
					}
				}
				Some(Ok(_)) => {}
				Some(Err(_)) | None => break,
			}
		}
	});
	client.batch_execute("listen lepis_epoch").await?;
	Ok((client, rx))
}

async fn run(changes: Arc<Changes>, rt: Arc<TenantRt>, first: Shape, ready: oneshot::Sender<()>) {
	let home = changes.database().clone();
	let mut ready = Some(ready);
	let mut first = Some(first);
	let mut nodes: HashMap<i32, Node> = HashMap::new();
	let mut warned: HashSet<String> = HashSet::new();
	let mut acks = Acks::new();
	let mut backoff = Duration::from_secs(1);
	loop {
		let (client, mut notes) = match listen(&home).await {
			Ok(c) => {
				backoff = Duration::from_secs(1);
				c
			}
			Err(e) => {
				tracing::warn!(tenant = %rt.id, error = %e, "following the lepis catalog");
				// Nobody waits on a follower that cannot reach home: the home stream itself will
				// fail and start again.
				if let Some(tx) = ready.take() {
					let _ = tx.send(());
				}
				tokio::time::sleep(backoff).await;
				backoff = (backoff * 2).min(POLL);
				continue;
			}
		};
		loop {
			let shape = match first.take() {
				Some(s) => s,
				None => match read(&client).await {
					Ok(s) => s,
					Err(e) => {
						tracing::warn!(tenant = %rt.id, error = %e, "reading the lepis catalog");
						break;
					}
				},
			};
			let shape = Arc::new(shape);
			changes.set_shape(Some(shape.clone()));
			reconcile(
				&changes,
				&rt,
				&home,
				&client,
				&shape,
				&mut nodes,
				&mut warned,
			)
			.await;
			let limit = if ready.is_some() {
				Duration::from_secs(10)
			} else {
				Duration::from_secs(2)
			};
			wait_ready(&nodes, limit).await;
			if let Some(tx) = ready.take() {
				let _ = tx.send(());
			}
			acks.ack(&client, shape.epoch, &rt.id).await;
			let since = Instant::now();
			let lost = loop {
				tokio::select! {
					n = notes.recv() => break n.is_none(),
					_ = tokio::time::sleep(HEARTBEAT) => {
						acks.ack(&client, shape.epoch, &rt.id).await;
						if since.elapsed() >= POLL {
							break false;
						}
					}
				}
			};
			if lost {
				break;
			}
			while notes.try_recv().is_ok() {}
		}
		tokio::time::sleep(backoff).await;
	}
}

/// Make the running node streams the shape's: a stream for each node but home, the realtime schema
/// and the publication on it first, and none for a node that is gone or moved.
async fn reconcile(
	changes: &Arc<Changes>,
	rt: &Arc<TenantRt>,
	home: &Database,
	home_client: &tokio_postgres::Client,
	shape: &Shape,
	nodes: &mut HashMap<i32, Node>,
	warned: &mut HashSet<String>,
) {
	nodes.retain(|id, n| {
		shape
			.members
			.iter()
			.any(|m| m.id == *id && !m.home && *m == n.member)
	});
	for m in shape.members.iter().filter(|m| !m.home) {
		if let std::collections::hash_map::Entry::Vacant(slot) = nodes.entry(m.id) {
			let database = m.database(home);
			let pool = match db::new_pool(&database, 2) {
				Ok(p) => p,
				Err(e) => {
					tracing::warn!(tenant = %rt.id, node = %m.name, error = %e, "a node's pool");
					continue;
				}
			};
			slot.insert(Node {
				member: m.clone(),
				database,
				pool,
				ready: Arc::new(watch::channel(false).0),
				prepared: false,
				task: None,
			});
		}
		let Some(node) = nodes.get_mut(&m.id) else {
			continue;
		};
		if !node.prepared {
			match db::prepare(&node.pool).await {
				Ok(()) => node.prepared = true,
				Err(e) => {
					// A standby being made into a node (a physical split) refuses until promoted.
					if e.code() != "25006" && warned.insert(format!("{}: {e}", m.name)) {
						tracing::warn!(tenant = %rt.id, node = %m.name, error = %e, "preparing a node for database changes");
					}
					continue;
				}
			}
		}
		match node.pool.get().await {
			Ok(client) => {
				match mirror_publication(home_client, &client, &home.publication, shape).await {
					Ok(done) => {
						for sql in done {
							tracing::info!(tenant = %rt.id, node = %m.name, statement = %sql, "a node's publication made to match home's");
						}
					}
					Err(e) => {
						if warned.insert(format!("{}: {e}", m.name)) {
							tracing::warn!(tenant = %rt.id, node = %m.name, error = %e, "a node's publication does not match home's, and this role cannot change it; run the statement on the node");
						}
					}
				}
			}
			Err(e) => {
				if warned.insert(format!("{}: {e}", m.name)) {
					tracing::warn!(tenant = %rt.id, node = %m.name, error = %e, "a node cannot be reached");
				}
				continue;
			}
		}
		if node.task.as_ref().is_none_or(|t| t.0.is_finished()) {
			node.task = Some(Aborting(tokio::spawn(run_node(
				changes.clone(),
				rt.clone(),
				m.clone(),
				node.database.clone(),
				node.pool.clone(),
				node.ready.clone(),
			))));
		}
	}
}

/// Wait, at most `limit` in all, until every started node stream is open.
async fn wait_ready(nodes: &HashMap<i32, Node>, limit: Duration) {
	let deadline = tokio::time::Instant::now() + limit;
	for node in nodes.values().filter(|n| n.task.is_some()) {
		let mut rx = node.ready.subscribe();
		let _ = tokio::time::timeout_at(deadline, rx.wait_for(|open| *open)).await;
	}
}

/// One node's stream, opened again after a failure: one second later, doubling to thirty while it
/// keeps failing.
async fn run_node(
	changes: Arc<Changes>,
	rt: Arc<TenantRt>,
	member: Member,
	database: Database,
	pool: Pool,
	ready: Arc<watch::Sender<bool>>,
) {
	let mut backoff = Duration::from_secs(1);
	loop {
		let opened = Instant::now();
		match changes
			.stream_node(&rt, member.id, &database, &pool, &ready)
			.await
		{
			Ok(()) => break,
			Err(e) => {
				tracing::warn!(tenant = %rt.id, node = %member.name, error = %e, "a node's database changes")
			}
		}
		ready.send_replace(false);
		if changes.is_retired() {
			break;
		}
		if opened.elapsed() > Duration::from_secs(60) {
			backoff = Duration::from_secs(1);
		}
		tokio::time::sleep(backoff).await;
		backoff = (backoff * 2).min(POLL);
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn member(id: i32, home: bool) -> Member {
		Member {
			id,
			name: format!("n{id}"),
			host: format!("node{id}"),
			port: 5432,
			dbname: "app".into(),
			home,
		}
	}

	#[test]
	fn a_node_is_reached_with_the_tenants_login() {
		let home = Database {
			host: "snoutpod-x".into(),
			port: 5432,
			name: "x".into(),
			user: "snout_realtime_admin".into(),
			password: "p".into(),
			publication: "snoutdata_realtime".into(),
			slot_name: "s".into(),
			poll_interval_ms: 100,
			poll_max_record_bytes: 1024,
			poll_max_changes: 100,
		};
		let d = member(2, false).database(&home);
		assert_eq!((d.host.as_str(), d.name.as_str()), ("node2", "app"));
		assert_eq!(d.user, home.user);
		assert_eq!(d.publication, home.publication);
	}

	#[test]
	fn only_the_sharded_tables_are_sharded() {
		let s = Shape::new(
			3,
			vec![member(1, true), member(2, false)],
			&[("app", "orders")],
		);
		assert!(s.is_sharded("app", "orders"));
		assert!(!s.is_sharded("app", "countries"));
		assert!(!s.is_sharded("public", "orders"));
	}

	#[test]
	fn the_catalog_row_parses() {
		let members: Vec<Member> = serde_json::from_value(serde_json::json!([
			{"id": 1, "name": "home", "host": "snoutpod-x", "port": 5432, "dbname": "x", "home": true},
			{"id": 2, "name": "n2", "host": "snoutpod-x-n2", "port": 5432, "dbname": "x", "home": false}
		]))
		.unwrap();
		assert_eq!(members.len(), 2);
		assert!(members[0].home && !members[1].home);
	}
}
