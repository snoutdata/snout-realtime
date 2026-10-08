//! Database changes on a SHARDED project (snout-lepis, `src/cluster.rs`), against three real
//! Postgres servers: one stream per node, merged into the same subscribers, with each change
//! decided by the row-level security of the node it came from.
//!
//! Skips without REALTIME_TEST_CLUSTER (`home:port,node2:port,node3:port`); `bash
//! tests/cluster.sh` starts the three and runs it. Lepis itself is not here: its catalog is written
//! by hand with the columns Realtime reads (`lepis/src/catalog.sql` is the source), and a row is
//! written straight to the node that owns it, as the router would. Ownership is `tenant % 3`: the
//! fence each node carries, as Lepis's `lepis_owns` CHECK.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use snout_realtime::changes::{self, Binding, Changes};
use snout_realtime::hub::{self, Hub, Inbox, Out};
use snout_realtime::tenants::{Database, Tenant};
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

const DB: &str = "realtime_cluster";
const U1: &str = "00000000-0000-4000-8000-000000000001";
const U2: &str = "00000000-0000-4000-8000-000000000002";

/// The columns of the Lepis catalog Realtime reads, and the notification a catalog change sends.
const CATALOG: &str = "
create schema lepis;
create table lepis.cluster (id int primary key default 1 check (id = 1), epoch bigint not null default 1);
insert into lepis.cluster (id) values (1);
create table lepis.node (id int primary key, name text not null unique, host text not null,
	port int not null, dbname text not null, sslmode text not null default 'disable',
	kind text not null, state text not null default 'joining', labels jsonb not null default '{}');
create table lepis.relation (schema_name text not null, table_name text not null, kind text not null,
	keyspace text, key_column text, primary key (schema_name, table_name));
create table lepis.router (id text primary key, epoch bigint not null, seen_at timestamptz not null default now());
create function lepis.notify_epoch() returns trigger language plpgsql as $$
begin
	perform pg_notify('lepis_epoch', (select epoch::text from lepis.cluster where id = 1));
	return null;
end $$;
create trigger epoch_moved after update of epoch on lepis.cluster
	for each statement execute function lepis.notify_epoch();
insert into lepis.relation values ('public', 'orders', 'sharded', 'tenants', 'tenant'),
	('public', 'countries', 'reference', null, null), ('public', 'notes', 'global', null, null);
";

/// What every node has: the roles and `auth` functions a policy reads (Lepis's role sync and DDL
/// fan-out put them on every node), the sharded table with its fence, and the reference table.
fn node_sql(owns: i32) -> String {
	format!(
		"do $$ begin
		   if to_regrole('anon') is null then create role anon nologin noinherit; end if;
		   if to_regrole('authenticated') is null then create role authenticated nologin noinherit; end if;
		   if to_regrole('service_role') is null then create role service_role nologin noinherit bypassrls; end if;
		 end $$;
		 create schema auth;
		 create function auth.uid() returns uuid language sql stable as
		   $$ select nullif(current_setting('request.jwt.claims', true)::jsonb ->> 'sub', '')::uuid $$;
		 grant usage on schema auth, public to anon, authenticated;
		 create table public.orders (tenant int not null, id bigint not null, owner uuid, note text,
		   primary key (tenant, id));
		 alter table public.orders enable row level security;
		 create policy mine on public.orders for select to authenticated using (owner = auth.uid());
		 grant select on public.orders to authenticated;
		 alter table public.orders add constraint lepis_owns check (tenant % 3 = {owns}) not valid;
		 create table public.countries (code text primary key, name text);
		 grant select on public.countries to authenticated;"
	)
}

struct Bed {
	nodes: Vec<(String, u16)>,
	password: String,
}

fn bed() -> Option<Bed> {
	let list = std::env::var("REALTIME_TEST_CLUSTER").ok()?;
	let nodes = list
		.split(',')
		.map(|n| {
			let (h, p) = n.rsplit_once(':').expect("host:port");
			(h.to_string(), p.parse().expect("port"))
		})
		.collect::<Vec<_>>();
	assert_eq!(nodes.len(), 3, "three nodes: home first");
	Some(Bed {
		nodes,
		password: std::env::var("REALTIME_TEST_PASSWORD").unwrap_or_else(|_| "test".into()),
	})
}

async fn connect(bed: &Bed, node: usize, db: &str) -> Client {
	let (host, port) = &bed.nodes[node];
	let (client, conn) = tokio_postgres::Config::new()
		.host(host)
		.port(*port)
		.user("postgres")
		.password(&bed.password)
		.dbname(db)
		.connect(NoTls)
		.await
		.unwrap_or_else(|e| panic!("{host}:{port}/{db}: {e}"));
	tokio::spawn(conn);
	client
}

fn binding(table: &str, event: &str) -> Binding {
	let params = json!({ "event": event, "schema": "public", "table": table });
	let (action, schema, table, filters, selected) =
		changes::parse(params.as_object().unwrap()).unwrap();
	Binding {
		subscription_id: Uuid::new_v4(),
		id: changes::binding_id(&params),
		action,
		schema,
		table,
		filters,
		selected,
	}
}

fn claims(sub: &str) -> serde_json::Map<String, Value> {
	json!({ "role": "authenticated", "sub": sub })
		.as_object()
		.unwrap()
		.clone()
}

/// The next change a channel is sent, as `{type, table, record, old_record}`.
async fn next(rx: &mut Inbox) -> Option<Value> {
	loop {
		match tokio::time::timeout(Duration::from_secs(10), rx.recv()).await {
			Ok(Some(Out::Changes { data, .. })) => return serde_json::from_str(&data).ok(),
			Ok(Some(_)) => continue,
			_ => return None,
		}
	}
}

/// A value of a record as text, however the payload typed it.
fn field(change: &Value, part: &str, column: &str) -> String {
	match &change[part][column] {
		Value::String(s) => s.clone(),
		other => other.to_string(),
	}
}

/// Nothing more for this channel within a moment.
async fn quiet(rx: &mut Inbox) -> bool {
	loop {
		match tokio::time::timeout(Duration::from_millis(1500), rx.recv()).await {
			Ok(Some(Out::Changes { .. })) => return false,
			Ok(Some(_)) => continue,
			_ => return true,
		}
	}
}

async fn rows(c: &Client) -> i64 {
	c.query_one("select count(*) from realtime.subscription", &[])
		.await
		.unwrap()
		.get(0)
}

#[tokio::test]
async fn a_sharded_projects_changes_come_from_every_node() {
	let Some(bed) = bed() else {
		eprintln!("skipped: REALTIME_TEST_CLUSTER is not set (tests/cluster.sh sets it)");
		return;
	};
	// A fresh database on every node, named the same, as Lepis's nodes are.
	for n in 0..3 {
		let admin = connect(&bed, n, "postgres").await;
		admin
			.batch_execute(&format!("drop database if exists {DB} with (force)"))
			.await
			.unwrap();
		admin
			.batch_execute(&format!("create database {DB}"))
			.await
			.unwrap();
	}
	let home = connect(&bed, 0, DB).await;
	let n2 = connect(&bed, 1, DB).await;
	let n3 = connect(&bed, 2, DB).await;
	for (n, c) in [&home, &n2, &n3].into_iter().enumerate() {
		c.batch_execute(&node_sql(n as i32)).await.unwrap();
	}
	home.batch_execute(
		"create table public.notes (id int primary key, body text);
		 grant select on public.notes to authenticated;
		 create publication snoutdata_realtime for table public.orders, public.countries, public.notes;",
	)
	.await
	.unwrap();
	home.batch_execute(CATALOG).await.unwrap();
	// Home and node 2 to start with; node 3 joins while the stream is open.
	home.execute(
		"insert into lepis.node (id, name, host, port, dbname, kind, state) values
		 (1, 'home', '127.0.0.1', 5432, $1, 'home', 'active'), (2, 'n2', $2, $3, $1, 'data', 'active')",
		&[&DB, &bed.nodes[1].0, &(bed.nodes[1].1 as i32)],
	)
	.await
	.unwrap();

	let database = Database {
		host: bed.nodes[0].0.clone(),
		port: bed.nodes[0].1,
		name: DB.into(),
		user: "postgres".into(),
		password: bed.password.clone(),
		publication: "snoutdata_realtime".into(),
		slot_name: "unused".into(),
		poll_interval_ms: 100,
		poll_max_record_bytes: 1_048_576,
		poll_max_changes: 100,
	};
	let tenant = Tenant {
		external_id: "cluster".into(),
		name: "cluster".into(),
		jwt_secret: "a-test-secret-of-reasonable-length".into(),
		max_concurrent_users: 100,
		max_channels_per_client: 100,
		max_events_per_second: 1000,
		max_joins_per_second: 100,
		max_presence_events_per_second: 100,
		max_payload_size_in_kb: 3000,
		private_only: false,
		presence_enabled: false,
		database: Some(database.clone()),
		postgres_changes_refusal: None,
	};
	let hub = Hub::default();
	let rt = hub.tenant(Arc::new(tenant));
	let pool = snout_realtime::db::new_pool(&database, 4).unwrap();
	snout_realtime::db::prepare(&pool).await.unwrap();
	let changes = Changes::new(database.clone());

	// U1 and U2 on the sharded table, U1 on the reference and the global tables.
	let (tx1, mut rx1) = hub::outbox();
	let (tx2, mut rx2) = hub::outbox();
	let (tx3, mut rx3) = hub::outbox();
	let orders1 = vec![binding("orders", "*")];
	let orders2 = vec![binding("orders", "INSERT")];
	let others = vec![binding("countries", "*"), binding("notes", "*")];
	changes
		.subscribe(rt.clone(), &pool, &claims(U1), &orders1, tx1, "realtime:o1")
		.await
		.unwrap();
	changes
		.subscribe(rt.clone(), &pool, &claims(U2), &orders2, tx2, "realtime:o2")
		.await
		.unwrap();
	changes
		.subscribe(rt.clone(), &pool, &claims(U1), &others, tx3, "realtime:x")
		.await
		.unwrap();
	assert_eq!(changes.shape().map(|s| s.members.len()), Some(2));
	// Node 2 had no publication: it was given home's sharded tables.
	let published: Vec<String> = n2
		.query(
			"select tablename::text from pg_publication_tables where pubname = 'snoutdata_realtime'",
			&[],
		)
		.await
		.unwrap()
		.iter()
		.map(|r| r.get(0))
		.collect();
	assert_eq!(published, vec!["orders".to_string()]);
	// The two bindings on the sharded table are rows on node 2; the others match nothing there.
	assert_eq!(rows(&n2).await, 2);

	// A row on home and a row on node 2: each reaches U1, once, and never U2 (not theirs).
	home.execute(
		"insert into orders values (0, 1, $1, 'home')",
		&[&Uuid::parse_str(U1).unwrap()],
	)
	.await
	.unwrap();
	n2.execute(
		"insert into orders values (1, 2, $1, 'node 2')",
		&[&Uuid::parse_str(U1).unwrap()],
	)
	.await
	.unwrap();
	let mut seen = Vec::new();
	for _ in 0..2 {
		let c = next(&mut rx1).await.expect("a change from each node");
		assert_eq!(c["type"], "INSERT");
		seen.push(field(&c, "record", "note"));
	}
	seen.sort();
	assert_eq!(seen, vec!["home", "node 2"]);
	assert!(quiet(&mut rx1).await, "each change once");
	assert!(quiet(&mut rx2).await, "row-level security on each node");

	// A reference table is written on every node at once; it is reported once, from home.
	for c in [&home, &n2, &n3] {
		c.batch_execute("insert into countries values ('FR', 'France')")
			.await
			.unwrap();
	}
	home.batch_execute("insert into notes values (1, 'global')")
		.await
		.unwrap();
	let a = next(&mut rx3).await.expect("the reference row");
	let b = next(&mut rx3).await.expect("the global row");
	let mut tables = vec![a["table"].clone(), b["table"].clone()];
	tables.sort_by_key(|t| t.to_string());
	assert_eq!(tables, vec![json!("countries"), json!("notes")]);
	assert!(
		quiet(&mut rx3).await,
		"a reference row once, not once per node"
	);

	// Node 3 joins: the stream follows the catalog, opens node 3 and only then acknowledges the
	// epoch, which is what a cutover onto a new node waits for.
	home.execute(
		"insert into lepis.node (id, name, host, port, dbname, kind, state) values (3, 'n3', $1, $2, $3, 'data', 'active')",
		&[&bed.nodes[2].0, &(bed.nodes[2].1 as i32), &DB],
	)
	.await
	.unwrap();
	let epoch: i64 = home
		.query_one(
			"update lepis.cluster set epoch = epoch + 1 returning epoch",
			&[],
		)
		.await
		.unwrap()
		.get(0);
	let mut acked = false;
	for _ in 0..150 {
		let row = home
			.query_opt(
				"select epoch from lepis.router where id like 'snout-realtime/%'",
				&[],
			)
			.await
			.unwrap();
		if row.is_some_and(|r| r.get::<_, i64>(0) >= epoch) {
			acked = true;
			break;
		}
		tokio::time::sleep(Duration::from_millis(100)).await;
	}
	assert!(acked, "the new epoch is acknowledged once node 3 streams");
	assert_eq!(
		rows(&n3).await,
		2,
		"the bindings written on the node that joined"
	);
	n3.execute(
		"insert into orders values (2, 3, $1, 'node 3')",
		&[&Uuid::parse_str(U1).unwrap()],
	)
	.await
	.unwrap();
	let c = next(&mut rx1).await.expect("a change from the new node");
	assert_eq!(field(&c, "record", "note"), "node 3");

	// Rows Lepis copies onto a node arrive by logical replication, with an origin: nobody wrote
	// them. The row after them, written plainly, is the next change.
	// One transaction each: the origin a commit carries is the session's at that commit.
	for sql in [
		"select pg_replication_origin_create('lepis_test_copy')",
		"select pg_replication_origin_session_setup('lepis_test_copy')",
		"insert into orders values (7, 7, '00000000-0000-4000-8000-000000000001', 'copied')",
		"select pg_replication_origin_session_reset()",
		"insert into orders values (4, 4, '00000000-0000-4000-8000-000000000001', 'written')",
	] {
		n2.batch_execute(sql).await.unwrap();
	}
	let c = next(&mut rx1).await.expect("the plain write");
	assert_eq!(field(&c, "record", "note"), "written");

	// Node 2 gives tenant 7 away (its fence no longer covers it) and the cleanup deletes the row:
	// it lives on elsewhere and nobody deleted it. A real delete after it is sent.
	n2.batch_execute(
		"alter table orders drop constraint lepis_owns,
		   add constraint lepis_owns check (tenant % 3 = 1 and tenant <> 7) not valid;
		 delete from orders where tenant = 7;
		 delete from orders where tenant = 4;",
	)
	.await
	.unwrap();
	let c = next(&mut rx1).await.expect("the real delete");
	assert_eq!(c["type"], "DELETE");
	assert_eq!(field(&c, "old_record", "tenant"), "4");
	assert!(
		quiet(&mut rx1).await,
		"the cleanup is not a delete anybody made"
	);

	// Leaving takes the rows off every node.
	let ids: Vec<Uuid> = orders1.iter().map(|b| b.subscription_id).collect();
	changes.unsubscribe(&pool, &ids).await;
	assert_eq!(rows(&n2).await, 1);
	assert_eq!(rows(&n3).await, 1);
	assert_eq!(rows(&home).await, 3);
	changes.retire();
}

/// The same server with no `lepis` schema: one stream, no shape, nothing written anywhere else.
#[tokio::test]
async fn an_unsharded_project_is_streamed_as_before() {
	let Some(bed) = bed() else {
		eprintln!("skipped: REALTIME_TEST_CLUSTER is not set (tests/cluster.sh sets it)");
		return;
	};
	let name = "realtime_single";
	let admin = connect(&bed, 0, "postgres").await;
	admin
		.batch_execute(&format!("drop database if exists {name} with (force)"))
		.await
		.unwrap();
	admin
		.batch_execute(&format!("create database {name}"))
		.await
		.unwrap();
	let db = connect(&bed, 0, name).await;
	db.batch_execute(&node_sql(0)).await.unwrap();
	db.batch_execute("create publication snoutdata_realtime for table public.orders")
		.await
		.unwrap();
	let database = Database {
		host: bed.nodes[0].0.clone(),
		port: bed.nodes[0].1,
		name: name.into(),
		user: "postgres".into(),
		password: bed.password.clone(),
		publication: "snoutdata_realtime".into(),
		slot_name: "unused".into(),
		poll_interval_ms: 100,
		poll_max_record_bytes: 1_048_576,
		poll_max_changes: 100,
	};
	let hub = Hub::default();
	let rt = hub.tenant(Arc::new(Tenant {
		external_id: "single".into(),
		name: "single".into(),
		jwt_secret: "a-test-secret-of-reasonable-length".into(),
		max_concurrent_users: 100,
		max_channels_per_client: 100,
		max_events_per_second: 1000,
		max_joins_per_second: 100,
		max_presence_events_per_second: 100,
		max_payload_size_in_kb: 3000,
		private_only: false,
		presence_enabled: false,
		database: Some(database.clone()),
		postgres_changes_refusal: None,
	}));
	let pool = snout_realtime::db::new_pool(&database, 4).unwrap();
	snout_realtime::db::prepare(&pool).await.unwrap();
	let changes = Changes::new(database);
	let (tx, mut rx) = hub::outbox();
	changes
		.subscribe(
			rt,
			&pool,
			&claims(U1),
			&[binding("orders", "*")],
			tx,
			"realtime:o",
		)
		.await
		.unwrap();
	assert!(changes.shape().is_none());
	// A row its fence would refuse is still a row here: nothing of Lepis applies.
	db.execute(
		"insert into orders values (0, 1, $1, 'one')",
		&[&Uuid::parse_str(U1).unwrap()],
	)
	.await
	.unwrap();
	let c = next(&mut rx).await.expect("the change");
	assert_eq!(field(&c, "record", "note"), "one");
	db.batch_execute("delete from orders").await.unwrap();
	assert_eq!(next(&mut rx).await.expect("the delete")["type"], "DELETE");
	changes.retire();
}
