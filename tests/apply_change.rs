//! snout_realtime::changes::decide against a real Postgres: who may see a change, and what of it.
//! Skips without REALTIME_TEST_DATABASE_URL; `bash tests/db.sh` runs it with one.

use tokio_postgres::{Client, NoTls};

const SCHEMA: &str = include_str!("../migrations/0001_realtime.sql");
const CHANGES: &str = include_str!("../migrations/0002_changes.sql");

const U1: &str = "00000000-0000-4000-8000-000000000001";
const U2: &str = "00000000-0000-4000-8000-000000000002";

async fn database() -> Option<Client> {
	let admin_url = std::env::var("REALTIME_TEST_DATABASE_URL").ok()?;
	let (admin, connection) = tokio_postgres::connect(&admin_url, NoTls).await.unwrap();
	tokio::spawn(connection);
	let name = format!("realtime_apply_{}", std::process::id());
	// One statement each: neither may run inside the implicit transaction of a batch.
	for statement in [
		format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"),
		format!("CREATE DATABASE {name}"),
	] {
		admin.batch_execute(&statement).await.unwrap();
	}
	admin
		.batch_execute(
			"DO $$ BEGIN
			   IF to_regrole('anon') IS NULL THEN CREATE ROLE anon NOLOGIN NOINHERIT; END IF;
			   IF to_regrole('authenticated') IS NULL THEN CREATE ROLE authenticated NOLOGIN NOINHERIT; END IF;
			   IF to_regrole('service_role') IS NULL THEN CREATE ROLE service_role NOLOGIN NOINHERIT BYPASSRLS; END IF;
			 END $$;",
		)
		.await
		.unwrap();
	let url = match admin_url.rsplit_once('/') {
		Some((base, _)) => format!("{base}/{name}"),
		None => unreachable!(),
	};
	let (client, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
	tokio::spawn(connection);
	// The second connection the checks are shared with, as the stream opens one for many users.
	let (second, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
	tokio::spawn(connection);
	SECOND.with(|s| *s.borrow_mut() = Some(second));
	client
		.batch_execute(
			"create schema auth;
			 create function auth.uid() returns uuid language sql stable as
			   $$ select nullif(current_setting('request.jwt.claims', true)::jsonb ->> 'sub', '')::uuid $$;
			 create function auth.jwt() returns jsonb language sql stable as
			   $$ select current_setting('request.jwt.claims', true)::jsonb $$;
			 grant usage on schema auth, public to anon, authenticated;",
		)
		.await
		.unwrap();
	client.batch_execute(SCHEMA).await.unwrap();
	client.batch_execute(CHANGES).await.unwrap();
	client
		.batch_execute(
			"create table public.items (id bigint generated always as identity primary key, owner uuid, room text, title text);
			 alter table public.items enable row level security;
			 create policy r on public.items for select to authenticated using (owner = auth.uid() or room = 'public');
			 grant select on public.items to anon, authenticated;
			 create table public.nano (id char(21) primary key, v text);
			 grant select on public.nano to authenticated;
			 create table public.secret (id bigint generated always as identity primary key, visible text, hidden text);
			 grant select (id, visible) on public.secret to authenticated;
			 create table public.poison (id bigint generated always as identity primary key, v text);
			 alter table public.poison enable row level security;
			 create policy p on public.poison for select to authenticated
			   using (nullif(auth.jwt() -> 'app_metadata' ->> 'org', '')::uuid is not null);
			 grant select on public.poison to authenticated;
			 insert into public.items (owner, room, title) values ('00000000-0000-4000-8000-000000000001', 'crud', 'first');
			 insert into public.nano values ('abc', 'x');
			 insert into public.secret (visible, hidden) values ('seen', 'HIDDEN');
			 insert into public.poison (v) values ('p');
			 create table public.typed (id bigint primary key, doc json, bin bytea, tags text[], at timestamptz);
			 grant select on public.typed to authenticated;",
		)
		.await
		.unwrap();
	Some(client)
}

async fn subscribe(
	db: &Client,
	id: &str,
	table: &str,
	filter: Option<(&str, &str, &str)>,
	claims: serde_json::Value,
	action: &str,
) {
	let filters = match filter {
		Some((c, o, v)) => format!("array[row('{c}','{o}','{v}')::realtime.user_defined_filter]"),
		None => "'{}'".into(),
	};
	db.execute(
		&format!(
			"insert into realtime.subscription (subscription_id, entity, filters, claims, action_filter)
			 values ($1::text::uuid, '{table}'::regclass, {filters}, $2, $3)"
		),
		&[&id, &claims, &action],
	)
	.await
	.unwrap();
}

thread_local! {
	static STATEMENTS: std::cell::RefCell<Vec<snout_realtime::changes::Statements>> = std::cell::RefCell::new(vec![Default::default(), Default::default()]);
	static SECOND: std::cell::RefCell<Option<Client>> = Default::default();
}

/// A batch of changes to one table: for each group, (change's position, subscription ids
/// sorted, payload, errors). Two connections for the whole test, as the stream keeps them, so the
/// prepared row checks are reused across calls, tables and roles, and a role's users are shared
/// between the two.
async fn batch(
	db: &Client,
	table: &str,
	action: &str,
	changes: &[(Option<serde_json::Value>, Option<serde_json::Value>)],
) -> Vec<(usize, Vec<String>, serde_json::Value, Vec<String>)> {
	let oid: u32 = db
		.query_one("select $1::text::regclass::oid", &[&table])
		.await
		.unwrap()
		.get(0);
	// A transaction that has committed, as a change carries its own, so the checks wait for it.
	let xid: i64 = db
		.query_one(
			"select pg_current_xact_id()::text::bigint % 4294967296",
			&[],
		)
		.await
		.unwrap()
		.get(0);
	let changes: Vec<snout_realtime::changes::Change> = changes
		.iter()
		.map(|(new, old)| snout_realtime::changes::Change {
			commit_seconds: 1_790_000_000.0,
			xid: xid as u32,
			new: new.clone(),
			old: old.clone(),
		})
		.collect();
	let mut statements = STATEMENTS.with(|s| std::mem::take(&mut *s.borrow_mut()));
	let second = SECOND.with(|s| s.borrow_mut().take()).unwrap();
	let groups = snout_realtime::changes::decide(
		&[db, &second],
		&mut statements,
		oid,
		action,
		&changes,
		1_048_576,
	)
	.await
	.unwrap();
	SECOND.with(|s| *s.borrow_mut() = Some(second));
	STATEMENTS.with(|s| *s.borrow_mut() = statements);
	let mut out: Vec<_> = groups
		.into_iter()
		.map(|(i, g)| {
			let mut ids = g.ids;
			ids.sort();
			(i, ids, g.payload, g.errors)
		})
		.collect();
	out.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));
	out
}

/// One change: each group (subscription ids, sorted; payload; errors).
async fn apply(
	db: &Client,
	table: &str,
	action: &str,
	new: Option<serde_json::Value>,
	old: Option<serde_json::Value>,
) -> Vec<(Vec<String>, serde_json::Value, Vec<String>)> {
	batch(db, table, action, &[(new, old)])
		.await
		.into_iter()
		.map(|(_, ids, payload, errors)| (ids, payload, errors))
		.collect()
}

fn user(sub: &str) -> serde_json::Value {
	serde_json::json!({ "role": "authenticated", "sub": sub })
}

#[tokio::test]
async fn who_sees_a_change_and_what_of_it() {
	let Some(db) = database().await else {
		eprintln!("skipped: REALTIME_TEST_DATABASE_URL is not set (tests/db.sh sets it)");
		return;
	};
	let a1 = "00000000-0000-4000-8000-0000000000a1";
	let a2 = "00000000-0000-4000-8000-0000000000a2";
	let a3 = "00000000-0000-4000-8000-0000000000a3";
	let a4 = "00000000-0000-4000-8000-0000000000a4";
	subscribe(&db, a1, "public.items", None, user(U1), "*").await;
	subscribe(&db, a2, "public.items", None, user(U2), "INSERT").await;
	subscribe(
		&db,
		a3,
		"public.items",
		Some(("room", "eq", "crud")),
		user(U1),
		"*",
	)
	.await;
	subscribe(
		&db,
		a4,
		"public.items",
		None,
		serde_json::json!({ "role": "anon" }),
		"*",
	)
	.await;

	// Row-level security per subscriber, filters as the column is typed.
	let row = serde_json::json!({ "id": "1", "owner": U1, "room": "crud", "title": "first" });
	let got = apply(&db, "public.items", "INSERT", Some(row.clone()), None).await;
	assert_eq!(got.len(), 1);
	assert_eq!(got[0].0, vec![a1.to_string(), a3.to_string()]);
	assert_eq!(got[0].1["record"]["id"], 1);
	assert_eq!(got[0].1["record"]["title"], "first");

	// An update carries the key as the old record.
	let got = apply(
		&db,
		"public.items",
		"UPDATE",
		Some(row),
		Some(serde_json::json!({ "id": "1" })),
	)
	.await;
	assert_eq!(got[0].0, vec![a1.to_string(), a3.to_string()]);
	assert_eq!(got[0].1["old_record"], serde_json::json!({ "id": 1 }));

	// A delete cannot be checked against a policy: every role subscribed hears of it, key only.
	let got = apply(
		&db,
		"public.items",
		"DELETE",
		None,
		Some(serde_json::json!({ "id": "1" })),
	)
	.await;
	let ids: Vec<&String> = got.iter().flat_map(|g| &g.0).collect();
	assert_eq!(ids.len(), 2, "{got:?}");
	assert!(
		got.iter()
			.all(|g| g.1["old_record"] == serde_json::json!({ "id": 1 }))
	);

	// char(21) keeps its padding.
	let b1 = "00000000-0000-4000-8000-0000000000b1";
	subscribe(
		&db,
		b1,
		"public.nano",
		Some(("id", "eq", "abc")),
		user(U1),
		"*",
	)
	.await;
	let got = apply(
		&db,
		"public.nano",
		"INSERT",
		Some(serde_json::json!({ "id": "abc                  ", "v": "x" })),
		None,
	)
	.await;
	assert_eq!(got[0].0, vec![b1.to_string()]);
	assert_eq!(got[0].1["record"]["id"], "abc                  ");

	// A column the role may not select never arrives, in the record or the columns.
	let c1 = "00000000-0000-4000-8000-0000000000c1";
	subscribe(&db, c1, "public.secret", None, user(U1), "*").await;
	let got = apply(
		&db,
		"public.secret",
		"INSERT",
		Some(serde_json::json!({ "id": "1", "visible": "seen", "hidden": "HIDDEN" })),
		None,
	)
	.await;
	assert_eq!(got[0].0, vec![c1.to_string()]);
	assert!(!got[0].1.to_string().contains("HIDDEN"));
	assert!(!got[0].1.to_string().contains("\"hidden\""));

	// A policy that raises for one subscriber costs that subscriber alone.
	let d1 = "00000000-0000-4000-8000-0000000000d1";
	let d2 = "00000000-0000-4000-8000-0000000000d2";
	subscribe(&db, d1, "public.poison", None, serde_json::json!({ "role": "authenticated", "sub": U1, "app_metadata": { "org": "not-a-uuid" } }), "*").await;
	subscribe(&db, d2, "public.poison", None, serde_json::json!({ "role": "authenticated", "sub": U2, "app_metadata": { "org": "11111111-1111-4111-8111-111111111111" } }), "*").await;
	let got = apply(
		&db,
		"public.poison",
		"INSERT",
		Some(serde_json::json!({ "id": "1", "v": "p" })),
		None,
	)
	.await;
	assert_eq!(got.len(), 1);
	assert_eq!(got[0].0, vec![d2.to_string()]);

	// Each value as a select would give it: json as JSON, bytea as the text Postgres prints,
	// an array as an array, a timestamp as to_jsonb writes one.
	let e1 = "00000000-0000-4000-8000-0000000000e1";
	subscribe(&db, e1, "public.typed", None, user(U1), "*").await;
	let got = apply(
		&db,
		"public.typed",
		"INSERT",
		Some(serde_json::json!({ "id": "1", "doc": "{\"a\": [1, 2]}", "bin": "\\x0102", "tags": "{x,\"y z\"}", "at": "2026-09-28 01:02:03+00" })),
		None,
	)
	.await;
	assert_eq!(got[0].0, vec![e1.to_string()]);
	let record = &got[0].1["record"];
	assert_eq!(record["doc"], serde_json::json!({ "a": [1, 2] }));
	assert_eq!(record["bin"], "\\x0102");
	assert_eq!(record["tags"], serde_json::json!(["x", "y z"]));
	assert_eq!(record["at"], "2026-09-28T01:02:03+00:00");

	// A batch: each change checked for each user once, whatever the number of changes, and each
	// change reaching only those who may see it. a1 (U1, everything) sees U1's rows and the public
	// one; a2 (U2, inserts) sees U2's and the public one; a3 filters for room `crud`.
	let rows = [
		serde_json::json!({ "id": "10", "owner": U1, "room": "crud", "title": "b1" }),
		serde_json::json!({ "id": "11", "owner": U2, "room": "private", "title": "b2" }),
		serde_json::json!({ "id": "12", "owner": U2, "room": "public", "title": "b3" }),
		serde_json::json!({ "owner": U1, "room": "crud", "title": "no key" }),
	];
	db.batch_execute(&format!(
		"insert into public.items (id, owner, room, title) overriding system value values
		 (10, '{U1}', 'crud', 'b1'), (11, '{U2}', 'private', 'b2'), (12, '{U2}', 'public', 'b3')"
	))
	.await
	.unwrap();
	let got = batch(
		&db,
		"public.items",
		"INSERT",
		&rows
			.iter()
			.map(|r| (Some(r.clone()), None))
			.collect::<Vec<_>>(),
	)
	.await;
	let seen = |i: usize| -> Vec<String> {
		let mut ids: Vec<String> = got
			.iter()
			.filter(|(at, _, _, errors)| *at == i && errors.is_empty())
			.flat_map(|(_, ids, _, _)| ids.clone())
			.collect();
		ids.sort();
		ids
	};
	assert_eq!(seen(0), vec![a1.to_string(), a3.to_string()]);
	assert_eq!(seen(1), vec![a2.to_string()]);
	assert_eq!(seen(2), vec![a1.to_string(), a2.to_string()]);
	assert!(
		got.iter()
			.any(|(at, _, _, errors)| *at == 3 && errors[0].contains("no primary key")),
		"{got:?}"
	);

	// The function leaves the session as it found it.
	let who: String = db
		.query_one("select current_user::text", &[])
		.await
		.unwrap()
		.get(0);
	assert_eq!(who, "postgres");
}
