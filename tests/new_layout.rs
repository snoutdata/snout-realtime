//! A project database in the new layout (the own layout) against a real Postgres: the
//! realtime schema made as `snout_live` with `signals`, `listeners` and `signal()`, grants to the
//! new role names, the daily partitions and authorisation in it, and the ledger holding the files'
//! own hashes. Skips without REALTIME_TEST_DATABASE_URL; `bash tests/db.sh` runs it with one.

use serde_json::Map;
use snout_realtime::db::{self, AuthContext, Policies};
use snout_realtime::tenants::Database;
use tokio_postgres::NoTls;

/// Roles are the cluster's: two tests creating them at once race, so one at a time.
static ROLES: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn sha256_hex(text: &str) -> String {
	use sha2::{Digest, Sha256};
	Sha256::digest(text.as_bytes())
		.iter()
		.map(|b| format!("{b:02x}"))
		.collect()
}

#[tokio::test]
async fn a_new_layout_database_is_made_and_spoken_to_in_its_names() {
	let Ok(admin_url) = std::env::var("REALTIME_TEST_DATABASE_URL") else {
		eprintln!("skipped: REALTIME_TEST_DATABASE_URL is not set (tests/db.sh sets it)");
		return;
	};
	let (admin, connection) = tokio_postgres::connect(&admin_url, NoTls).await.unwrap();
	tokio::spawn(connection);
	let name = format!("realtime_layout_{}", std::process::id());
	for statement in [
		format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"),
		format!("CREATE DATABASE {name}"),
	] {
		admin.batch_execute(&statement).await.unwrap();
	}
	// The new role names, as a database made in the new layout has them. No realtime schema of
	// either name yet: the roles are what say which layout to make.
	let one_at_a_time = ROLES.lock().await;
	admin
		.batch_execute(
			"DO $$ BEGIN
			   IF to_regrole('snout_app') IS NULL THEN CREATE ROLE snout_app NOLOGIN NOINHERIT; END IF;
			   IF to_regrole('snout_member') IS NULL THEN CREATE ROLE snout_member NOLOGIN NOINHERIT; END IF;
			   IF to_regrole('snout_server') IS NULL THEN CREATE ROLE snout_server NOLOGIN NOINHERIT BYPASSRLS; END IF;
			   -- The old names as the aliases a new-layout database keeps: each inherits its new role.
			   IF to_regrole('anon') IS NULL THEN CREATE ROLE anon NOLOGIN NOINHERIT; END IF;
			   IF to_regrole('authenticated') IS NULL THEN CREATE ROLE authenticated NOLOGIN NOINHERIT; END IF;
			   IF to_regrole('service_role') IS NULL THEN CREATE ROLE service_role NOLOGIN NOINHERIT BYPASSRLS; END IF;
			 END $$;
			 GRANT snout_app TO anon WITH INHERIT TRUE;
			 GRANT snout_member TO authenticated WITH INHERIT TRUE;
			 GRANT snout_server TO service_role WITH INHERIT TRUE;",
		)
		.await
		.unwrap();
	drop(one_at_a_time);
	let config: tokio_postgres::Config = admin_url.parse().unwrap();
	let host = match &config.get_hosts()[0] {
		tokio_postgres::config::Host::Tcp(h) => h.clone(),
		#[allow(unreachable_patterns)]
		_ => unreachable!(),
	};
	let database = Database {
		host,
		port: config.get_ports().first().copied().unwrap_or(5432),
		name: name.clone(),
		user: config.get_user().unwrap_or("postgres").to_string(),
		password: String::from_utf8_lossy(config.get_password().unwrap_or_default()).into_owned(),
		publication: "snoutdata_realtime".into(),
		slot_name: "unused".into(),
		poll_interval_ms: 100,
		poll_max_record_bytes: 1_048_576,
		poll_max_changes: 100,
	};
	let pool = db::new_pool(&database, 2).unwrap();
	db::prepare(&pool).await.unwrap();
	let names = pool.names().await.unwrap();
	assert!(names.is_new());
	assert!(names.has_new_roles);
	// Twice: the second finds everything made and the ledger current.
	db::prepare(&pool).await.unwrap();

	let client = pool.get().await.unwrap();
	let row = client
		.query_one(
			"select to_regclass('snout_live.signals') is not null, to_regclass('snout_live.listeners') is not null,
			   to_regclass('realtime.messages') is null, to_regnamespace('realtime') is null,
			   to_regprocedure('snout_live.signal(jsonb, text, text, boolean)') is not null,
			   to_regprocedure('snout_live.signal(bytea, text, text, boolean)') is not null,
			   has_table_privilege('snout_member', 'snout_live.signals', 'INSERT'),
			   has_table_privilege('snout_app', 'snout_live.listeners', 'SELECT'),
			   has_sequence_privilege('snout_server', 'snout_live.listeners_id_seq', 'USAGE'),
			   has_function_privilege('snout_server', 'snout_live.on(text, regprocedure, text)', 'EXECUTE'),
			   (select count(*) from pg_inherits where inhparent = 'snout_live.signals'::regclass),
			   to_regclass('snout_live.messages_' || to_char(current_date, 'YYYY_MM_DD')) is not null",
			&[],
		)
		.await
		.unwrap();
	for i in 0..10 {
		assert!(row.get::<_, bool>(i), "column {i}");
	}
	assert_eq!(row.get::<_, i64>(10), 5);
	assert!(row.get::<_, bool>(11));
	// The ledger holds the hashes of the files as written, in this layout as in the old.
	let ledger: Vec<(String, String)> = client
		.query(
			"select name, sha256 from snout_realtime.migrations order by name",
			&[],
		)
		.await
		.unwrap()
		.iter()
		.map(|r| (r.get(0), r.get(1)))
		.collect();
	assert_eq!(
		ledger,
		[
			(
				"changes".to_string(),
				sha256_hex(include_str!("../migrations/0002_changes.sql"))
			),
			(
				"handlers".to_string(),
				sha256_hex(include_str!("../migrations/0003_handlers.sql"))
			),
		]
	);
	// Broadcast from SQL lands in the new table, and the old partitions go.
	client
		.batch_execute(
			"select snout_live.signal('{\"a\": 1}'::jsonb, 'e', 'room', true);
			 create table snout_live.messages_2000_01_01 partition of snout_live.signals
			   for values from ('2000-01-01') to ('2000-01-02');",
		)
		.await
		.unwrap();
	let sent: i64 = client
		.query_one(
			"select count(*) from snout_live.signals where topic = 'room' and event = 'e'",
			&[],
		)
		.await
		.unwrap()
		.get(0);
	assert_eq!(sent, 1);
	db::drop_old_partitions(&pool).await.unwrap();
	let gone: bool = client
		.query_one(
			"select to_regclass('snout_live.messages_2000_01_01') is null",
			&[],
		)
		.await
		.unwrap()
		.get(0);
	assert!(gone);
	let stored = db::replay(&pool, "room", 0, 25).await.unwrap();
	assert_eq!(stored.len(), 1);
	// A claim in the old vocabulary is run as the role it names, the alias, as the data API runs
	// it; with no policy it may do nothing.
	let ctx = AuthContext {
		topic: "room".into(),
		role: "authenticated".into(),
		sub: None,
		claims: Map::new(),
		headers: Map::new(),
	};
	let read = db::authorize_read(&pool, &ctx, true, Policies::default())
		.await
		.unwrap();
	assert_eq!(read.broadcast_read, Some(false));
	let write = db::authorize_write(&pool, &ctx, "broadcast", Policies::default())
		.await
		.unwrap();
	assert_eq!(write.broadcast_write, Some(false));
	// A policy written to the alias, and one to the new name, each let the claim it names through.
	client
		.batch_execute(
			"create policy old_name on snout_live.signals for select to authenticated using (topic = 'old');
			 create policy new_name on snout_live.signals for select to snout_member using (topic = 'new');",
		)
		.await
		.unwrap();
	for (topic, role, seen) in [
		("old", "authenticated", true),
		("old", "snout_member", false),
		("new", "snout_member", true),
		// The alias inherits the new role, so a policy to the new name applies to it too.
		("new", "authenticated", true),
	] {
		let ctx = AuthContext {
			topic: topic.into(),
			role: role.into(),
			sub: None,
			claims: Map::new(),
			headers: Map::new(),
		};
		let read = db::authorize_read(&pool, &ctx, false, Policies::default())
			.await
			.unwrap();
		assert_eq!(read.broadcast_read, Some(seen), "{topic} as {role}");
	}
	drop(client);
	pool.close();
	admin
		.batch_execute(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
		.await
		.unwrap();
}

/// A database an earlier release migrated (the same statements, in the old layout's names, and the
/// hashes of the files it carried), in either layout: nothing is run again and the ledger is left
/// as it was.
#[tokio::test]
async fn a_database_migrated_by_an_earlier_release_is_left_alone() {
	let Ok(admin_url) = std::env::var("REALTIME_TEST_DATABASE_URL") else {
		eprintln!("skipped: REALTIME_TEST_DATABASE_URL is not set (tests/db.sh sets it)");
		return;
	};
	let (admin, connection) = tokio_postgres::connect(&admin_url, NoTls).await.unwrap();
	tokio::spawn(connection);
	let one_at_a_time = ROLES.lock().await;
	admin
		.batch_execute(
			"DO $$ BEGIN
			   IF to_regrole('snout_app') IS NULL THEN CREATE ROLE snout_app NOLOGIN NOINHERIT; END IF;
			   IF to_regrole('snout_member') IS NULL THEN CREATE ROLE snout_member NOLOGIN NOINHERIT; END IF;
			   IF to_regrole('snout_server') IS NULL THEN CREATE ROLE snout_server NOLOGIN NOINHERIT BYPASSRLS; END IF;
			   IF to_regrole('anon') IS NULL THEN CREATE ROLE anon NOLOGIN NOINHERIT; END IF;
			   IF to_regrole('authenticated') IS NULL THEN CREATE ROLE authenticated NOLOGIN NOINHERIT; END IF;
			   IF to_regrole('service_role') IS NULL THEN CREATE ROLE service_role NOLOGIN NOINHERIT BYPASSRLS; END IF;
			 END $$;",
		)
		.await
		.unwrap();
	drop(one_at_a_time);
	let config: tokio_postgres::Config = admin_url.parse().unwrap();
	let host = match &config.get_hosts()[0] {
		tokio_postgres::config::Host::Tcp(h) => h.clone(),
		#[allow(unreachable_patterns)]
		_ => unreachable!(),
	};
	let files = [
		("changes", include_str!("../migrations/0002_changes.sql")),
		("handlers", include_str!("../migrations/0003_handlers.sql")),
	];
	for (layout, schema) in [("old", "realtime"), ("new", "snout_live")] {
		let names = db::Names {
			schema: schema.into(),
			has_new_roles: layout == "new",
		};
		let name = format!("realtime_released_{layout}_{}", std::process::id());
		for statement in [
			format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"),
			format!("CREATE DATABASE {name}"),
		] {
			admin.batch_execute(&statement).await.unwrap();
		}
		let url = match admin_url.rsplit_once('/') {
			Some((base, _)) => format!("{base}/{name}"),
			None => unreachable!(),
		};
		let (client, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
		tokio::spawn(connection);
		// What the earlier release did in this database.
		client
			.batch_execute(&names.migration(include_str!("../migrations/0001_realtime.sql")))
			.await
			.unwrap();
		client
			.batch_execute(
				"create schema snout_realtime;
				 create table snout_realtime.migrations (
					name text primary key, sha256 text not null, applied_at timestamptz not null default now())",
			)
			.await
			.unwrap();
		for (file, sql) in files {
			client.batch_execute(&names.migration(sql)).await.unwrap();
			let (_, _, released) = db::RELEASED_AS.iter().find(|(n, _, _)| *n == file).unwrap();
			client
				.execute(
					"insert into snout_realtime.migrations (name, sha256, applied_at) values ($1, $2, '2026-10-01')",
					&[&file, released],
				)
				.await
				.unwrap();
		}
		let database = Database {
			host: host.clone(),
			port: config.get_ports().first().copied().unwrap_or(5432),
			name: name.clone(),
			user: config.get_user().unwrap_or("postgres").to_string(),
			password: String::from_utf8_lossy(config.get_password().unwrap_or_default())
				.into_owned(),
			publication: "snoutdata_realtime".into(),
			slot_name: "unused".into(),
			poll_interval_ms: 100,
			poll_max_record_bytes: 1_048_576,
			poll_max_changes: 100,
		};
		let pool = db::new_pool(&database, 2).unwrap();
		db::prepare(&pool).await.unwrap();
		assert_eq!(pool.names().await.unwrap().schema, schema, "{layout}");
		let ledger: Vec<(String, String, bool)> = client
			.query(
				"select name, sha256, applied_at = '2026-10-01' from snout_realtime.migrations order by name",
				&[],
			)
			.await
			.unwrap()
			.iter()
			.map(|r| (r.get(0), r.get(1), r.get(2)))
			.collect();
		let expected: Vec<(String, String, bool)> = files
			.iter()
			.map(|(file, _)| {
				let (_, _, released) = db::RELEASED_AS.iter().find(|(n, _, _)| n == file).unwrap();
				(file.to_string(), released.to_string(), true)
			})
			.collect();
		assert_eq!(ledger, expected, "{layout}: nothing ran again");
		// The file as it is today is what the ledger's hash stands for.
		for (file, sql) in files {
			assert!(
				db::RELEASED_AS
					.iter()
					.any(|(n, today, _)| *n == file && *today == sha256_hex(sql)),
				"{file}"
			);
		}
		drop(client);
		pool.close();
		admin
			.batch_execute(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
			.await
			.unwrap();
	}
}
