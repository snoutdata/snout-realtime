# snout-realtime

Realtime for Postgres-backed apps over WebSockets: **broadcast** between clients, **presence**
(who is here), and **database changes** as they are committed, with the project's own
**row-level security deciding who sees what**. One static binary, written in Rust, built to run
every project on a SnoutData Cloud host.

> **Status: in production.** Every SnoutData Cloud project's Realtime runs on it since
> 2026-09-28 (image 0.1.0).

- **Your policies decide.** A private channel is authorised by the policies on
  `realtime.messages`, and a database change reaches a subscriber only if a `select` of that row,
  run as the subscriber, returns it. The server never decides access itself.
- **The channel protocol the JavaScript clients speak**, in both serializers (`vsn=1.0.0` JSON
  objects, and `vsn=2.0.0` arrays with binary broadcast frames): join, leave, heartbeat,
  token refresh and expiry, broadcast (with acknowledgement, and to yourself), presence (track,
  untrack, state, diffs), and database changes with filters.
- **Database changes streamed, not polled.** Changes are read from a logical replication slot the
  moment they commit and decided in batches: the changes to a table that have queued, one when
  they are sparse. Each subscriber's filters are checked in the project's database, each in its
  own subtransaction; then its row-level security, as the subscriber, once per distinct set of
  claims for the whole batch (a thousand subscribers who are the same user cost one check, a
  busy table one check per batch rather than per change), shared between two connections once
  a project has many subscribers, each check its own statement, so a
  policy that raises for one subscriber costs that subscriber alone. What a subscriber sees of the
  row is what its role may select.
- **Broadcast from the database.** A row written to `realtime.messages` (by `realtime.send()` or
  a trigger of yours) is broadcast to the topic it names, streamed from the WAL, not polled.
- **Its own schema.** On a project's first connection it creates the `realtime` tables and
  functions (`migrations/0001_realtime.sql`) and its change logic in `snout_realtime`
  (`migrations/0002_changes.sql`), recording each file's hash in `snout_realtime.migrations`;
  the second is replaced whenever it changes. `realtime.messages` is partitioned by day: every
  hour, partitions are made three days ahead and those older than three days are dropped.
- **Many projects, one process.** A tenant (a project) is registered through an admin API with its
  database and its JWT secret; sockets are routed to it by host name.
- **Sharded databases.** When the registered database is the home node of a
  snout-lepis cluster (it has a `lepis` schema), its
  changes come from every node: one stream per node, all delivering to the same subscribers. The
  home node streams every published table; each other node streams its sharded tables, so a
  reference table written on every node is sent once. A subscription is written on every node and
  a change is checked on the node it came from, by that node's own policies. The node list is
  followed as it changes (`LISTEN lepis_epoch`). Rows Lepis copies between nodes (they arrive by
  logical replication) and rows it deletes from a node that no longer owns them (they fail that
  node's `lepis_owns` fence) are not sent, since nobody changed them. Changes from different
  nodes are not ordered against each other; each node's are in its commit order. See
  "Sharded databases" below for what the role needs.
- **Small.** A 3 MB image built `FROM scratch`, under 1 MB of memory at idle, ready in about
  0.15 s. On a 2-CPU arm64 machine, 1,000 subscribers of one table, each a different user under
  a row-level security policy, used 33 MB; broadcast to 90 subscribers at 20 a second, 3 MB.

## Running it

```sh
podman build -f realtime/Containerfile -t snout-realtime .   # from the workspace root
```

| Variable | Default | Secret | What |
|---|---|---|---|
| `PORT` | `4000` | no | Sockets (`/socket/websocket`), the tenant API, `/metrics`, `/` |
| `HOST` | `0.0.0.0` | no | Where it listens |
| `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME` | port `5432` | the password | The metadata database that holds the tenants |
| `API_JWT_SECRET` | none, required | yes | Verifies the tenant API's bearer token |
| `METRICS_JWT_SECRET` | none, required | yes | Verifies `/metrics`' bearer token |
| `DB_ENC_KEY` | none, required | yes | Encrypts each tenant's JWT secret and database password at rest |
| `SNOUT_REALTIME_MAX_SOCKETS_PER_ADDRESS` | `100` | no | Sockets one client address may hold per project; `0` for no cap |
| `LOG_LEVEL` | `info` | no | `error`, `warn`, `info`, `debug` |

There is no default secret anywhere: a missing one is a refusal to start.

## The tenant API

Every call carries `Authorization: Bearer <JWT signed with API_JWT_SECRET>`.

| Call | What |
|---|---|
| `POST /api/tenants` | Create or replace a tenant: `{ tenant: { external_id, jwt_secret, max_concurrent_users, max_channels_per_client, max_events_per_second, extensions: [{ type: "postgres_cdc_rls", settings: { db_host, db_port, db_name, db_user, db_password, publication, slot_name } }] } }` |
| `GET /api/tenants`, `GET /api/tenants/{id}` | List, or one; secrets are never returned |
| `DELETE /api/tenants/{id}` | Forget one, closing its sockets |
| `GET /api/tenants/{id}/health` | Connects to the project's database (preparing its schema) and says whether it could |

`/metrics` (behind its own secret) reports, per tenant: `realtime_connections_connected`,
`realtime_channel_events`, `realtime_channel_presence_events`, `realtime_channel_db_events`,
`realtime_channel_joins`, `realtime_channel_output_bytes`.

## Broadcast over HTTP

`POST /api/broadcast` with `{ messages: [{ topic, event, payload, private? }] }`, or
`POST /api/broadcast/{topic}/events/{event}` with the payload as the body (`application/json`, or
bytes with `application/octet-stream`; `?private=true` for a private channel). Either takes the
project's key or a user's token, and a private message is sent only if the caller's policy on
`realtime.messages` allows the insert.

## Inspection

`GET /socket/inspect` (`?channel=`) is what a project has open now: each channel, its clients
(socket number, presence key, when it joined, how long since a frame last arrived from it), its
presence state, and its last minute of broadcasts in, deliveries out and the busiest second.
`GET /socket/events` (`?since=<epoch ms>&channel=`) is a log of the newest thousand connects,
refused connects, joins, refused joins, leaves, channels the server closed and disconnects, each
with its reason (the client's close code and reason, a lost connection, a rate limit). Both are
per project from the `Host`, in memory, and answer only the project's `service_role` token: they
are every user's presence, which the anon key in a web page must not read. Under `/socket` so a
gateway that maps `/realtime/v1` to `/socket` reaches them with no route of its own.

## Operating it

- **Health.** `GET /` answers `ok` once the server listens. `GET /api/tenants/{id}/health` (tenant
  API token) connects to that project's database, preparing its schema, and says whether it could.
- **Metrics.** `GET /metrics`, Prometheus text, behind `METRICS_JWT_SECRET` (the counters above).
- **Logs.** JSON lines on standard output, filtered by `LOG_LEVEL` (a `tracing` filter, so
  `info,snout_realtime=debug` works). A policy or filter that raises during a change is logged as
  a Postgres WARNING in the project's own log, naming the subscription, and not here.
- **Stopping.** SIGTERM or Ctrl-C stops it at once; sockets are closed and clients reconnect.
  Temporary replication slots go with the connections that made them, so a stopped server holds
  no WAL.
- **Upgrading.** Replace the image. Each project's `snout_realtime` functions are replaced on its
  next connection when `migrations/0002_changes.sql` has changed (its hash is in
  `snout_realtime.migrations`); the `realtime` tables are created once and not altered.

## Sharded databases

The tenant is registered with the home node, as for any database; nothing else is configured.
The login it connects with is used on every node (snout-lepis gives every node the same roles
with the same password verifiers), and reaches each at its `peer_host` label, or its `host` when
it has none. On the home node that role needs to read the catalog:

```sql
grant usage on schema lepis to <role>;
grant select on lepis.cluster, lepis.node, lepis.relation to <role>;
grant insert, update on lepis.router to <role>;   -- optional, see below
```

Without the first two the project is streamed from its home node alone, with a warning in the log.
The third lets the server acknowledge each catalog epoch in `lepis.router` once the new nodes'
streams are open, so a cutover onto a brand-new node waits for them and no change on it is missed;
without it the server logs that it is not acknowledging, once.

On each other node it prepares its schema, as on any project's first connection, and makes that
node's copy of the publication hold the sharded tables the home node's does (the rights `alter
publication` needs there: the publication's and the tables' owner, or a superuser). When it cannot,
it logs the statement to run there, once. A row-level security policy on a sharded table runs on
the node that holds the row, so it may read that node's own tables, reference tables and functions,
and not a table that lives on the home node alone.

What the server streams in a sharded project, beyond the home node's stream: nothing while nobody
is subscribed; one probe of `pg_namespace` when a project's stream opens and every 30 s while it is
open, so a project sharded with subscribers connected is followed onto its nodes. Changes made on a
node in the moment it is being promoted (a physical split) can be missed when the acknowledgement
above is not granted.

## Development

`bash scripts/test.sh` runs the checks in a container; Docker or Podman is the only thing you
need. `bash tests/db.sh` runs the same suite against a throwaway Postgres 17, database tests
included: who sees each change, and what of it. `bash tests/cluster.sh [18|13]` runs a sharded
project's streams against three throwaway Postgres servers.

## Licence

[Apache License 2.0](./LICENSE). Security reports: [SECURITY.md](./SECURITY.md).
