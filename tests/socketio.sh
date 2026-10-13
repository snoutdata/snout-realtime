#!/usr/bin/env bash
# Realtime handlers and the Socket.IO door, end to end: a throwaway Postgres 17 (logical WAL, the
# anon/authenticated/service_role roles), snout-realtime built and run in the stack's dev
# container, and tests/socketio.mjs driving it with the REAL socket.io-client (polling with the
# upgrade, and websocket only) and a raw Phoenix client, from a Node container.
#
#   bash tests/socketio.sh
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
stack="$(cd "$here/../.." && pwd)"
engine="${STACK_ENGINE:-docker}"
run="$$"
net="snout-sio-$run"
pg="snout-sio-pg-$run"
rt="snout-sio-rt-$run"

cleanup() {
	"$engine" rm -fv "$rt" "$pg" >/dev/null 2>&1 || true
	"$engine" network rm "$net" >/dev/null 2>&1 || true
}
trap cleanup EXIT

"$engine" network create "$net" >/dev/null
"$engine" run -d --rm --name "$pg" --network "$net" -e POSTGRES_PASSWORD=test \
	postgres:17-alpine -c wal_level=logical -c snout_utils.privileged_role=project_owner >/dev/null
# Over TCP: the image's first-boot init runs a temporary server on the socket only, then restarts.
until "$engine" exec "$pg" psql -h 127.0.0.1 -U postgres -tAc 'select 1' >/dev/null 2>&1; do sleep 0.5; done
"$engine" exec "$pg" psql -h 127.0.0.1 -U postgres -q -c "create database meta"
"$engine" exec "$pg" psql -h 127.0.0.1 -U postgres -v ON_ERROR_STOP=1 -q -c "
	create role anon nologin noinherit;
	create role authenticated nologin noinherit;
	create role service_role nologin noinherit bypassrls;
	grant anon, authenticated, service_role to postgres;
	create role project_owner login password 'owner' nosuperuser bypassrls;
	grant create on schema public to project_owner;"

# Build once, then run the binary from the dev container's build cache.
bash "$stack/scripts/dev.sh" cargo build -p snout-realtime >&2
image="$("$engine" images --format '{{.Repository}}:{{.Tag}}' | grep '^snout-stack-dev:' | head -1)"
"$engine" run -d --rm --name "$rt" --network "$net" \
	-v snout-stack-target:/cache/target \
	-e DB_HOST="$pg" -e DB_USER=postgres -e DB_PASSWORD=test -e DB_NAME=meta \
	-e API_JWT_SECRET=api-secret-for-the-socketio-test-0123456789 \
	-e METRICS_JWT_SECRET=metrics-secret-for-the-socketio-test-01234 \
	-e DB_ENC_KEY=enc-key-for-the-socketio-test-0123456789abc \
	-e LOG_LEVEL=info \
	"$image" /cache/target/debug/snout-realtime >/dev/null

"$engine" run --rm --network "$net" \
	-v "$here/socketio.mjs:/test/socketio.mjs:ro" \
	-e RT="http://$rt:4000" -e PG="postgres://postgres:test@$pg:5432/postgres" \
	-e API_JWT_SECRET=api-secret-for-the-socketio-test-0123456789 \
	node:22-alpine sh -c '
		cd /tmp && npm init -y >/dev/null 2>&1 &&
		npm install --silent --no-audit --no-fund socket.io-client@4.8.1 ws@8.18.0 pg@8.13.1 >/dev/null 2>&1 &&
		cp /test/socketio.mjs . && node socketio.mjs' || {
	echo "--- snout-realtime log (last 60 lines)" >&2
	"$engine" logs "$rt" 2>&1 | tail -60 >&2
	exit 1
}
