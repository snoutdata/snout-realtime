#!/usr/bin/env bash
# tests/cluster.rs against three real Postgres servers (a sharded project's home node and two
# others), on their own network, with the stack's dev container joined to it.
#
#   bash tests/cluster.sh          postgres:18, the fleet's
#   bash tests/cluster.sh 17       the oldest Postgres a Lepis node may run (L1)
#
# Everything it starts is named for this run and removed after, by name. One test at a time: two
# streams from one process to one server would want the same slot name.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
engine="${STACK_ENGINE:-docker}"
major="${1:-18}"
run="snout-realtime-cluster-$$"
net="$run-net"
nodes=("$run-1" "$run-2" "$run-3")

cleanup() {
	"$engine" rm -fv "${nodes[@]}" >/dev/null 2>&1 || true
	"$engine" network rm "$net" >/dev/null 2>&1 || true
}
trap cleanup EXIT

"$engine" network create "$net" >/dev/null
for n in "${nodes[@]}"; do
	"$engine" run -d --name "$n" --network "$net" -e POSTGRES_PASSWORD=test \
		"docker.io/library/postgres:$major" -c wal_level=logical >/dev/null
done
for n in "${nodes[@]}"; do
	for _ in 1 2; do
		until "$engine" exec "$n" psql -U postgres -Atc 'select 1' >/dev/null 2>&1; do sleep 0.5; done
		sleep 1
	done
done

# The stack's dev container: two levels up in the monorepo, one in the published repository.
dev="$here/../../scripts/dev.sh"
[ -f "$dev" ] || dev="$here/../scripts/dev.sh"

STACK_DEV_ARGS="--network $net -e REALTIME_TEST_CLUSTER=${nodes[0]}:5432,${nodes[1]}:5432,${nodes[2]}:5432" \
	bash "$dev" cargo test -p snout-realtime --test cluster -- --nocapture --test-threads 1
