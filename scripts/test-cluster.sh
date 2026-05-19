#!/usr/bin/env bash
set -euo pipefail

# Local repro for the chasquimq Redis Cluster integration test.
# Spins up a minimal 3-primary (no-replica) Redis 8.6 cluster in one
# container, waits for the slot table to settle, runs the `#[ignore]`d
# `cluster` test against it, then tears the container down.
#
# A 3-primary, 0-replica cluster is the smallest topology that still
# distributes the 16384 hash slots across multiple nodes — enough to
# prove the `{chasqui:<queue>}` hash tag co-locates a queue's whole
# keyspace on one slot and that every multi-key Lua script routes there
# instead of returning CROSSSLOT.
#
# The dedicated `cluster` CI job runs the same test the same way; this
# script is the local mirror (like scripts/test-tls.sh).

CONTAINER_NAME="${CLUSTER_REDIS_CONTAINER:-chasquimq-cluster-redis}"
BASE_PORT="${CLUSTER_REDIS_BASE_PORT:-7000}"
IMAGE="${CLUSTER_REDIS_IMAGE:-redis:8.6.2}"

cleanup() {
  docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

P0="$BASE_PORT"
P1="$((BASE_PORT + 1))"
P2="$((BASE_PORT + 2))"

echo ">> starting 3-primary redis cluster ($IMAGE) on ports $P0-$P2"
docker run -d --name "$CONTAINER_NAME" \
  -p "${P0}:${P0}" -p "${P1}:${P1}" -p "${P2}:${P2}" \
  "$IMAGE" \
  bash -c "
    for p in $P0 $P1 $P2; do
      redis-server --port \$p --cluster-enabled yes \
        --cluster-config-file nodes-\$p.conf --cluster-node-timeout 5000 \
        --appendonly no --save '' --daemonize yes
    done
    sleep 1
    yes yes | redis-cli --cluster create \
      127.0.0.1:$P0 127.0.0.1:$P1 127.0.0.1:$P2 \
      --cluster-replicas 0
    # Keep the container alive in the foreground.
    tail -f /dev/null
  " >/dev/null

# Wait for the cluster to report `cluster_state:ok` (slots fully assigned).
ready=""
for _ in $(seq 1 60); do
  if docker exec "$CONTAINER_NAME" redis-cli -p "$P0" cluster info 2>/dev/null \
      | grep -q "cluster_state:ok"; then
    ready=1
    break
  fi
  sleep 1
done

if [ -z "$ready" ]; then
  echo "!! cluster did not reach cluster_state:ok in time" >&2
  docker logs "$CONTAINER_NAME" >&2 || true
  exit 1
fi

export REDIS_CLUSTER_URL="redis-cluster://127.0.0.1:${P0}"

echo ">> running cargo test --test cluster -- --include-ignored"
cargo test --manifest-path chasquimq/Cargo.toml --test cluster -- \
  --include-ignored --nocapture
