#!/usr/bin/env bash
set -euo pipefail

# Local repro for the chasquimq TLS integration test.
# Spins up a TLS-enabled Redis 8.6 container with self-signed certs, runs the
# `#[ignore]`d TLS test against it, then tears the container down.
#
# CI does NOT run this — TLS infra is local-only for now (see Phase 1 plan).

PORT="${TLS_REDIS_PORT:-6390}"
CONTAINER_NAME="${TLS_REDIS_CONTAINER:-chasquimq-tls-redis}"
CERT_DIR="${TLS_CERT_DIR:-$(pwd)/.claude/tls-test-certs}"

cleanup() {
  docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

mkdir -p "$CERT_DIR"

if [ ! -f "$CERT_DIR/redis.crt" ] || [ ! -f "$CERT_DIR/ca.crt" ]; then
  echo ">> generating self-signed CA + server cert in $CERT_DIR"
  rm -f "$CERT_DIR"/*.crt "$CERT_DIR"/*.key "$CERT_DIR"/*.csr "$CERT_DIR"/*.srl
  openssl req -x509 -nodes -newkey rsa:2048 \
    -keyout "$CERT_DIR/ca.key" \
    -out "$CERT_DIR/ca.crt" \
    -days 1 \
    -subj "/CN=chasquimq-test-CA" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" \
    >/dev/null 2>&1
  openssl req -new -nodes -newkey rsa:2048 \
    -keyout "$CERT_DIR/redis.key" \
    -out "$CERT_DIR/redis.csr" \
    -subj "/CN=127.0.0.1" \
    >/dev/null 2>&1
  openssl x509 -req \
    -in "$CERT_DIR/redis.csr" \
    -CA "$CERT_DIR/ca.crt" -CAkey "$CERT_DIR/ca.key" -CAcreateserial \
    -out "$CERT_DIR/redis.crt" \
    -days 1 \
    -extfile <(printf "subjectAltName=IP:127.0.0.1,DNS:localhost\nextendedKeyUsage=serverAuth\n") \
    >/dev/null 2>&1
fi

cleanup

echo ">> starting TLS redis on port $PORT"
docker run -d --name "$CONTAINER_NAME" \
  -p "${PORT}:6379" \
  -v "$CERT_DIR:/tls:ro" \
  redis:8.6 \
  redis-server \
  --port 0 \
  --tls-port 6379 \
  --tls-cert-file /tls/redis.crt \
  --tls-key-file /tls/redis.key \
  --tls-ca-cert-file /tls/ca.crt \
  --tls-auth-clients no \
  >/dev/null

# Wait for redis to accept TLS connections (via redis-cli inside the container).
for _ in $(seq 1 30); do
  if docker exec "$CONTAINER_NAME" redis-cli \
      --tls --cert /tls/redis.crt --key /tls/redis.key --cacert /tls/ca.crt \
      -p 6379 ping 2>/dev/null | grep -q PONG; then
    break
  fi
  sleep 0.5
done

# Cargo's rustls trust roots are platform-native; tell it to also trust our CA
# via the SSL_CERT_FILE env var that rustls-native-certs honours.
export SSL_CERT_FILE="$CERT_DIR/ca.crt"
export REDIS_TLS_URL="rediss://127.0.0.1:${PORT}"

echo ">> running cargo test --test tls -- --ignored"
cargo test --manifest-path chasquimq/Cargo.toml --test tls -- --ignored --nocapture
