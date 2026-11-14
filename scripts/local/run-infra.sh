#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/../.." && pwd)"

COMPOSE_FILE="$ROOT_DIR/deploy/local-docker-compose.yaml"
NATS_ENV_FILE="$ROOT_DIR/deploy/nats/local/.local.env"
REDIS_ENV_FILE="$ROOT_DIR/deploy/redis/local/.local.env"

if [[ ! -f "$COMPOSE_FILE" ]]; then
  echo "Missing Docker Compose file: $COMPOSE_FILE" >&2
  exit 1
fi

if [[ ! -f "$NATS_ENV_FILE" ]]; then
  echo "Missing NATS local environment file: $NATS_ENV_FILE" >&2
  exit 1
fi

if [[ ! -f "$REDIS_ENV_FILE" ]]; then
  echo "Missing Redis local environment file: $REDIS_ENV_FILE" >&2
  exit 1
fi

exec docker compose \
  --env-file "$NATS_ENV_FILE" \
  --env-file "$REDIS_ENV_FILE" \
  -f "$COMPOSE_FILE" \
  up -d
