#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/../.." && pwd)"

COMPOSE_FILE="$ROOT_DIR/deploy/local-docker-compose.yaml"
AUTH_ENV_FILE="$ROOT_DIR/deploy/auth/local/.env"
MYSQL_ENV_FILE="$ROOT_DIR/deploy/mysql/local/.env"
NATS_ENV_FILE="$ROOT_DIR/deploy/nats/local/.env"
REDIS_ENV_FILE="$ROOT_DIR/deploy/redis/local/.env"

if [[ ! -f "$COMPOSE_FILE" ]]; then
  echo "Missing docker compose file: $COMPOSE_FILE" >&2
  exit 1
fi

if [[ ! -f "$AUTH_ENV_FILE" ]]; then
  echo "Missing auth local environment file: $AUTH_ENV_FILE" >&2
  exit 1
fi

if [[ ! -f "$MYSQL_ENV_FILE" ]]; then
  echo "Missing MySQL local environment file: $MYSQL_ENV_FILE" >&2
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

if [[ $# -eq 0 ]]; then
  set -- up -d
fi

exec docker compose \
  --env-file "$AUTH_ENV_FILE" \
  --env-file "$MYSQL_ENV_FILE" \
  --env-file "$NATS_ENV_FILE" \
  --env-file "$REDIS_ENV_FILE" \
  -f "$COMPOSE_FILE" \
  "$@"
