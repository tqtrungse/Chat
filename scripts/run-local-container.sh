#!/usr/bin/env bash

#
# Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"

COMPOSE_FILE="$ROOT_DIR/infrastructure/local-docker-compose.yaml"
REDIS_ENV_FILE="$ROOT_DIR/infrastructure/redis/local/.local.env"

if [[ ! -f "$COMPOSE_FILE" ]]; then
  echo "Missing Docker Compose file: $COMPOSE_FILE" >&2
  exit 1
fi

if [[ ! -f "$REDIS_ENV_FILE" ]]; then
  echo "Missing Redis local environment file: $REDIS_ENV_FILE" >&2
  exit 1
fi

exec docker compose \
  --env-file "$REDIS_ENV_FILE" \
  -f "$COMPOSE_FILE" \
  up -d
