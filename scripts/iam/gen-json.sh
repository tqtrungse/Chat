#!/usr/bin/env bash
# Regenerate public Hub protobufs using the validate schema pinned in go.mod.
set -euo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
cd "$ROOT_DIR"
easyjson -all api/iam/v2/json/message.go
