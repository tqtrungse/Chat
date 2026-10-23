#!/usr/bin/env bash
# Regenerate public Hub protobufs using the validate schema pinned in go.mod.
set -euo pipefail
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
cd "$ROOT_DIR"
VALIDATE_DIR="$(go list -m -f '{{.Dir}}' github.com/envoyproxy/protoc-gen-validate)"
protoc -I api/hub/v2/proto/src -I "$VALIDATE_DIR" \
  --go_out=api/hub/v2/proto/gen --go_opt=paths=source_relative \
  --validate_out=lang=go,paths=source_relative:api/hub/v2/proto/gen \
  pub/message.proto
