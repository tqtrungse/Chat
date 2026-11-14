#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
INFRA_SCRIPT="$SCRIPT_DIR/../local/infra.sh"

if [[ $# -ne 1 || -z "$1" ]]; then
  echo "Usage: $0 <project>" >&2
  exit 1
fi

PROJECT="$1"

exec "$INFRA_SCRIPT" run --rm dbmigrator sh -c 'ridgepole -E local -c "/internal/$1/database/config/ridgepole.yaml" -f "/internal/$1/database/schemas/Schemafile" --apply --verbose;' sh "$PROJECT"
