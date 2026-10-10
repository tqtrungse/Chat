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

CONFIG_FILE="$ROOT_DIR/cmd/iam/.local/.config.env"
CERT_DIR="$ROOT_DIR/cmd/iam/.local/certs"
CERT_FILE="$CERT_DIR/iam.crt"
KEY_FILE="$CERT_DIR/iam.key"

if [[ ! -f "$CONFIG_FILE" ]]; then
  mkdir -p "$(dirname -- "$CONFIG_FILE")"
  echo "Missing local IAM config: $CONFIG_FILE" >&2
  echo "Initialize it with:" >&2
  echo "  cp \"$ROOT_DIR/cmd/iam/.example.env\" \"$CONFIG_FILE\"" >&2
  exit 1
fi

# Read KEY from the config file (last assignment wins; CR and surrounding quotes stripped).
get_config() {
  local value
  value="$(grep -E "^$1=" "$CONFIG_FILE" | tail -n 1 | cut -d= -f2- | tr -d '\r' || true)"
  value="${value#\"}"
  value="${value%\"}"
  printf '%s' "$value"
}

# Set KEY="value" in the config file: replace the existing line (keeping CRLF if used) or append it.
set_config() {
  local key="$1" value="$2" tmp
  tmp="$(mktemp "$CONFIG_FILE.XXXXXX")"
  if grep -qE "^$key=" "$CONFIG_FILE"; then
    awk -v key="$key" -v line="$key=\"$value\"" '
      $0 ~ "^" key "=" { print line (($0 ~ /\r$/) ? "\r" : ""); next }
      { print }
    ' "$CONFIG_FILE" > "$tmp"
  else
    cat -- "$CONFIG_FILE" > "$tmp"
    [[ -z "$(tail -c 1 -- "$CONFIG_FILE")" ]] || echo >> "$tmp"
    printf '%s="%s"\n' "$key" "$value" >> "$tmp"
  fi
  mv -f -- "$tmp" "$CONFIG_FILE"
}

# Generate TICKET_KEYS (format: keyID:base64(32-byte key)) only when it is empty
# or still the placeholder from the example env. An existing key is never
# overwritten, because it must stay in sync with Hub and rotating it would
# invalidate outstanding tickets.
TICKET_KEY_ID="$(get_config TICKET_CURRENT_KEY_ID)"
TICKET_KEY_ID="${TICKET_KEY_ID:-1}"
TICKET_KEYS_VALUE="$(get_config TICKET_KEYS)"

if [[ -z "$TICKET_KEYS_VALUE" || "$TICKET_KEYS_VALUE" == *"<"* ]]; then
  TICKET_KEY="$(openssl rand -base64 32)"
  set_config TICKET_KEYS "${TICKET_KEY_ID}:${TICKET_KEY}"
  grep -qE '^TICKET_CURRENT_KEY_ID=' "$CONFIG_FILE" || set_config TICKET_CURRENT_KEY_ID "$TICKET_KEY_ID"
  echo "Generated TICKET_KEYS (key id $TICKET_KEY_ID) in $CONFIG_FILE" >&2
  echo "Copy the same TICKET_CURRENT_KEY_ID/TICKET_KEYS to Hub so it can open IAM tickets." >&2
fi

if [[ ! -s "$CERT_FILE" || ! -s "$KEY_FILE" ]]; then
  mkdir -p "$CERT_DIR"
  umask 077
  openssl req -x509 -newkey rsa:2048 -sha256 -nodes -days 365 \
    -keyout "$KEY_FILE" \
    -out "$CERT_FILE" \
    -subj "/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
  chmod 600 "$KEY_FILE"
fi

export IAM_CONFIG_FILE="$CONFIG_FILE"
export HTTP_SERVER_CERT_FILE="$CERT_FILE"
export HTTP_SERVER_KEY_FILE="$KEY_FILE"

cd "$ROOT_DIR"
exec go run ./cmd/iam