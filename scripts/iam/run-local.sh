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
ROOT_DIR="$(cd -- "$SCRIPT_DIR/../.." && pwd)"

CONFIG_FILE="$ROOT_DIR/cmd/iam/.local/.config.env"
HUB_CONFIG_FILE="$ROOT_DIR/cmd/hub/.local/.config.env"
CERT_DIR="$ROOT_DIR/cmd/iam/.local/certs"
CERT_FILE="$CERT_DIR/iam.crt"
KEY_FILE="$CERT_DIR/iam.key"
SIGNING_KEY_PEM=""

cleanup_signing_key() {
  if [[ -n "$SIGNING_KEY_PEM" ]]; then
    rm -f -- "$SIGNING_KEY_PEM"
  fi
}
trap cleanup_signing_key EXIT

if [[ ! -f "$CONFIG_FILE" ]]; then
  mkdir -p "$(dirname -- "$CONFIG_FILE")"
  echo "Missing local IAM config: $CONFIG_FILE" >&2
  echo "Initialize it with:" >&2
  echo "  cp \"$ROOT_DIR/cmd/iam/.example.env\" \"$CONFIG_FILE\"" >&2
  exit 1
fi

# Read KEY from the config file (last assignment wins; CR and surrounding quotes stripped).
get_config() {
  local key="$1" config_file="${2:-$CONFIG_FILE}" value
  value="$(grep -E "^$key=" "$config_file" | tail -n 1 | cut -d= -f2- | tr -d '\r' || true)"
  value="${value#\"}"
  value="${value%\"}"
  printf '%s' "$value"
}

# Set KEY="value" in the config file: replace the existing line (keeping CRLF if used) or append it.
set_config() {
  local key="$1" value="$2" config_file="${3:-$CONFIG_FILE}" tmp
  tmp="$(mktemp "$config_file.XXXXXX")"
  if grep -qE "^$key=" "$config_file"; then
    awk -v key="$key" -v line="$key=\"$value\"" '
      $0 ~ "^" key "=" { print line (($0 ~ /\r$/) ? "\r" : ""); next }
      { print }
    ' "$config_file" > "$tmp"
  else
    cat -- "$config_file" > "$tmp"
    [[ -z "$(tail -c 1 -- "$config_file")" ]] || echo >> "$tmp"
    printf '%s="%s"\n' "$key" "$value" >> "$tmp"
  fi
  mv -f -- "$tmp" "$config_file"
}

# Add or replace one ID:base64 entry while preserving other key IDs. This keeps
# Hub's old verification keys available during local key rotation.
set_key_entry() {
  local key="$1" id="$2" encoded="$3" config_file="$4"
  local current entry entry_id merged="" replaced=false

  current="$(get_config "$key" "$config_file")"
  while [[ -n "$current" ]]; do
    entry="${current%%,*}"
    if [[ "$current" == *,* ]]; then
      current="${current#*,}"
    else
      current=""
    fi

    entry="${entry#"${entry%%[![:space:]]*}"}"
    entry="${entry%"${entry##*[![:space:]]}"}"
    [[ -z "$entry" || "$entry" == *"<"* ]] && continue

    entry_id="${entry%%:*}"
    if [[ "$entry_id" == "$id" ]]; then
      if [[ "$replaced" == false ]]; then
        entry="$id:$encoded"
        replaced=true
      else
        continue
      fi
    fi

    if [[ -n "$merged" ]]; then
      merged+=","
    fi
    merged+="$entry"
  done

  if [[ "$replaced" == false ]]; then
    [[ -z "$merged" ]] || merged+=","
    merged+="$id:$encoded"
  fi
  set_config "$key" "$merged" "$config_file"
}

derive_ticket_signing_public_key() {
  local seed="$1"
  {
    printf '\x30\x2e\x02\x01\x00\x30\x05\x06\x03\x2b\x65\x70\x04\x22\x04\x20'
    printf '%s' "$seed" | openssl base64 -d -A
  } | openssl pkey -inform DER -pubout -outform DER | tail -c 32 | openssl base64 -A
}

# IAM signs tickets with an Ed25519 seed; Hub receives only its public key.
# Keep configured signing keys stable, and generate a keypair only for the
# empty/example placeholder values.
TICKET_SIGNING_KEY_ID="$(get_config TICKET_SIGNING_KEY_ID)"
TICKET_SIGNING_KEY_ID="${TICKET_SIGNING_KEY_ID:-1}"
TICKET_SIGNING_PRIVATE_KEY="$(get_config TICKET_SIGNING_PRIVATE_KEY)"
TICKET_SIGNING_PUBLIC_KEY=""

if [[ -z "$TICKET_SIGNING_PRIVATE_KEY" || "$TICKET_SIGNING_PRIVATE_KEY" == *"<"* ]]; then
  mkdir -p "$CERT_DIR"
  umask 077
  SIGNING_KEY_PEM="$(mktemp "$CERT_DIR/ticket-signing.XXXXXX.pem")"
  openssl genpkey -algorithm ED25519 -out "$SIGNING_KEY_PEM"
  TICKET_SIGNING_PRIVATE_KEY="$(openssl pkey -in "$SIGNING_KEY_PEM" -outform DER | tail -c 32 | openssl base64 -A)"
  TICKET_SIGNING_PUBLIC_KEY="$(openssl pkey -in "$SIGNING_KEY_PEM" -pubout -outform DER | tail -c 32 | openssl base64 -A)"
  rm -f -- "$SIGNING_KEY_PEM"
  SIGNING_KEY_PEM=""

  set_config TICKET_SIGNING_PRIVATE_KEY "$TICKET_SIGNING_PRIVATE_KEY"
  echo "Generated TICKET_SIGNING_PRIVATE_KEY (key id $TICKET_SIGNING_KEY_ID) in $CONFIG_FILE" >&2
fi

if [[ -z "$(get_config TICKET_SIGNING_KEY_ID)" ]]; then
  set_config TICKET_SIGNING_KEY_ID "$TICKET_SIGNING_KEY_ID"
fi

if [[ -z "$TICKET_SIGNING_PUBLIC_KEY" ]]; then
  TICKET_SIGNING_PUBLIC_KEY="$(derive_ticket_signing_public_key "$TICKET_SIGNING_PRIVATE_KEY")"
fi

# Keep Hub's local verifier config in sync with IAM. Start it from the example
# when it does not exist, then merge entries by key ID so older keys survive.
if [[ ! -f "$HUB_CONFIG_FILE" ]]; then
  mkdir -p "$(dirname -- "$HUB_CONFIG_FILE")"
  cp -- "$ROOT_DIR/cmd/hub/.example.env" "$HUB_CONFIG_FILE"
fi

set_key_entry TICKET_SIGNING_PUBLIC_KEYS "$TICKET_SIGNING_KEY_ID" "$TICKET_SIGNING_PUBLIC_KEY" "$HUB_CONFIG_FILE"
echo "Synchronized IAM public ticket verification keys to $HUB_CONFIG_FILE" >&2

# Point IAM only at the public output of the separately provisioned Hub key.
DESCRIPTOR_FILE="$(get_config HUB_NOISE_DESCRIPTOR_FILE)"
if [[ -z "$DESCRIPTOR_FILE" ]]; then
  DESCRIPTOR_FILE="$ROOT_DIR/cmd/hub/.local/hub-descriptor.json"
  set_config HUB_NOISE_DESCRIPTOR_FILE "$DESCRIPTOR_FILE"
fi
if [[ ! -s "$DESCRIPTOR_FILE" ]]; then
  echo "Provision the Hub keyring and public descriptor first; missing $DESCRIPTOR_FILE" >&2
  echo "See docs/protocol/noise-v2.md. IAM must never receive the Hub private keyring." >&2
  exit 1
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
