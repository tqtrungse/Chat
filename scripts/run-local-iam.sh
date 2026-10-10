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
