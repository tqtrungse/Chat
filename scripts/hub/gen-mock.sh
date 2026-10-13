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

cd "$ROOT_DIR"

mockgen \
  -source=internal/hub/connection/broker.go \
  -destination=internal/hub/connection/mocks/broker_mock.go \
  -package=mocks

mockgen \
  -source=internal/hub/connection/cache.go \
  -destination=internal/hub/connection/mocks/cache_mock.go \
  -package=mocks

mockgen \
  -source=internal/hub/connection/cb.go \
  -destination=internal/hub/connection/mocks/cb_mock.go \
  -package=mocks

mockgen \
  -source=internal/hub/connection/msg_reader.go \
  -destination=internal/hub/connection/mocks/msg_reader_mock.go \
  -package=mocks

mockgen \
  -source=internal/hub/connection/signer.go \
  -destination=internal/hub/connection/mocks/signer_mock.go \
  -package=mocks

mockgen \
  -source=internal/hub/connection/ticket_verifier.go \
  -destination=internal/hub/connection/mocks/ticket_verifier_mock.go \
  -package=mocks

mockgen \
  -source=internal/hub/protocol/secure_channel.go \
  -destination=internal/hub/protocol/mocks/secure_channel_mock.go \
  -package=mocks
