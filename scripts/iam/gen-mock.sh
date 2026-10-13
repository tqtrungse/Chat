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
  -source=internal/iam/application/exchange_key/device_meta_reader.go \
  -destination=internal/iam/application/exchange_key/mocks/device_meta_reader_mock.go \
  -package=mocks

mockgen \
  -source=internal/iam/application/exchange_key/key_deriver.go \
  -destination=internal/iam/application/exchange_key/mocks/key_deriver_mock.go \
  -package=mocks

mockgen \
  -source=internal/iam/application/exchange_key/ticket_issuer.go \
  -destination=internal/iam/application/exchange_key/mocks/ticket_issuer_mock.go \
  -package=mocks
