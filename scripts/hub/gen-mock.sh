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
  -source=internal/hub/application/activateconn/ports.go \
  -destination=internal/hub/application/activateconn/mocks/ports_mock.go \
  -package=mocks

mockgen \
  -source=internal/hub/application/offlinedelivery/ports.go \
  -destination=internal/hub/application/offlinedelivery/mocks/ports_mock.go \
  -package=mocks

mockgen \
  -source=internal/hub/application/sendmsg/ports.go \
  -destination=internal/hub/application/sendmsg/mocks/ports_mock.go \
  -package=mocks

mockgen \
  -source=internal/hub/core/activation/ports.go \
  -destination=internal/hub/core/activation/mocks/ports_mock.go \
  -package=mocks

mockgen \
  -source=internal/hub/core/connection/ports.go \
  -destination=internal/hub/core/connection/mocks/ports_mock.go \
  -package=mocks

mockgen \
  -source=internal/hub/core/presence/ports.go \
  -destination=internal/hub/core/presence/mocks/ports_mock.go \
  -package=mocks

mockgen \
  -source=internal/hub/core/transport/ports.go \
  -destination=internal/hub/core/transport/mocks/ports_mock.go \
  -package=mocks

mockgen \
  -source=internal/hub/adapter/tcp/ports.go \
  -destination=internal/hub/adapter/tcp/mocks/ports_mock.go \
  -package=mocks
