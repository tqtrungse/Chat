#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/../.." && pwd)"

cd "$ROOT_DIR"

### Hub

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

### IAM

mockgen \
  -source=internal/iam/adapter/http/ports.go \
  -destination=internal/iam/adapter/http/mocks/ports_mock.go \
  -package=mocks

mockgen \
  -source=internal/iam/application/connticket/ports.go \
  -destination=internal/iam/application/connticket/mocks/ports_mock.go \
  -package=mocks

mockgen \
  -source=internal/iam/core/ticketlimit/ports.go \
  -destination=internal/iam/core/ticketlimit/mocks/ports_mock.go \
  -package=mocks

### Pkg

mockgen \
  -source=pkg/breaker/port.go \
  -destination=pkg/breaker/mocks/port_mock.go \
  -package=mocks

mockgen \
  -source=pkg/database/sql/db.go \
  -destination=pkg/database/sql/mocks/db_mock.go \
  -package=mocks

mockgen \
  -source=pkg/nio/nio.go \
  -destination=pkg/nio/mocks/nio_mock.go \
  -package=mocks