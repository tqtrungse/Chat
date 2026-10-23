# Distributed chat backend

> **Status: 🚧 In progress.** Under active development and not production-ready. Architecture and APIs may change. See [Project Status](#project-status) for per-component progress.

A distributed hub server written in Go. Each hub holds a large number of TCP connections from devices and routes messages between devices, including devices that stay offline for long periods.

## Goals

- ~10k concurrent TCP connections per hub, with multiple hubs running side by side.
- Online routing between hubs over NATS core.
- Offline message storage in ScyllaDB, delivered when the device reconnects.
- Multi-device support. No chat history is stored server-side (clients keep their own).
- Crash detection and distributed reaping of dead hubs.
- Resilience against reconnect storms (circuit breakers, batching, bounded queues).

## Project Status

| Component | Status | Notes |
|---|---|---|
| Connection layer (pkg/nio) | 🟢 Implemented | TCP connection handling |
| Redis connection ledger | 🟢 Implemented | Tested with miniredis |
| Crash detection / distributed reaping | 🟡 In progress | Current focus |
| Router (`router.go`) | 🟡 In progress | NATS core routing for online devices |
| NATS connection lifecycle | 🟢  Implemented | `Connect()` / `Start()` separation, reconnect handlers |
| Offline message pull on connect | 🟡 In progress | Gated on Redis registration, live-message buffering |
| ScyllaDB offline storage | 🟠 Design | Schema decided, implementation pending |
| ScyllaDB circuit breaker | 🟠 Design | Separate from the Redis breaker |
| Benchmarks at 10k conns/hub | ⚪ Not started | |

Legend: 🟢 implemented · 🟡 in progress · 🟠 designed, not built · 🔵 evaluating · ⚪ not started

## Architecture

```
                ┌──────────────┐
   Device ─TCP─▶│  Hub (nio)  │◀──────────────┐
                └──────┬───────┘               │
                       │                       │ NATS core
        ┌──────────────┼──────────────┐        │ (online routing)
        ▼              ▼              ▼        │
     Redis          Router ───────────────────▶│ Other hubs
 (connection        │
  ledger)           ▼
                 ScyllaDB
             (offline messages)
```

| Component | Role |
|---|---|
| **Connection layer** (pkg/nio) | Accepts and manages device TCP connections |
| **Redis** (standalone + replicas, no cluster) | Shared ledger: which device is online on which hub |
| **Router** | Routes messages to the target hub via NATS core |
| **NATS core** | Online routing only. JetStream is not used |
| **ScyllaDB** | Offline messages and idempotence records |

## Message Flow

### Device connects

1. The hub verifies the connection.
2. The hub registers the device in Redis, retrying with full-jitter backoff on failure.
3. Only after a successful Redis registration does the hub pull offline messages.
4. Live messages arriving during the backlog pull are buffered and delivered after the backlog is drained, to preserve ordering.

### Sending a message

- The server assigns `MsgId` using sonyflake (all hubs share one start time, `machineID = hubID`).
- The client generates a random `uint64` `dedupKey` per message so retries can be deduplicated.
- **Target online:** the router looks up the device in Redis and publishes over NATS core to the hub holding the connection.
- **Target offline:** the message is written directly to ScyllaDB.

## Protocol: Noise NK v2

Suite: `Noise_NK_25519_ChaChaPoly_SHA256`.

### Connection flow

1. **Client → IAM:** call `POST /iam/v2/connection-ticket` over HTTPS with a Bearer token and `{"device_id":"42"}`.
2. **IAM:** verify the token, load the device from DB, check ownership and active state, and validate its stored Ed25519 identity public key.
3. **IAM → Client:** return a signed ticket valid for 15 seconds and the public Hub descriptor.
4. **Client → Hub:** connect to the descriptor's TCP address and send Noise message 1 with the wire version and Hub key ID. Use the Hub public key received from IAM.
5. **Hub → Client:** select the matching private key and send Noise message 2 containing a fresh 32-byte challenge.
6. **Client:** finish the Noise handshake and obtain the transport ciphers and channel binding.
7. **Client:** sign the canonical channel binding, ticket hash and challenge with the device Ed25519 private key.
8. **Client → Hub:** send the ticket and signature in an encrypted `AuthenticateReq` (type 1).
9. **Hub:** verify IAM's ticket signature, expiry, service/key claims and the device signature bound to this connection.
10. **Hub → Client:** register presence in Redis, then queue an encrypted `AuthenticateResp{SUCCESS}` (type 505) before enabling business sends.
11. **Client ↔ Hub:** exchange encrypted application records using the same transport ciphers.

The Hub descriptor contains `address`, `audience`, `key_id`, Base64 `public_key`,
`protocol` and `version`. Tickets contain public authorization claims; IAM does
not derive or receive transport keys.

IAM limits connection-ticket requests with per-process token buckets, checked
after JWT verification and before the DB lookup or ticket signing. Each subject
has a burst of 20 and refills one token per second, including malformed or denied
requests. Each `(subject, device ID)` has a burst of 5 and refills one token every
5 seconds; numeric aliases such as `42` and `042` share a quota. Limits return
`429 Too Many Requests` with `Retry-After` in seconds. Clients should wait for
that interval and add retry jitter.

The limiter uses independent tables (default 16,384 subject slots and 49,152
subject/device slots), with at most 5 device buckets per subject. New devices
above that per-subject cap receive `429`; existing device buckets keep their
quotas. Exhausting either whole table returns `503 Service Unavailable` for new
keys, with a randomized `Retry-After` of 5..10 seconds. Active quotas are never
evicted to admit new keys. Fully refilled idle entries are reclaimed on subsequent
requests. A fully depleted subject or device bucket takes 20 or 25 seconds,
respectively, to refill by default if no further requests are admitted.

Configure intervals, bursts and capacities with `HTTP_TICKET_*` in
`cmd/iam/.example.env`. The legacy total capacity remains a fallback split of
1/4 subject slots and 3/4 device slots; explicit table capacities override their
respective shares. Nonpositive settings normalize to defaults in both config
loading and limiter construction.

The admission contract lives in `internal/iam/core/ticketlimit`; the GCRA,
min-heap and limiter configuration live in `internal/iam/infrastructure/ticketlimit`.
IAM bootstrap constructs the limiter and injects it into HTTP, which maps its
decisions to HTTP status and `Retry-After` without depending on the implementation.

Limits reset on restart and apply independently to each IAM replica. Use a
shared store when a fleet-wide quota is required. These buckets run after JWT
verification: configure a pre-authentication IP limiter at the trusted reverse
proxy/LB to protect JWT/JWKS work. Never trust arbitrary forwarded IP headers.

### Key ownership

| Party | Key material |
|---|---|
| IAM | Ed25519 ticket-signing seed; public Hub descriptor |
| Hub | Static X25519 service keyring; IAM Ed25519 verification public keys |
| Client | Device Ed25519 identity private key; fresh Noise ephemeral state |

### Wire format

Every frame starts with `body_length:uint16LE`, excluding the length prefix itself.

| Stage | Frame body |
|---|---|
| ClientHello | `version:uint8 || key_id:uint32LE || Noise_message_1` |
| ServerHello | `Noise_message_2` |
| Authentication and application records | `Noise_ciphertext` |

Decrypted record layout:

```text
msg_kind:uint16LE || protobuf_length:uint16LE || protobuf || expand
```

- Authenticate the exact two-byte frame-length prefix as AEAD additional data.
- Encrypt both protobuf and opaque E2EE `expand` bytes.
- Keep Noise nonce counters across authentication: AuthReq uses c2s record 0; ACK uses s2c record 0.

### Runtime behavior

- Buffer partial frames and drain complete frames from each read.
- Yield with `Wake()` when the frame/byte fairness budget is reached.
- Keep session state and ciphers on the connection's event loop; worker sends enqueue owned payloads there.
- `REQ_SEND_MSG` and `REQ_ACK_RECV_MSG` are decrypted and validated. Business routing, offline persistence and their responses are not yet wired by bootstrap.

See the [wire contract and provisioning guide](docs/protocol/noise-v2.md) and
[Kubernetes examples](deploy/k8s/README.md).

## Data Model

### Redis

| Key / Channel | Description |
|---|---|
| Connection ledger | Device → hub mapping (TCP connection tracking) |
| `pending_buckets:<deviceID>` | Set of day buckets holding pending offline messages |
| `del_hub_event` (pub/sub) | Published when a hub is removed (crash/reap) |
| `del_device_event` (pub/sub) | Published when a device is removed |

The Lua script `batchAddDevices` (Go: `BatchAddDevices`) registers devices in batches and returns each device's pending buckets. There is no add-hub broadcast event.

### ScyllaDB

```
offline_messages
  PRIMARY KEY ((device_id, day_bucket), msg_id)
  TTL 14 days, TWCS with 24h windows
  E2E-encrypted payload, 64 KiB hard cap (typically < 4 KiB)

offline_message_idempotence
  PRIMARY KEY ((device_id, channel_id), dedup_key)
  column: msg_id
  TTL 7 days
```

Clients auto-retry for ~24h with backoff. Proposed client dedup key lifespan is 3 days (max 7).

## Resilience

- **Crash detection / distributed reaping:** surviving hubs clean up the Redis entries of dead hubs.
- **Circuit breakers:** one hub-level breaker for Redis, and a separate one for ScyllaDB.
- **`IsInfraError` classifier:** only cluster/network-level errors (unavailable, overloaded, timeout, connection failure) count toward the breaker. Query, schema and auth errors do not.
- **Reconnect storms:** batched/pipelined Redis writes and a bounded pending-registration queue.
- **NATS:** `Connect()` is separated from `Start()`, with disconnect/reconnect handlers.

## Design Decisions

| Decision | Rationale |
|---|---|
| Drop NATS JetStream and Kafka | N offline devices would require N consumers |
| NATS core for online routing, ScyllaDB for offline | Separates online routing from offline storage |
| Server-assigned `MsgId` (sonyflake) | Replaces client-generated `MsgId` with client-side dedup |
| Redis standalone with replicas | No Redis Cluster |
| No server-side chat history | Clients store their own history |

## Testing

- Redis logic is tested with [miniredis](https://github.com/alicebob/miniredis).
- Run tests: `go test ./...`
- With the race detector: `go test -race ./...`
- Hub component and socket integration tests (excluded by default):
  `go test -race -tags=integration ./internal/hub/integration`
- Run all default and integration tests: `go test -race -tags=integration ./...`

Hub integration tests use a separate `integration_test` package, real Hub Noise
and connection components, and a mocked ticket verifier. Socket tests bind
`127.0.0.1:0`, set I/O deadlines, wait for shutdown, and check for goroutine leaks.
Ticket expiry behavior is checked through mocks in unit tests without waiting
for the ticket TTL.

## Getting Started

Requirements: Go (see `go.mod`), MySQL for IAM, Redis for Hub presence, and an
OIDC issuer (for example the [local Keycloak setup](deploy/auth/local/keycloak/README.md)).
NATS/ScyllaDB components are used by the separate message-routing/storage work.

Copy `.example.env` into each service's ignored `.local/.config.env`, configure
MySQL/Redis/OIDC, then provision a service key once:

```sh
go run ./scripts/hub/gen-noise-key.go \
  -address localhost:3008 -audience hub.chat -key-id 1 \
  -private-out cmd/hub/.local/service.noise-keyring.json \
  -public-out cmd/hub/.local/hub-descriptor.json
```

Set Hub's `HUB_NOISE_KEYRING_FILE` to the private output and IAM's
`HUB_NOISE_DESCRIPTOR_FILE` to the public output. Set matching key ID/audience.
IAM holds an Ed25519 signing seed; Hub holds only its public verification key.
The local IAM script initializes those signing/TLS keys when absent, synchronizes
only verification public keys to Hub, and requires the provisioned descriptor.

```sh
bash scripts/iam/run-local.sh
# In another terminal, with the configured Hub env file:
HUB_CONFIG_FILE=cmd/hub/.local/.config.env go run ./cmd/hub
```

## Roadmap

- [ ] Finish crash detection and distributed reaping
- [ ] Implement ScyllaDB offline storage and its circuit breaker
- [ ] Implement offline pull with live-message buffering
- [ ] Benchmark at 10k connections per hub

## License

Apache 2.0
