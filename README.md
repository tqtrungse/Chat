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
| Connection layer (gnet v2) | 🟢 Implemented | TCP connection handling |
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
   Device ─TCP─▶│  Hub (gnet)  │◀──────────────┐
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
| **Connection layer** (gnet v2) | Accepts and manages device TCP connections |
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

## Protocol (current)

The custom packet headers and transcript fields use little-endian integers. Protobuf payloads use the standard Protocol Buffers wire encoding. This section describes the current v1 formats and calls out paths that are not fully wired yet.

### TCP framing

TCP is a byte stream, so each client-to-hub packet starts with a two-byte little-endian frame length. The length counts the packet body and excludes the two-byte length prefix:

```text
frame_length: uint16 LE | packet body: frame_length bytes
```

The packet body starts with a two-byte little-endian packet type. HMAC packets and the server's activation challenge then have a two-byte little-endian protobuf length. The inbound activation request and activation proof are exceptions: after the packet type, their protobuf message follows directly, with no inner length. The server reads the outer frame length before dispatching the packet type.

### IAM key exchange

The client first calls `POST /iam/v1/exchange-key` over HTTPS with an OIDC access token:

```http
Authorization: Bearer <access-token>
Content-Type: application/json
```

```json
{
  "device_id": 42,
  "client_pub_key": "<base64-encoded 32-byte X25519 public key>"
}
```

The IAM service verifies the token against its configured issuer, audience, and JWKS, then checks that the token subject owns the active device. A successful response contains `server_pub_key` and `ticket`, both Base64-encoded. The server returns no derived session keys; the client derives the same keys locally.

```json
{
  "server_pub_key": "<base64-encoded 32-byte X25519 public key>",
  "ticket": "<base64-encoded sealed ticket>"
}
```

For each request, both sides derive:

```text
raw = X25519(client_private_key, server_pub_key)
salt = client_pub_key[32] || server_pub_key[32]
prk = HKDF-Extract(SHA-256, raw, salt)
info = "xxx/iam/exchange-key/v1" || device_id_LE64 || "/" || label
key = HKDF-Expand(SHA-256, prk, info, 32)
```

| Label | Client use | Hub use |
|---|---|---|
| `c2s/enc` | Encrypt client-to-hub packets | Receive/decrypt |
| `s2c/enc` | Decrypt hub-to-client packets | Send/encrypt |
| `c2s/mac` | Authenticate client-to-hub packets | Verify |
| `s2c/mac` | Verify hub-to-client packets | Authenticate |
| `c2s/act` | Prove possession during activation | Verify activation proof |

The ticket is an AES-256-GCM sealed value containing the device ID, expiry, four session keys, activation key, and device identity public key. Its binary form is `key_id (1 byte) | nonce (12 bytes) | ciphertext and tag (224 bytes)`, 237 bytes total before Base64 encoding. The device ID encoded as little-endian uint64 is also used as GCM additional authenticated data. Tickets expire after 15 seconds; the hub allows 5 seconds of clock skew when checking expiry.

### TCP activation

After key exchange, the client opens a TCP connection and sends this framed activation request. The activation packet body has no protobuf-length field:

```text
packet_type: uint16 LE = REQ_ACTIVE_CONN
protobuf: ActiveConnReq {
  token:     8-byte device ID, uint64 LE
  sign:      Ed25519(device_identity_private_key, ticket)
  ticket:    ticket returned by IAM
}
```

The hub opens the ticket for the device ID and verifies `sign` using the identity public key inside the ticket. It does not activate the connection yet. Instead, it returns this packet body, without an outer frame-length prefix:

```text
packet_type: uint16 LE = RESP_ACTIVE_CONN_CHALLENGE
protobuf_size: uint16 LE
protobuf: ActiveConnChallengeResp{nonce: 32 fresh bytes}
```

The challenge is tied to that TCP connection and must be answered within 5 seconds.

The client computes the transcript and proof as follows. Each transcript field is prefixed with its four-byte little-endian length:

```text
transcript = SHA-256(
  "xxx/hub/activate/v1\0" ||
  LE32(len(token)) || token ||
  LE32(len(ticket)) || ticket ||
  LE32(len(sign)) || sign
)

proof = HMAC-SHA-256(
  activation_key,
  "xxx/hub/activate-proof/v1\0" || transcript || nonce
)
```

It sends a framed `REQ_ACTIVE_CONN_PROOF` packet whose body is `packet_type: uint16 LE | protobuf: ActiveConnProofReq{proof}`; this packet also has no inner protobuf-length field. The hub compares the proof in constant time and accepts it only for the pending challenge on the same connection. After the device is registered in Redis, the hub sends `ActiveConnResp{code: SUCCESS}`.

### Session packet bodies

The codec defines two session packet formats. In both formats, the packet type is a two-byte little-endian value.

**HMAC packet** — protobuf is visible; the HMAC provides integrity and authentication, not confidentiality:

```text
packet_type: uint16 LE | protobuf_size: uint16 LE | protobuf |
expand | HMAC-SHA-256: 32 bytes
```

The MAC covers the packet type, protobuf length, protobuf bytes, and `expand` bytes. The direction-specific `c2s/mac` or `s2c/mac` key is used.

**Encrypted packet** — protobuf is encrypted with AES-256-GCM; `expand` is transmitted in clear and supplied as GCM additional authenticated data:

```text
packet_type: uint16 LE | nonce: 12 bytes | cipher_size: uint16 LE |
ciphertext_and_GCM_tag | expand
```

`cipher_size` includes the 16-byte GCM tag. The encrypted message request currently contains protobuf metadata (`channel_id`, `dedup_key`, and recipient device IDs); its `expand` carries the encrypted message material described beside `SendMsgReq` in [`message.proto`](api/hub/v1/proto/src/pub/message.proto).

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

## Getting Started

> TODO: adjust to match the repository.

```bash
git clone <repo-url>
cd hub
go build ./...
./hub --config config.yaml
```

Requirements: Go (see `go.mod`), Redis, NATS, ScyllaDB.

## Roadmap

- [ ] Finish crash detection and distributed reaping
- [ ] Implement ScyllaDB offline storage and its circuit breaker
- [ ] Implement offline pull with live-message buffering
- [ ] Benchmark at 10k connections per hub

## License

Apache 2.0
