# XXX Chatting Backend

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
- [ ] Finish router and NATS lifecycle
- [ ] Implement ScyllaDB offline storage and its circuit breaker
- [ ] Implement offline pull with live-message buffering
- [ ] Benchmark at 50k connections per hub
- [ ] Decide on Redis vs DragonflyDB vs Valkey

## License

Apache 2.0
