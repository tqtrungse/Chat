# Hub transport v2: Noise NK

Status: migration steps 1–9 are implemented, including signed tickets, IAM
connection-ticket issuance, the Noise adapter, channel-bound identity proof,
Session/sender ownership and TCP runtime wiring. The listener accepts only v2.
Business routing/offline handlers, production fuzz campaigns and throughput
benchmarks remain separate work; the runtime bootstrap does not enable them.

## Dependency and roles

- Pin `github.com/flynn/noise` at tag **v1.1.0**, with checksums in `go.sum`.
- Fixed suite: `Noise_NK_25519_ChaChaPoly_SHA256`; no negotiation or legacy fallback.
- Client is the initiator; Hub is the responder. NK authenticates the service to
  the Client; device Ed25519 authentication remains an application operation.
- Hub replicas share a service static X25519 keyring. A handshake generates fresh
  ephemeral material for each connection; no traffic secret is stored in a DB.
- IAM holds its own Ed25519 ticket-signing key and **only the public Hub
  descriptor**. It does not receive the Hub private key or derive transport keys.

The public descriptor refers to a service endpoint (usually TCP load balancer),
not a specific pod. IAM does not select the least-loaded pod in this design.
Routing/load scheduling is independent of the Noise identity.

## Public descriptor and trust

Public JSON, read by `cmd/iam/bootstrap/loadHubEndpoint` through
`HUB_NOISE_DESCRIPTOR_FILE`:

```json
{
  "address": "hub.example.com:9000",
  "audience": "hub.chat",
  "key_id": 7,
  "public_key": "<Base64 of 32 X25519 public-key bytes>",
  "protocol": "Noise_NK_25519_ChaChaPoly_SHA256",
  "version": 2
}
```

- Key ID: uint32, 1..4294967295. The ID is not a public key or a secret.
- Address: host:port, maximum 255 bytes, nonzero port <=65535, bracketed IPv6.
  DNS labels contain ASCII letters/digits/hyphens; no URL scheme or credentials.
- Audience: 1..128 printable non-space ASCII bytes, compared byte-for-byte.
- Public key: exactly 32 decoded bytes; reject low-order X25519 points.
- Reject unknown fields, trailing JSON and files larger than 4096 bytes.
- Client obtains this descriptor from authenticated IAM HTTPS. Never trust a
  replacement key supplied by the unauthenticated TCP peer. IAM's signed
  ticket repeats audience, key ID, public key and wire version; Hub compares
  those claims with the identity actually used in the connection.

The descriptor has no `private_key` field. A private keyring passed to the IAM
loader is rejected. IAM returns `{"ticket":"<Base64>","hub":{...}}` from
`POST /iam/v2/connection-ticket` for `{"device_id":"42"}`.

## Canonical encoding and vectors

All integers below use little endian. `F(x)` means:

```text
uint32LE(byte_length(x)) || x
```

Strings are the exact ASCII bytes, with no NUL terminator or implicit whitespace.
This document fixes the encoding rather than depending on JSON/protobuf field
ordering. `internal/shared/hubendpoint.Prologue(audience, keyID)` implements the context;
`Descriptor.Prologue()` validates a client descriptor and delegates to it:

```text
F("xxx/hub/noise/v2")
|| version:uint8
|| F("Noise_NK_25519_ChaChaPoly_SHA256")
|| F(audience)
|| hub_key_id:uint32LE
```

Supply the same Prologue to both Noise configurations. The responder static
public key is also bound by the standard NK pre-message. Address is not part of
the Prologue; the same service identity can be reached through multiple routes.

After handshake completion, copy `HandshakeState.ChannelBinding()` before
discarding handshake state. Device authentication signs these bytes directly
using Ed25519 (not Ed25519ph, and no extra prehash):

```text
F("xxx/hub/device-auth/v2")
|| F(channel_binding)        # 32 bytes with SHA-256
|| F(SHA256(raw_ticket))     # 32 bytes
|| F(challenge)              # 32 bytes
```

[`noise-v2-vectors.json`](noise-v2-vectors.json) publishes descriptor, Prologue,
an opaque ticket input, binding, challenge, identity public key and signature.
The included identity seed is deliberately public **test data**, never a service
or device production secret. The opaque ticket tests signing-input encoding;
it is not an example of the future signed-ticket wire format.

## Connection flow

```text
Client -> IAM HTTPS: Bearer token + device_id
IAM -> Client: signed connection ticket + public Hub descriptor

Client -> Hub: ClientHello with NK message 1 (empty payload)
Hub -> Client: NK message 2 (server challenge payload)
Client -> Hub: transport AuthenticateReq{ticket, signature}
Hub: verify authorization/identity, register presence
Hub -> Client: transport AuthenticateResp{SUCCESS}
Client <-> Hub: transport application records
```

Only the first two messages are Noise handshake messages. No ticket, identity
proof or business data in message 1: NK's first message is replayable and has
different forward-secrecy properties from completed transport traffic.

The server challenge is a fresh 32-byte CSPRNG value in
`NoiseServerHelloPayload { bytes challenge = 1; }`. The canonical payload is
exactly `0a 20 || challenge` (34 bytes); reject other fields/lengths for this
version. This payload is included in the final Noise handshake hash.

States: CONNECTED -> NOISE_HANDSHAKING -> AWAIT_AUTH -> VERIFYING -> REGISTERING -> ACTIVE
-> CLOSED. Handshake success is not device authorization. Do not register
presence, replace connections or process business messages before authorization.
Authorization runs on a dedicated bounded worker pool. Noise decryption and
connection transitions remain on the owning event loop. VERIFYING rejects
additional frames and retains the original authentication deadline. Worker
inputs are independently owned copies; results for closed or superseded pending
connections are discarded. Expiry is checked during ticket verification, again
after the asynchronous completion before replacing a connection, and after
presence registration before ACK. The last two checks cover separate queue/I/O
waits.


## Framing and plaintext records

Every frame has a LE16 **body** length, excluding its two-byte prefix:

| Phase/direction | Body |
|---|---|
| Initial ClientHello | `version:uint8 (=2) || hub_key_id:uint32LE || Noise_message_1` |
| ServerHello | `Noise_message_2` |
| All subsequent records | `Noise_transport_ciphertext` |

An empty-payload NK message 1 is 48 bytes; the first frame body is 53 bytes.
Message 2 with the challenge payload is 82 bytes. Select the responder key using
the initial ID, reject unknown IDs, and authenticate the chosen ID/version/audience
through the Prologue. Do not use trial decryption or suite fallback.

Transport plaintext:

```text
msg_kind:uint16LE || protobuf_length:uint16LE || protobuf || expand
```

The exact LE16 ciphertext body-length prefix is AEAD additional data. Compute
`len(plaintext)+16` before calling `CipherState.Encrypt`; pass those same two
prefix bytes to `Decrypt`. Message type, protobuf and opaque expand bytes are
all encrypted. Existing E2EE data inside expand is unchanged.

Auth request/response use MsgKind IDs **1 / 505**. The protobuf shapes are:

```protobuf
message AuthenticateReq { bytes ticket = 1; bytes signature = 2; }
message AuthenticateResp { Code code = 1; }
```

`signature` must contain 64 bytes. `ticket` must fit the bounded format below.
Other business protobuf schemas retain their current semantics.

Noise messages must be <=65535 bytes. Transport plaintext must be <=65519
bytes, including its four-byte inner header. ClientHello's five-byte header
also counts against the outer frame body limit. Validate all lengths before
allocation, decryption and encryption; never treat a partial TCP read as a
complete Noise message.

## Signed ticket v2

Ticket version and TCP wire version are separate fields, even though both use
2 for this migration. Legacy envelope version 1 is rejected.

```text
ticket_version:uint8 (=2)
|| signing_key_id:uint8
|| payload_length:uint16LE
|| payload
|| IAM_Ed25519_signature:64

payload:
  ticket_id:16
  device_id:uint64LE
  issued_at:int64LE            # Unix seconds
  expires_at:int64LE           # Unix seconds
  device_identity_pub:32
  audience_length:uint16LE
  audience:audience_length
  hub_key_id:uint32LE
  hub_public_key:32
  wire_version:uint8 (=2)
```

IAM signs `"xxx/iam/ticket-signature/v2\x00" || all_envelope_bytes_before_signature`.
Maximum audience is 128 bytes, hence maximum payload 239 and total ticket 307
bytes. Check structural bounds before parsing and verify IAM signature before
trusting claims. Require nonzero IDs, valid identity, supported wire version,
matching service/key claims and valid issue/expiry times. Initial ticket TTL is
15 seconds; retain the configured Hub clock skew (currently 5 seconds).

The ticket has no AES layer, session keys or ActivationKey. It contains an
identity snapshot, so it does not provide immediate revocation by itself.
Reusing a ticket inside its TTL is permitted only with a new channel-bound
device signature; single-use distributed consumption is not part of this step.

## Cipher ownership, limits and deadlines

- `Split` result 1 is initiator -> responder: Client send / Hub receive.
  Result 2 is Hub send / Client receive. Do not assume result 1 always means
  "my sending cipher" on both peers.
- Transport counters start at 0 after Split. AuthenticateReq consumes c2s nonce
  0; AuthResp consumes s2c nonce 0. Do not reset counters at ACTIVE.
- One connection event loop owns handshake, both cipher states and lifecycle.
  Workers enqueue immutable results rather than calling crypto concurrently.
- Validate/encode before Encrypt. Once Encrypt advances a nonce, enqueue/write
  failure closes the stream; no rollback, retry with the same state or resync.
- Release handshake references after copying binding and creating transport.
  Closing one connection never clears the shared service keyring.
- Preparation defaults: `TCP_SERVER_MAX_FRAME_BYTES=4096` (body), configurable
  512..65535, and `HUB_NOISE_HANDSHAKE_TIMEOUT=5s` (positive effective duration).
- Future runtime wiring must also keep the whole pre-auth deadline, currently
  `ACTIVATE_PREAUTH_TIMEOUT=10s`, through registration and ACK; effective phase
  deadlines never extend the whole pre-auth deadline. Authentication challenge
  expires after 5 seconds or the whole deadline, whichever is sooner.
- Preserve a pre-auth connection cap. During shutdown/draining stop new
  handshakes; live traffic keys remain per connection, in memory.

## Private key provisioning

The private keyring is strict JSON, <=64 KiB, with 1..32 entries. Each entry
contains nonzero uint32 `key_id`, Base64 `private_key` (32 bytes) and Base64
`public_key` (32 bytes). The public key must equal X25519(private, basepoint).
Duplicate IDs, missing current ID, unknown fields and trailing JSON are errors.

```sh
go run ./scripts/hub/gen-noise-key.go \
  -address hub.example.com:9000 -audience hub.chat -key-id 7 \
  -private-out cmd/hub/.local/service.noise-keyring.json \
  -public-out cmd/hub/.local/hub-descriptor.json
```

The command creates missing private directories with mode 0700 and public
directories with mode 0755, subject to the process umask. Existing directories
and their permissions are unchanged. Use separate directory trees if another
user must read the public output: a shared private parent with mode 0700 still
prevents traversal. Keep the private output directory protected from other
writers.

Each output is written to a temporary file in its destination directory, synced,
closed, then published with a hard link. The private file has mode 0600 and the
public file has mode 0644. Existing destinations, including symlinks, are never
overwritten. The destination filesystem must support hard links; unsupported
filesystems produce an error without falling back to partial writes at the final
path. Generate on a suitable filesystem before importing into Kubernetes.

Publication is atomic for each file, not for the pair, and does not guarantee
directory-entry durability after power loss. If the second publication returns
an error, the command removes its newly published private output. A forced kill
can leave staging files named `.noise-key-*` or only the complete first output;
inspect and remove incomplete provisioning results before retrying. The command
prints paths/ID, not key material. Send only the public descriptor to IAM.

Hub settings:

```text
HUB_NOISE_KEYRING_FILE=/etc/hub/noise/keyring.json
HUB_NOISE_CURRENT_KEY_ID=7
HUB_NOISE_AUDIENCE=hub.chat
TCP_SERVER_MAX_FRAME_BYTES=4096
HUB_NOISE_HANDSHAKE_TIMEOUT=5s
```

Configuration namespaces follow their owners: `CONNECTION_*` controls the
connection manager, `PRESENCE_*` controls presence synchronization, and
`ACTIVATE_*` controls activation's own settings. The former `ROUTER_*` names
are not read; update deployed configuration to the names in `cmd/hub/.example.env`.

Each component loader applies its independent defaults. Bootstrap then resolves
activation's handshake timeout from Noise and its frame-body limit from TCP.
`ACTIVATE_MAX_PREAUTH_CONNS=0` requests automatic sizing:
`max(CONNECTION_MAX_CONNS / 10, 1)`. An explicit nonzero limit is retained.
Bootstrap validates the resolved configuration before constructing the services;
constructors do not fill in missing defaults.
`ACTIVATE_VERIFY_WORKERS=0` defaults to `max(GOMAXPROCS / 2, 1)`.
`ACTIVATE_MAX_VERIFY_JOBS=0` defaults to four times that worker count and bounds
queued plus running authorization jobs. These slots remain occupied until a job
finishes even if its connection closes. Submission is nonblocking; overload
closes the requesting connection. This CPU pool is separate from the Redis and
presence worker pool.


IAM setting: `HUB_NOISE_DESCRIPTOR_FILE=/etc/iam/hub/descriptor.json`.
The Hub loader accepts owner read/write and optional group read for Kubernetes
fsGroup volumes (0400/0600/0440/0640), rejects executable/group-writable/world-
accessible keys, and follows projected Secret symlinks. Do not generate keys
inside each pod or embed them in an image. Config is loaded as a snapshot;
watching config files does not automatically rotate an in-memory keyring.

## Kubernetes and rotation

See `deploy/k8s/README.md` and example StatefulSet/Services. StatefulSet ordinals
are required by the current Hub ID derivation; `hub-0` and `hub-1` must not use
the same explicit HUB_ID. Static Noise service identity and numeric per-process
Hub ID are different concepts. Endpoint is TCP passthrough; readiness TCP checks
alone do not prove support for a Noise key.

The provisioning tool creates a new single-key keyring; it does not append to
or overwrite an existing keyring. To rotate A (ID 7) to B (ID 8):

1. Generate B into **new paths**, keeping the deployed A keyring and descriptor:

   ```sh
   go run ./scripts/hub/gen-noise-key.go \
     -address hub.example.com:9000 -audience hub.chat -key-id 8 \
     -private-out cmd/hub/.local/service-b.noise-keyring.json \
     -public-out cmd/iam/.local/hub-descriptor-b.json
   ```

2. Merge the existing keyring (A, possibly with other retained keys) and B into
   another new file. This example requires `jq`, rejects duplicate IDs and more
   than 32 entries, and never replaces the original inputs or an existing output:

   ```sh
   (
     set -eu
     umask 077
     merged=cmd/hub/.local/service-ab.noise-keyring.json
     tmp="$(mktemp cmd/hub/.local/.keyring-ab.XXXXXX)"
     trap 'rm -f "$tmp"' EXIT
     jq -s -e '
       {keys: (.[0].keys + .[1].keys)}
       | if (.keys | length) > 32
           or (.keys | map(.key_id) | unique | length) != (.keys | length)
         then error("too many keys or duplicate key_id")
         else . end
     ' cmd/hub/.local/service.noise-keyring.json \
       cmd/hub/.local/service-b.noise-keyring.json > "$tmp"
     chmod 600 "$tmp"
     ln "$tmp" "$merged"
   )
   ```

   Inputs must be validated keyrings from the provisioning tool. The Hub loader
   also validates every private/public pair when loading the merged keyring.

3. Deploy A+B on **all** Hub replicas, initially keeping
   `HUB_NOISE_CURRENT_KEY_ID=7`. Perform a controlled rollout: projected Secret
   changes alone do not reload an in-memory keyring. Verify that every replica
   accepts both IDs before proceeding. Use a reference initiator to verify both keys against each replica before
   switching IAM.
4. Set the Hub current key ID to 8 and roll out the configuration, retaining A+B.
   Then publish B's public descriptor to IAM. Keep both IDs through the whole
   rollout and for ticket TTL + skew + pre-auth grace after the last possible
   issuance of an A ticket. Include any client descriptor-cache lifetime.
5. Remove A from the keyring and roll out again when the overlap window has
   ended. Established connections keep their own cipher states; restarted pods
   require clients to reconnect and handshake anew.

Never reconstruct traffic state from a DB. Best-effort clearing in Go does not
guarantee removal of every runtime/library copy of key material.

## Interoperability and regression checks

Tests check the canonical Prologue/signature fixture, malformed descriptors,
private/public agreement, duplicate IDs/rotation, Secret symlinks/permissions,
no secret output/overwriting, and two independently loaded replicas doing NK
with a common static public key and independent transport sessions. Tests also cover signed-ticket validation, channel-bound proof replay, concurrent
sends, registration/timeout cleanup, fragmented TCP frames and fairness wake.
Production load/rate-limit tuning, fuzz campaigns and benchmark validation remain
part of the final rollout checks.

References: [Noise specification](https://noiseprotocol.org/noise.html),
[pinned flynn/noise source](https://github.com/flynn/noise/tree/v1.1.0).

## Regenerating API bindings

From the repository root, with `protoc`, `protoc-gen-go`, `protoc-gen-validate`,
`easyjson` and `mockgen` installed:

```sh
bash scripts/hub/gen-proto.sh
easyjson -all api/iam/v2/json/message.go
bash scripts/hub/gen-mock.sh
bash scripts/iam/gen-mock.sh
```

The protobuf generation script resolves the validate schema from the module
version pinned in `go.mod`, so public schema generation needs no Buf registry
fetch. Generated files are committed alongside their schema sources.

Presence transitions are serialized to backend completion per device, including
close/reconnect during registration. The per-device pending Add backlog is
bounded at 64; final Del cleanup remains admissible. Idle queues are released.

Validation on 24/10/2026: IAM/Hub builds, race tests across shared/IAM/Hub/bootstrap
and provisioning packages, HTTPS JWT issuance and an actual nio Noise listener
pass. `go test ./...` reports the pre-existing environment failure in
`pkg/nio/TestServeMulticast`; the full suite passes with only that test excluded
using `go test -skip '^TestServeMulticast$' -timeout=3m ./...`.
