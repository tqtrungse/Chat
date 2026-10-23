# Kế hoạch chuyển transport sang Noise NK

Ngày: 23/10/2026. Cập nhật: 24/10/2026. Trạng thái: đã triển khai bước 1–9.
Bước 3–4 được thực hiện cùng 5–9 vì ticket/API IAM là dependency của activation.

Kết quả: `flynn/noise v1.1.0`, service keyring/provisioning, ticket v2 chỉ ký,
API `/iam/v2/connection-ticket`, Noise responder/channel, codec plaintext/frame,
identity proof gắn channel binding, Session/sender trên event loop, ACK gate,
TCP drain/fairness wake, bootstrap và scripts/env/docs tương ứng. Protocol cũ
không còn listener/fallback trong runtime mới. Public descriptor không chứa bí mật.

Kubernetes dùng StatefulSet vì `pkg/k8s/hostname.go` tạo Hub ID từ ordinal.
IAM trả endpoint dịch vụ chung; L4 load balancer chịu trách nhiệm chọn pod.
Rotation phải nạp old+new key trên mọi replica trước khi IAM quảng bá key mới.

Đã thêm reference initiator và regression/integration tests cho ticket, proof,
Noise replay/direction/nonce, concurrent sends, lifecycle registration/timeout,
TCP fragmentation/drain/Wake, HTTPS JWT/API và một listener nio thực.
Bước 10 còn chiến dịch fuzz, benchmark/load test, và rollout production thực tế.
Không sửa các secrets trong `.local` và chưa apply Kubernetes manifests.

Kiểm chứng 24/10/2026: build IAM/Hub pass; race tests shared/IAM/Hub/bootstrap
và provisioning pass; test HTTPS JWT và listener nio thực pass. Toàn repo pass
với `go test -skip '^TestServeMulticast$' -timeout=3m ./...` ngoài sandbox.
Chạy không skip chỉ còn `pkg/nio/TestServeMulticast` lỗi multicast của môi trường.
Hàng đợi presence theo device có giới hạn backlog và giữ thứ tự hoàn tất Add/Del
qua close/reconnect; đã có regression test riêng.

Các đường dẫn trong tài liệu tính từ thư mục gốc repository `xxx`.

## 1. Phạm vi và kết quả cần đạt

- Dùng `github.com/flynn/noise`, suite cố định `Noise_NK_25519_ChaChaPoly_SHA256`.
- Client là initiator; Hub là responder. Client nhận Hub public key đáng tin cậy qua HTTPS của IAM.
- IAM chỉ xác thực quyền và ký connection ticket; bỏ ECDH/HKDF và mọi khóa đối xứng trong ticket.
- Giữ device identity Ed25519, IAM ticket signing Ed25519, OIDC, kiểm tra ownership/state và presence Redis.
- Mọi packet sau handshake, kể cả authentication và ACK, được Noise bảo vệ.
- Các replica Hub trong cùng dịch vụ dùng chung static keyring; ephemeral key và cipher states riêng theo connection.
- Không tạo bảng chứa transport/session keys. Không thay nội dung E2EE trong `SendData.Expand`.
- Mặc định cắt chuyển đồng bộ trên môi trường phát triển; không tự động fallback sang protocol cũ.

Hai quyết định để tránh mở rộng phạm vi:

1. Ticket chứa snapshot `IdentityPub` do IAM ký như hiện tại. Hub chưa cần thêm MySQL dependency. Thu hồi device/identity tức thời cần cơ chế riêng; ticket ngắn hạn không tự cung cấp tính chất này.
2. Ticket được dùng lại trong TTL nếu Client chứng minh identity trên một Noise connection mới. Chưa thêm yêu cầu single-use toàn cụm/Redis consume. `ticket_id` phục vụ nhận diện/audit, không tự chống replay.

## 2. Hiện trạng trước migration (đối chiếu lịch sử)

| Khu vực | Hiện trạng |
|---|---|
| TCP | `internal/hub/adapter/tcp/server.go` dùng `xxx/pkg/nio`, không import gnet trực tiếp. |
| IAM | `keyExchanger.Exchange()` tạo ECDH/HKDF qua `KeyDeriver.DeriveKeys()`. |
| Khóa | Bốn khóa `RecvEnc/SendEnc/RecvMac/SendMac` và một `ActivationKey`. |
| Ticket | AES-GCM seal rồi IAM Ed25519 ký; không phải lưu transport keys vào DB device. |
| Activation | `Activate()` verify ticket + chữ ký ticket, gửi nonce; `Prove()` verify HMAC rồi derive connection keys. |
| Codec | `Encode/Decode` dùng HMAC; `EncodeS/DecodeS` dùng AES-GCM và AAD tự xây. |
| Connection | Context đổi từ `pendingActivation` sang `connection.Session`; gửi từ callback/worker qua AsyncWrite. |
| TCP buffering | `OnTraffic()` chỉ xử lý một frame; frame còn trong user-space có thể bị treo. |
| Business handlers | `REQ_SEND_MSG` / `REQ_ACK_RECV_MSG` mới kiểm tra session, chưa nối đầy đủ use case; bootstrap truyền sender `nil`. |
| Deployment | Có local scripts/env; chưa có Kubernetes manifests Hub trong repo. |
| Client | Không thấy client app trong repo. Cần reference client/test peer để xác nhận wire contract. |

Lưu ý lịch sử: `SignedVersion` trong source cũ là `1`, trong khi README mô tả envelope version `2`. Source là căn cứ cho migration; ticket mới chọn version mới và cập nhật tài liệu cho thống nhất.

## 3. Flow đích và wire contract

```text
Client -> IAM HTTPS: Bearer token + device_id
IAM: verify JWT, ownership, active, identity public key
IAM -> Client: signed ticket + hub_address + hub_key_id + hub_public_key

Client -> Hub: ClientHello header + Noise NK message 1, payload rỗng
Hub -> Client: Noise NK message 2, payload chứa challenge ngẫu nhiên 32 byte

Client: hoàn tất handshake, lấy ChannelBinding và transport cipher states
Client -> Hub: Noise transport AuthReq{ticket, identity_signature}
Hub: decrypt, verify IAM signature/claims và device signature
Hub: đăng ký presence, xử lý replacement connection
Hub -> Client: Noise transport AuthResp{SUCCESS}
Client <-> Hub: Noise transport application records
```

Challenge đặt trong message thứ hai của handshake để không thêm một lượt challenge request/response riêng. Chỉ gọi/lưu bản copy `ChannelBinding()` sau khi handshake hoàn tất. Không gửi ticket, device signature hay business data trong message đầu NK.

Device signature ký message có encoding xác định, domain riêng:

```text
Encode(
  "xxx/hub/device-auth/v2",
  channel_binding,
  SHA256(raw_signed_ticket),
  challenge
)
```

`Encode` dùng length-prefix rõ ràng cho trường biến độ dài; ký và verify đúng cùng byte sequence. Đây là proof identity ở lớp ứng dụng; bỏ hoàn toàn HMAC activation proof và activation key cũ.

Ticket payload mới gồm: `ticket_id`, `device_id`, `issued_at`, `expires_at`, `identity_pub`, `audience`, `hub_key_id`, `hub_public_key`, protocol version. Không chứa private key, traffic key hay activation key. Giới hạn kích thước audience/payload trước khi parse/cấp phát. Response phải nhất quán với các trường được ký.

Framing đề xuất:

```text
ClientHello đầu tiên:
  uint16LE(body_length) | version:uint8 | hub_key_id:uint32LE | Noise_message_1

ServerHello:
  uint16LE(body_length) | Noise_message_2

Transport record:
  uint16LE(ciphertext_length) | Noise_ciphertext

Plaintext bên trong transport:
  msg_kind:uint16LE | protobuf_length:uint16LE | protobuf | expand
```

- Suite cố định, không negotiation/fallback. Prologue encode domain `xxx/hub/noise/v2`, wire version, service audience và hub key ID. Public key Hub được Noise pre-message ràng buộc sẵn.
- `msg_kind`, protobuf và `expand` đều nằm trong ciphertext. `expand` vẫn là nội dung opaque đối với lớp transport.
- AAD cho transport là hai byte length prefix chính xác trên wire; tính chiều dài trước Encrypt và dùng lại khi Decrypt.
- Giới hạn Noise message tối đa 65535 byte; transport plaintext tối đa 65519 byte với tag 16 byte, gồm cả inner header. ClientHello phải tính thêm overhead header ngoài Noise. Giới hạn cấu hình có thể nhỏ hơn.
- Tách `MaxFrameBytes` khỏi read chunk/buffer capacity; giữ default frame limit hiện tại nếu muốn tránh đổi giới hạn sử dụng trong cùng đợt.
- Không reset nonce sau authentication. Với flow này, AuthReq là c2s transport record 0; ACK là s2c transport record 0. Handshake có cipher state riêng do thư viện quản lý.
- Theo thứ tự Split của Noise: cipher thứ nhất c2s, thứ hai s2c khi Client là initiator. Hub dùng `recv=cs1`, `send=cs2`; Client ngược lại. Test mapping này bằng hai peer thật.

## 4. Thứ tự thực hiện

### Bước 1 — Chốt đặc tả và dependency

Sửa `go.mod`, `go.sum`: thêm một phiên bản/commit `github.com/flynn/noise` đã kiểm tra, pin rõ ràng. Không mặc định lấy moving branch.

Thêm `docs/protocol/noise-v2.md` từ contract ở trên: byte layout, domain/prologue, claims, public key trust, timeout, size limits, thứ tự cipher và rollout. Thêm vector cho signature input/encoding để client khác ngôn ngữ có thể triển khai.

Điều kiện xong: có một contract dùng chung cho IAM/Hub/reference client; phân biệt ticket version và wire version.

### Bước 2 — Khóa Hub theo danh tính dịch vụ

Thêm:

| File mới | Nội dung/hàm dự kiến |
|---|---|
| `internal/shared/hubendpoint/descriptor.go` | Descriptor public: address, audience, key ID, public key, protocol. |
| `internal/hub/infrastructure/noise/config.go`, `keyring.go` | `Config.Load()` đọc Noise config; `LoadKeyring()` đọc keyring từ file secret, derive/validate public key, reject duplicate/invalid IDs. |
| `cmd/iam/bootstrap/hub.go` | `loadHubEndpoint()`: load descriptor public hiện hành; không load Hub private key. |
| `scripts/hub/gen-noise-key.go` | Công cụ provisioning X25519; output private file quyền hạn chế và public descriptor. Không tự chạy mỗi lần pod khởi động. |

Sửa `cmd/hub/.example.env`, `cmd/iam/.example.env`, thêm config cho public endpoint, audience, current key ID, Hub private keyring file và frame/handshake limits. Tên config chốt trong đặc tả; không nhét secret thật vào ví dụ.

Kubernetes: đã thêm `deploy/k8s/hub-statefulset.example.yaml`, `hub-service.example.yaml` và README provisioning. Mount Secret có sẵn vào tất cả replica; cấu hình TCP passthrough. IAM nhận public descriptor tương ứng. Không tạo registry theo pod hay migration DB cho key. Chưa apply manifests lên cluster.

Điều kiện xong: hai replica độc lập handshake được với cùng public descriptor; session keys khác nhau.

### Bước 3 — Ticket chỉ ký, không mã hóa

| File hiện có | Bỏ/sửa cụ thể |
|---|---|
| `internal/shared/ticket/info.go` | Bỏ `Keys`, `ActivationKey`; thêm claims ở mục 3. Sửa `Clear()` thành cleanup theo dữ liệu mới nếu còn cần. |
| `internal/shared/ticket/payload.go` | Viết lại `marshal/unmarshal` thành codec có bounds checking và trả error; bỏ `aad(deviceID)`. |
| `internal/shared/ticket/envelope.go` | Version/domain mới; envelope ký trên header + plaintext claims. Sửa constants size vì không còn sealed payload cố định. |
| `internal/shared/ticket/errors.go` | Bỏ error chỉ thuộc Sealer; thêm lỗi version/claims/length phù hợp. |
| `internal/iam/infrastructure/crypto/ticket_signer.go` | Bỏ field/constructor dependency `sealer`; đổi `Seal()` thành `Sign()`. Giữ IAM Ed25519 signing key ID. |
| `internal/hub/infrastructure/crypto/ticket_verifier.go` | Bỏ dependency `sealer`; đổi `Open(deviceID, wire)` thành `Verify(wire)`; verify signature trước khi tin claims. Giữ `CheckExpiry()`, thêm kiểm tra audience/protocol/Hub key binding. |
| `cmd/iam/bootstrap/ticket.go` | `loadTicketSigner()` bỏ load AES keyring; giữ `parseTicketSigningPrivate()`. |
| `cmd/hub/bootstrap/ticket.go` | `loadTicketVerifier()` bỏ AES keyring; giữ `parseTicketSigningPublicKeys()` và clock skew. |

Xóa sau khi không còn reference:

- `internal/shared/ticket/cipher.go` và `cipher_test.go`: `Sealer`, `NewSealer`, `Seal`, `Open`.
- `internal/shared/ticket/utils.go` và `utils_test.go`: `ParseTicketEncryptKeys` chỉ phục vụ AES keyring cũ.

Viết lại `descriptor_test.go`, `payload_test.go`, IAM signer tests, Hub verifier tests và hai bootstrap ticket test files. Giữ kiểm tra signature tamper, unknown key ID, rotation, expiry và input ownership; thay roundtrip AES bằng signed-claims roundtrip.

Điều kiện xong: ticket IAM tạo được Hub verify mà Hub chỉ có IAM public signing keys; ticket không chứa bí mật transport.

### Bước 4 — IAM chuyển exchange-key thành cấp ticket

Đổi folder `internal/iam/application/keyexchange/` thành `internal/iam/application/connectionticket/`.

| File/hàm | Thay đổi |
|---|---|
| `usecase.go`: `keyExchanger`, `New()`, `Exchange()` | Đổi thành `ticketSigner`/tên tương đương và `Issue()`. Bỏ KeyDeriver. Giữ JWT subject, ownership, active và `checkIdentityPub()`. Sinh ticket ID/expiry, lấy descriptor public, ký ticket. |
| `ports.go`: `TicketIssuer.Seal()` | Đổi port thành `TicketSigner.Sign()` và tên file phù hợp. |
| `device_meta_reader.go` | Giữ port/model; chỉ đổi package/import. |
| `errors.go`, tests, mocks | Đổi tên và loại bỏ cases derive/clear session keys; giữ toàn bộ cases authorization và DB errors. |
| `api/iam/v2/json/message.go` | `ExchangeKeyReq/Resp` -> `ConnectionTicketReq/Resp`; request bỏ `ClientPubKey`; response bỏ ephemeral `ServerPubKey`, thêm Hub descriptor. Public key JSON dùng Base64 bytes và kiểm tra đúng 32 byte. |
| `api/iam/v2/json/message_easyjson.go` | Regenerate từ source DTO; không sửa tay generated code. |
| `internal/iam/adapter/http/exchange_key.go` | Đổi tên `conn_ticket.go`; `exchangeKey()` -> `issueConnectionTicket()`, bỏ contract ECDH, bỏ cleanup `req.ClientPubKey`. |
| `internal/iam/adapter/http/server.go` | Inject ticket use case; route mới `POST /iam/v2/connection-ticket`. Giữ HTTPS, Bearer auth, no-store, body limit và error mapping. |
| `internal/iam/infrastructure/mysql/device_meta_reader.go` và test | Chỉ đổi import/type package; giữ query ownership/state/identity. |
| `cmd/iam/bootstrap/bootstrap.go`: `Run()` | Inject signer + public descriptor; bỏ `crypto.NewHkdfDeriver()`. |

Xóa:

- `internal/iam/application/keyexchange/key_deriver.go` (`KeyDeriver`, `DerivedKeys`).
- `internal/iam/application/keyexchange/mocks/key_deriver_mock.go`.
- `internal/iam/infrastructure/crypto/hkdf_deriver.go`, `hkdf_deriver_test.go` (`NewHkdfDeriver`, `DeriveKeys`, `withLabel`).

Các file còn lại của `keyexchange` được move/refactor, không xóa logic authorization cùng cả folder. Giữ schema `devices.identity_pub`; không xóa `peer_pub` vì đó có thể là khóa của chức năng khác.

Điều kiện xong: IAM hoàn toàn không tham gia ECDH, không sinh/lưu traffic keys; integration test HTTPS trả đúng ticket/Hub descriptor.

### Bước 5 — Adapter Noise có state riêng từng connection

Thêm:

| File mới | Nội dung/hàm dự kiến |
|---|---|
| `internal/hub/core/transport/ports.go` | Interface nhỏ cho handshake/channel, không export raw keys. Tránh ép interface stateless `Seal(key, nonce)` cũ vào Noise. |
| `internal/hub/infrastructure/noise/factory.go` | `NewFactory()`, `NewResponder(keyID)`: cấu hình NK, suite cố định, static key và prologue. |
| `internal/hub/infrastructure/noise/handshake.go` | `ReadClientHello()`, `WriteServerHello()`, copy channel binding sau complete. |
| `internal/hub/infrastructure/noise/channel.go` | Giữ hai `*noise.CipherState`; `EncryptRecord()`/`DecryptRecord()`, không cho set/reset nonce hoặc export key. |
| `internal/hub/infrastructure/noise/*_test.go` | Hai peer thực: mapping c2s/s2c, sai static key/prologue, tamper, replay, giới hạn và nonce progression. |

Không dùng `UnsafeKey()`, `UnsafeNewCipherState()`, `SetNonce()` để phục hồi protocol cũ. Handshake/cipher state không chia sẻ giữa connection và không copy theo giá trị.

Lifecycle chỉ clear những buffer wrapper sở hữu, bỏ references handshake khi không cần. Không hứa secure zeroization tuyệt đối cho trạng thái ẩn trong thư viện Go; không clear static key dùng chung khi đóng một connection.

Điều kiện xong: responder độc lập đọc/ghi được NK với reference initiator và tạo đúng transport channel.

### Bước 6 — Codec chỉ serialize payload và đóng frame

| File | Sửa cụ thể |
|---|---|
| `internal/hub/core/protocol/encoder.go` | Bỏ implementations `EncodeHandshake()`, `Encode()` HMAC, `EncodeS()` AES-GCM. Thay bằng `EncodePayload(msgType, msg, expand)` và frame encoding không làm crypto. |
| `internal/hub/core/protocol/decoder.go` | Bỏ `DecodeActivationPacket`, `DecodeActivationProofPack`, `Decode`, `DecodeS`; thay bằng `DecodePayload(plaintext)` sau Noise decrypt. |
| `internal/hub/core/protocol/frame.go` | Thêm frame read/length validation dùng chung cho hello/Noise records; giữ LE16, kiểm tra overflow/size trước allocation. |
| `internal/hub/core/protocol/errors.go` | Bỏ lỗi chỉ dành AAD/custom keys; giữ lỗi frame, protobuf, message type và size. |
| `api/hub/v2/proto/src/pub/message.proto` | Thêm `NoiseServerHelloPayload{challenge}`, `AuthenticateReq{ticket, signature}`, `AuthenticateResp{code}`. Dùng MsgKind ID 1/505 cho auth request/response theo schema hiện tại. Bỏ messages/comments mô tả HMAC activation và AES transport. Giữ business messages. |
| `api/hub/v2/proto/gen/pub/message.pb.go`, `message.pb.validate.go` | Regenerate từ proto. Không chỉnh generated code tay. |

Xóa:

- `internal/hub/core/protocol/aad.go`, `aad_test.go`: `seqBytes`, `securePacketAAD`, các domain/seq layout cũ.
- `internal/hub/core/protocol/ports.go`: interface `SecureChannel` stateless cũ.
- `internal/hub/core/protocol/mocks/ports_mock.go`.
- `internal/hub/core/protocol/protocoltest/secure_channel.go`; bỏ folder nếu rỗng.
- `internal/hub/infrastructure/crypto/aesgcm_channel.go`, `aesgcm_channel_test.go`.

Viết lại `encoder_test.go`, `decoder_test.go`: giữ roundtrip message/expand, bounds, malformed protobuf; crypto/replay cases chuyển sang test Noise/integration. Transport mã hóa cả expand nhưng không thay đổi nội dung E2EE bên trong.

Điều kiện xong: chỉ còn một định dạng transport được bảo vệ; không có cờ chọn plaintext/HMAC/AES path.

### Bước 7 — Activation là kiểm tra identity trên Noise channel

| File | Sửa cụ thể |
|---|---|
| `internal/hub/core/activation/service.go` | `Prepare(token, ticket, sign)` -> `VerifyAuthorization(ticket, signature, binding, challenge, hubContext)`; verify ticket, expiry, audience/key binding và Ed25519 signature mới. Bỏ token deviceID LE riêng: lấy ID từ authenticated ticket. |
| `internal/hub/core/activation/ports.go` | `TicketVerifier.Open` -> `Verify`; giữ port `Signer.Verify()`. |
| `internal/hub/core/activation/handshake.go` | Xóa bản cũ chứa `Transcript`, `Proof`, `writeField`. |
| `internal/shared/hubauth/proof.go` (mới) | `SigningMessage(binding, rawTicket, challenge)`: domain/encoding dùng chung với test/reference client. Chỉ chuẩn hóa dữ liệu ký, không xây key schedule mới. |
| `internal/hub/application/activateconn/usecase.go` | Giữ `Admit`, `Deactivate`, `Tick`, capacity/timeout. Thay `Activate/Prove` bằng `Handshake/Authenticate`. Sinh challenge cho M2; hoàn tất Noise vẫn ở trạng thái chưa authorized. |
| `internal/hub/application/activateconn/pending_activation.go` | Bỏ ticket keys và transcript/HMAC state; giữ handshake/channel pointer, binding copy, challenge, deadlines, owner/release-once. |
| `internal/hub/application/activateconn/ports.go` | Sửa ActivationService/ConnManager signatures; truyền ownership của transport channel thay `*sharedsession.Keys`. |
| `internal/hub/application/activateconn/config.go` | Timeout bao phủ accept -> handshake -> auth -> Redis registration/ACK; vẫn giới hạn MaxPreAuthConns. |
| `internal/hub/infrastructure/crypto/ed25519_signer.go` | Giữ `NewEd25519Signer()/Verify()`, dùng signing message mới. |

State machine:

```text
CONNECTED -> NOISE_HANDSHAKING -> AWAIT_AUTH
          -> REGISTERING -> ACTIVE -> CLOSED
```

- Trước ACTIVE chỉ chấp nhận packet đúng phase; không có business side effect.
- Không dừng timeout/release preauth slot ngay khi Noise complete; phải xử lý cả Redis registration bị chậm và cleanup sau khi context đã đổi thành Session.
- Chuyển cùng channel pointer vào Session, không tạo lại CipherState/counter sau auth.
- `pending.clear()` phải phân biệt đã transfer ownership: không xóa channel đang được Session sử dụng.
- Kiểm tra expiry lại khi hoàn tất đăng ký trước ACK; nếu đã hết hạn, close và gỡ presence tương ứng.
- Nếu callback registration tới sau close/replacement, bỏ callback; không kích hoạt hoặc gửi ACK cho connection mới của cùng device.

Xóa `internal/hub/core/activation/keys.go`, `keys_test.go`: `DeriveConnKeys`, `deriveConnKey` không còn cần.

Viết lại `handshake_test.go` thành tests của `hubauth/proof.go`; thay `replay_test.go` bằng identity-proof/ciphertext replay giữa hai Noise connections. Viết lại `service_test.go` và `activateconn/usecase_test.go` nhưng giữ coverage admission/rejection/timeout/presence/cleanup.

Điều kiện xong: client đúng identity mới active; sao chép ticket/signature/ciphertext từ connection khác đều không active được.

### Bước 8 — Session, sender và event-loop ownership

| File | Sửa cụ thể |
|---|---|
| `internal/hub/core/connection/session.go` | Bỏ `keys`, `recvSeq`, `sendSeq`, `RecvSeq()`, `AdvanceRecv()`, `WithSendSeq()`. Giữ device/state/heartbeat; thêm channel pointer và REGISTERING phase. Noise giữ nonce. |
| `internal/hub/core/connection/manager.go` | `ActivateConn()` nhận channel ownership; thêm `CompleteActivation()` để enqueue ACK trước rồi mới mở business send. Bỏ tham số `secure bool` khỏi `AsyncSend/AsyncSendConn`. |
| `internal/hub/core/connection/sender.go` | Serialize business payload -> Noise Encrypt -> frame -> write; bỏ nhánh Encode/EncodeS. `AsyncSend*` enqueue một tác vụ lên `conn.EventLoop().Execute()`. |
| `internal/hub/core/connection/registry.go` | Giữ capacity/replacement/lookup; cập nhật REGISTERING/ACTIVE/close để callback cũ không ảnh hưởng session thay thế. |
| `internal/hub/adapter/tcp/server.go` | `OnOpen` admit; `OnTraffic` đọc theo phase, decrypt trước dispatch, loop qua complete frames; `OnClose` cleanup pending/session; `OnTick` giữ lifecycle. |
| `internal/hub/adapter/tcp/config.go` | Tách MaxFrameBytes và ngân sách frames/bytes mỗi callback khỏi read-buffer size. |
| `internal/hub/application/sendmsg/usecase.go` | Dọn signature/comment tham chiếu codec cũ; contract nhận message/owned payload đã decrypt. Chưa xây toàn bộ routing/offline business logic trong migration này. |

Chọn event loop của connection làm chủ duy nhất handshake + cả hai cipher states + thay đổi lifecycle. Worker/callback Redis chỉ gửi kết quả qua `EventLoop.Execute`; kiểm tra lại connection/session hiện hành khi tác vụ thật sự chạy.

Các điều kiện cần giữ:

1. Encode/validate size trước Encrypt. Sau Encrypt, nonce đã tăng: nếu enqueue/write thất bại phải đóng stream, không rollback nonce hay gửi packet tiếp theo.
2. Cả auth, ACK, live/offline business send đi cùng cơ chế thứ tự; ACK phải đứng trước business record.
3. `AsyncSend*` trả thành công nghĩa là tác vụ đã được nhận; lỗi phát sinh sau đó phải xử lý bằng callback/close, không giả vờ đó là đồng bộ write success.
4. Payload worker gửi phải được copy/serialize thành buffer có ownership rõ ràng trước enqueue. Không giữ slice alias nio inbound hoặc caller-owned mutable protobuf.
5. Close/cleanup không chạy song song với Encrypt/Decrypt. Không tái dùng channel đã close/replaced.
6. `OnTraffic()` drain complete frames tới khi thiếu dữ liệu. Nếu chạm fairness budget mà vẫn còn frame, schedule `Wake()` để không treo user-space buffer.
7. Read chưa đủ frame không gọi Noise, không tăng nonce; malformed length/tag/error phải close, không thử resync.

Không cần đổi `pkg/nio` production API: hiện đã có `Conn.EventLoop().Execute()` và `Wake()`. Chỉ bổ sung regression tests ở đây nếu integration phát hiện thiếu hành vi cần thiết.

Xóa `internal/shared/session/keys.go`; bỏ folder `internal/shared/session/` nếu rỗng và không còn reference.

Điều kiện xong: concurrent sends có thứ tự decrypt chính xác, không data race; nhiều frame trong một read không bị treo; close/replacement không reuse cipher state.

### Bước 9 — Wiring, scripts, generation và deployment

Sửa:

- `cmd/hub/bootstrap/bootstrap.go`, `Run()`: bỏ `NewAesGcmChannel`, inject Noise factory/keyring, codec mới, activation service và sender mới.
- `scripts/iam/gen-mock.sh`: bỏ KeyDeriver; đổi source paths/package connectionticket.
- `scripts/hub/gen-mock.sh`: bỏ SecureChannel mock; regenerate activation/connection ports thay đổi.
- `scripts/iam/run-local.sh`: bỏ sinh/đồng bộ `TICKET_ENCRYPT_*`, giữ IAM signing key; nối public Noise descriptor từ provisioning Hub. Không cấp Hub private key cho IAM.
- Hai `.example.env`: bỏ `TICKET_ENCRYPT_CURRENT_KEY_ID`/`TICKET_ENCRYPT_KEYS`; thêm cấu hình Noise/public endpoint. Không sửa secret thực trong `.local` khi chưa triển khai.
- `.gitignore`: bảo đảm bỏ qua `.local` của cả Hub/IAM và file private key do provisioning tạo, giữ example manifests.
- `README.md`: thay toàn bộ mục custom key exchange/activation/HMAC/AES transport, sửa trạng thái gnet/nio và giới hạn handler đúng thực tế.
- `deploy/auth/local/keycloak/README.md`: curl endpoint connection-ticket mới; request body không còn X25519 client pub.

Regenerate easyjson/protobuf/mocks bằng toolchain của repo; chốt executable cho protoc-gen-validate trong `buf.gen.yaml` nếu generation hiện tại không chạy được. Không sửa tay file generated và không thay schemas NATS/private nếu không cần.

Key rotation trên Kubernetes:

1. Cấp keyring `{old, new}` cho mọi replica Hub; readiness xác nhận đã load đầy đủ.
2. Sau khi toàn bộ pod sẵn sàng, IAM bắt đầu cấp descriptor/ticket cho key mới.
3. Giữ old key cho ticket cũ trong TTL + clock skew + handshake/deploy grace.
4. Ngừng nhận handshake bằng old key sau cửa sổ đó; các connection đã thiết lập tiếp tục bằng cipher states riêng.
5. Pod restart làm connection của pod đó reconnect/handshake mới; không migrate hoặc lưu session keys vào DB.

Điều kiện xong: scale và rolling restart không làm IAM chọn key mà một phần backend chưa hỗ trợ.

### Bước 10 — Kiểm chứng và cắt chuyển

Thêm reference Go initiator trong test fixture (`internal/hub/infrastructure/noise/noisetest/peer.go` nếu cần dùng chung nhiều package); fixture không được import package đang được test theo cách gây import cycle.

Thêm `internal/hub/adapter/tcp/server_test.go` và `noise_integration_test.go` để kiểm tra thật đường nio -> frames -> Noise -> auth -> ACK. Presence có thể fake có kiểm soát; không cần MySQL/Redis thật cho mọi transport case.

Kiểm tra bắt buộc:

- Happy path IAM ticket -> NK -> device signature -> Redis registration -> ACK -> encrypted record.
- Sai Hub static key/key ID/prologue; ticket signature/audience/key binding/expiry sai.
- Signature identity sai; proof từ connection khác; auth gửi lại hoặc business packet trước ACTIVE.
- Ciphertext sửa/replay/reorder/truncate; hướng c2s/s2c đảo; nonce không reset sau auth.
- Frame TCP chia ở mọi vị trí header/body; nhiều frame cùng read; oversized frame và fairness wake.
- Worker gửi đồng thời; Redis callback chậm/sai thứ tự, closed/stale/replaced connection; write failure sau Encrypt.
- Timeout ở mọi phase; preauth slot/presence/buffer được cleanup đúng một lần.
- Hai replica cùng static key nhưng session độc lập; rotation old/new và reconnect khi pod chết.
- Legacy ticket/frame bị từ chối rõ ràng, không tự fallback.

Giữ và cập nhật tests lifecycle ở `connection/manager_test.go`, `sender_test.go`, `registry_test.go`, `helpers_test.go`. Không xóa tests để làm build xanh; chuyển assertion crypto cũ sang Noise behavior tương ứng.

Lệnh kiểm chứng khi triển khai:

```sh
go test ./internal/shared/... ./internal/iam/... ./internal/hub/... ./cmd/...
go test -race ./internal/hub/...
go test ./...
go build ./cmd/iam ./cmd/hub
```

Fuzz decoder/framing và đo handshake throughput cùng memory/connection trước production. Dùng giới hạn preauth/rate limit phù hợp để chi phí DH không chiếm event loop khi reconnect storm. Không mặc định offload từng bước handshake sang goroutine độc lập nếu chưa thiết kế serialization/state ownership.

Mặc định rollout v2 đồng bộ vì repo đang phát triển. Nếu đã có client production, giữ listener legacy ở port/deployment riêng trong cửa sổ migration; không trộn cả hai protocol và fallback tự động trên cùng stream. Rollback cả nhóm IAM/Hub/Client/config theo version tương thích.

## 5. Danh sách xóa sau cùng

Chỉ xóa khi dependency đã được chuyển và tests thay thế đã có:

```text
internal/iam/infrastructure/crypto/hkdf_deriver.go
internal/iam/infrastructure/crypto/hkdf_deriver_test.go
internal/iam/application/keyexchange/key_deriver.go
internal/iam/application/keyexchange/mocks/key_deriver_mock.go

internal/shared/session/keys.go
internal/shared/ticket/cipher.go
internal/shared/ticket/cipher_test.go
internal/shared/ticket/utils.go
internal/shared/ticket/utils_test.go

internal/hub/core/activation/keys.go
internal/hub/core/activation/keys_test.go
internal/hub/core/activation/handshake.go
internal/hub/core/activation/handshake_test.go
internal/hub/core/protocol/aad.go
internal/hub/core/protocol/aad_test.go
internal/hub/core/protocol/ports.go
internal/hub/core/protocol/mocks/ports_mock.go
internal/hub/core/protocol/protocoltest/secure_channel.go
internal/hub/infrastructure/crypto/aesgcm_channel.go
internal/hub/infrastructure/crypto/aesgcm_channel_test.go
```

`keyexchange/` được đổi tên phần còn lại thành `connectionticket/`. Các folder `crypto/`, `protocol/`, `activation/` vẫn cần giữ: còn ticket signing/verification, Ed25519 identity, framing/serialization và authorization.

Không xóa `pkg/nio`, `pkg/buffer`, OIDC/JWT, presence/registry, NATS, Scylla hoặc `devices.peer_pub` trong migration này.

## 6. Các nhóm thay đổi có thể review độc lập

1. Contract + dependency + provisioning/public descriptor.
2. Ticket chỉ ký + IAM connection-ticket API.
3. Noise adapter + framing/plaintext codec + generated protobuf.
4. Activation + Session/sender/OnTraffic + wiring.
5. Integration tests + scripts/env/docs + xóa phần legacy.

Các nhóm 2–4 phụ thuộc nhau ở runtime; ghép và validate đầy đủ trước deploy. Không triển khai một nhóm trung gian vào hệ thống đang phục vụ client cũ.

## 7. Tài liệu đối chiếu

- [Noise specification](https://noiseprotocol.org/noise.html): NK, Prologue, channel binding, framing và chiều Split.
- [flynn/noise source](https://github.com/flynn/noise/blob/main/state.go): HandshakeState, ChannelBinding, CipherState và nonce progression.
- [flynn/noise API](https://pkg.go.dev/github.com/flynn/noise): cấu hình và API message-based.

Các quyết định wire/API/lifecycle trong kế hoạch là thiết kế dành cho repo này, không phải tính năng mà Noise tự làm thay ứng dụng.
