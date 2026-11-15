# Partial fork of zap

This directory holds a partial fork of [go.uber.org/zap](https://github.com/uber-go/zap) at **v1.28.0**.
The fork has only the encoder, the core and the buffer. Each other part of zap comes from upstream.
Each package that logs continues to import `go.uber.org/zap`.

## Why the fork is necessary

`pkg/log` writes logs through an asynchronous sink (see `../sink.go`). The goroutine that writes a log
encodes the entry and puts it in a queue. One writer goroutine takes the entry from the queue and writes it
to stdout. Therefore, a slow log collector or a full pipe cannot stop the goroutine that wrote the log.

Upstream zap cannot give this design with no copy and with a pool of limited size. There are two causes.

### 1. The core of upstream frees the buffer immediately after the write

`ioCore.Write` of upstream does this:

```go
_, err = c.out.Write(buf.Bytes())
buf.Free()
```

The free is correct only if the write is complete when `Write` returns. An asynchronous sink does not
obey this condition. The sink puts the bytes in a queue, and the writer goroutine reads them later. After
the free, the encoder gives the same bytes to the next entry. Then stdout shows the next entry at the
position of this entry.

With upstream, the sink must copy each entry before `Write` returns. That copy is on the goroutine that
wrote the log, and it costs one allocation or one copy for each entry.

The fork changes only `ioCore.Write` (`zapcore/core.go`). If the sink implements `BufferWriteSyncer`, the
core gives the complete buffer to `WriteBuffer` and frees nothing. The sink is then the owner of the buffer.
The sink frees the buffer after the write to stdout. Therefore, the bytes move from the encoder to stdout
with no copy. A sink that does not implement `BufferWriteSyncer` continues to work as with upstream.

### 2. The buffer pool of upstream has no size limit

Upstream keeps each `buffer.Buffer` and its byte slice in one `sync.Pool`. Thus, the pool has these
problems:

- The pool has no limit of size. One large entry makes the slice larger, and the pool keeps that large
  slice. Later, the pool gives the large slice to a small entry. The allocation stays for the life of the
  process.
- The pool is only for zap. The bytes of a log entry cannot serve other parts of the program.
- `Free` does not detect a second call. With the transfer of ownership, more code paths free a buffer.
  Thus, a second `Free` becomes a more probable error. Upstream then gives one buffer to two callers, and
  the two callers write in the same bytes.

The fork takes the byte slices from `xxx/pkg/pool/slice` (`buffer/pool.go`). That pool puts each slice in a
bucket for its size, from 2^0 to 2^30 bytes. The complete program shares that pool. A `sync.Pool` holds
only the `Buffer` objects. Also, `put` ignores a second `Free` of the same `Buffer`.

### Why the encoder is also in the fork

The buffer type is in the signature of `Encoder.EncodeEntry`. The JSON encoder of upstream gets its buffers
from `internal/bufferpool`, and Go does not permit an import of an internal package from another module.
Therefore, a change of the buffer type makes a fork of these parts necessary:

- the `Encoder` interface (`zapcore/encoder.go`),
- the JSON encoder (`zapcore/json_encoder.go`),
- the generic pool that the JSON encoder uses (`internal/pool/pool.go`).

## What the fork contains

| File                            | Upstream source                                      | Change                                                                                              |
| ------------------------------- | ---------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `zapcore/core.go`               | `zapcore/core.go`                                    | `Write` gives the buffer to a `BufferWriteSyncer`. `Core` is an alias. `nopCore` is not in the fork. |
| `zapcore/encoder.go`            | `zapcore/encoder.go`                                 | `EncodeEntry` returns the `Buffer` of the fork.                                                     |
| `zapcore/json_encoder.go`       | `zapcore/json_encoder.go`                            | Import paths, `buffer.Get` in place of `bufferpool.Get`, `any` in place of `interface{}`.           |
| `zapcore/reflected_encoder.go`  | `zapcore/reflected_encoder.go`                       | Only `defaultReflectedEncoder`. The interface is an alias.                                          |
| `zapcore/field.go`              | `addFields` in `zapcore/field.go`                    | A copy, because upstream does not export `addFields`.                                               |
| `zapcore/alias.go`              | (new)                                                | Type aliases to the types of upstream.                                                              |
| `buffer/buffer.go`              | `buffer/buffer.go`                                   | No `pool` field. `Free` calls the pool of this package.                                             |
| `buffer/pool.go`                | `buffer/pool.go` and `internal/bufferpool`           | Slices from `pkg/pool/slice`. One pool for the package. A guard against a second `Free`.            |
| `internal/pool/pool.go`         | `internal/pool/pool.go`                              | No change of code.                                                                                  |

## What the fork does not contain

`zapcore/alias.go` declares the types of upstream as aliases: `Entry`, `Field`, `Level`, `Core`,
`WriteSyncer`, `EncoderConfig`, `ObjectEncoder` and the other encoder interfaces. An alias is the same type
as the type of upstream, and it is not a copy. This has two results:

- A `Core` of the fork satisfies the `zapcore.Core` of upstream. Therefore, `zap.New`, `zapcore.NewTee` and
  `zapcore.NewSamplerWithOptions` of upstream accept it. The fork needs no copy of the logger, the tee or
  the sampler.
- Each call site continues to use `zap.String`, `zap.Error` and the other field functions of upstream.
  The fork changes no call site.

Only the `Encoder` interface is not an alias, because `EncodeEntry` must return the `Buffer` of the fork.
No exported part of zap accepts an `Encoder`. Therefore, this difference stays inside `pkg/log`.

## The contract of `BufferWriteSyncer`

A sink that implements `BufferWriteSyncer` must obey these three rules. `zapcore/core.go` has the full
text.

1. `WriteBuffer` is the owner of `buf` after the call. The core keeps no reference and frees nothing.
2. `WriteBuffer` must call `buf.Free` one time on each path. This includes the path that discards the
   entry.
3. `WriteBuffer` must not keep `buf` after `Free`, and it must not keep the result of `buf.Bytes`.

## Upgrade of zap

The fork must stay at the same version as `go.uber.org/zap` in `go.mod`. When you change that version, do
these steps:

1. Compare each upstream source in the table above between the old version and the new version.
2. Apply each change of upstream to the file of the fork.
3. Change the version in the `Forked from ... at vX.Y.Z` line of each file, and in this README.
4. Run `go test ./pkg/log/...`. The `TestEncodeEntry_MatchesUpstream*` tests in
   `zapcore/json_encoder_test.go` compare the output of the forked encoder with the output of the encoder
   of upstream. They must pass. If they fail, the fork changed more than the buffer.

## License

zap has the MIT license. `LICENSE.upstream` holds the license text of upstream. Each forked file keeps
the copyright line of Uber Technologies, Inc. and the `SPDX-License-Identifier: MIT` line.
