/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package connection

import (
	"xxx/api/hub/v1/proto/gen/pub"
	"xxx/internal/hub/domain/device"
	"xxx/internal/hub/protocol"

	"xxx/pkg/batcher"
	"xxx/pkg/nio"
	slicepool "xxx/pkg/pool/slice"

	"google.golang.org/protobuf/proto"
)

//func RespondStatus(
//	codec *Codec,
//	conn *Conn,
//	code pb.Code,
//) error {
//	buf, err := codec.Encode(
//		&conn.hmacKey,
//		pb.PacketType_RESP_STATUS,
//		&pb.StatusResp{Code: code},
//		nil,
//	)
//	if err != nil {
//		return err
//	}
//
//	_, err = conn.gconn.Write(buf)
//	slicepool.Put(buf)
//	return err
//}
//
//func AsyncRespondStatus(
//	logger *pkg.Logger,
//	codec *Codec,
//	conn *Conn,
//	code pb.Code,
//) error {
//	buf, err := codec.Encode(
//		&conn.hmacKey,
//		pb.PacketType_RESP_STATUS,
//		&pb.StatusResp{Code: code},
//		nil,
//	)
//	if err != nil {
//		return err
//	}
//
//	err = conn.gconn.AsyncWrite(buf, func(c gnet.Conn, err error) error {
//		HandleErrCallback(logger, c, err)
//		slicepool.Put(buf)
//		return nil
//	})
//	if err != nil {
//		slicepool.Put(buf)
//	}
//	return err
//}
//
//func HandleErrCallback(
//	logger *pkg.Logger,
//	conn nio.Conn,
//	err error,
//) {
//	if errors.Is(err, net.ErrClosed) {
//		return
//	}
//	logger.Error("nio.Conn.AsyncWrite callback failed", zap.Error(err))
//
//	var errno syscall.Errno
//	if errors.As(err, &errno) {
//		err = conn.Close()
//		if err != nil {
//			logger.Error("failed to close nio.Conn", zap.Error(err))
//		}
//	}
//}

type deviceOp struct {
	DeviceID  device.ID
	Timestamp int64
	Op        Op
}

func send(
	conn nio.Conn,
	hmacKey *[32]byte,
	encoder *protocol.Encoder,
	packType pub.PacketType,
	msg proto.Message,
	expand []byte,
) error {
	buf, err := encoder.Encode(
		hmacKey,
		packType,
		msg,
		expand,
	)
	if err != nil {
		return err
	}
	_, err = conn.Write(buf)
	slicepool.Put(buf)
	return err
}

func sendS(
	conn nio.Conn,
	secretKey *[32]byte,
	encoder *protocol.Encoder,
	packType pub.PacketType,
	msg proto.Message,
	expand []byte,
) error {
	buf, err := encoder.EncodeS(
		secretKey,
		packType,
		msg,
		expand,
	)
	if err != nil {
		return err
	}
	_, err = conn.Write(buf)
	slicepool.Put(buf)
	return err
}

// dedupeDeviceOps keeps only the latest operation for each device.
//
// An operation is considered newer when:
//   - it has a greater timestamp, or
//   - it has the same timestamp but appears later in reqs.
//
// isLatest[idx] is true when reqs[idx] is the operation selected for
// its device.
//
// Every DeviceID appears in exactly one of addDevices or delDevices,
// assuming the operation is valid.
func dedupeDeviceOps(reqs []batcher.Req[deviceOp]) (
	isLatest []bool,
	addDevices []device.ID,
	delDevices []device.ID,
) {
	type deviceOpMetadata struct {
		idx       int
		timestamp int64
	}

	latest := make(map[device.ID]deviceOpMetadata, len(reqs))

	for idx, req := range reqs {
		deviceID := req.Data.DeviceID

		meta, ok := latest[deviceID]
		if !ok ||
			req.Data.Timestamp > meta.timestamp ||
			(req.Data.Timestamp == meta.timestamp && idx > meta.idx) {
			latest[deviceID] = deviceOpMetadata{
				idx:       idx,
				timestamp: req.Data.Timestamp,
			}
		}
	}

	isLatest = make([]bool, len(reqs))
	addDevices = make([]device.ID, 0, len(latest))
	delDevices = make([]device.ID, 0, len(latest))

	for _, meta := range latest {
		idx := meta.idx
		req := reqs[idx]

		switch req.Data.Op {
		case addDeviceOp:
			isLatest[idx] = true
			addDevices = append(addDevices, req.Data.DeviceID)

		case delDeviceOp:
			isLatest[idx] = true
			delDevices = append(delDevices, req.Data.DeviceID)

		default:
			// Do not mark the request as latest because it cannot be
			// executed by Redis. flush() will resolve this request
			// separately.
		}
	}

	return isLatest, addDevices, delDevices
}

// sendResult sends the Redis result to the latest requests of the
// specified operation.
func sendResult(
	reqs []batcher.Req[deviceOp],
	isLatest []bool,
	op Op,
	err error,
) {
	for idx, req := range reqs {
		if !isLatest[idx] {
			continue
		}
		if req.Data.Op != op {
			continue
		}
		req.OnResult(nil, err)
	}
}
