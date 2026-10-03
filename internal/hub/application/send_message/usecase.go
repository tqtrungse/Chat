/*
 * Copyright 2026 tqtrungse@gmail.com. All rights reserved.
 * Use of this pub code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package send_message

import (
	"xxx/pkg/nio"
)

type Usecase interface {
	Send(conn nio.Conn, packet []byte) nio.Action
	Stop()
}

//type MsgSenderConfig struct {
//	NumWorkers           uint32
//	WorkerScaleThreshold uint32
//}
//
//type msgSender struct {
//	ctx         context.Context
//	logger      *pkg.Logger
//	workerPool  *workerpool.Pool
//	decoder     *protocol.Decoder
//	connManager *connection.Router
//	idGenerator generator.IntIDGenerator
//	mqProducer  shared.MsgQueueProducer
//}
//
//func NewMsgSender(
//	ctx context.Context,
//	config MsgSenderConfig,
//	logger *pkg.Logger,
//	decoder *protocol.Decoder,
//	connManager *connection.Router,
//	idGenerator generator.IntIDGenerator,
//	mqProducer shared.MsgQueueProducer,
//) Usecase {
//	ms := &msgSender{
//		ctx:         ctx,
//		logger:      logger,
//		decoder:     decoder,
//		connManager: connManager,
//		idGenerator: idGenerator,
//		mqProducer:  mqProducer,
//	}
//	ms.workerPool = workerpool.New(
//		workerpool.Config{
//			Name:           "handler-worker-pool",
//			ScaleThreshold: config.WorkerScaleThreshold,
//			MaxWorkers:     config.NumWorkers,
//		},
//	)
//	ms.workerPool.SetPanicHandler(ms.handlePanic)
//	return ms
//}
//
//func (ms *msgSender) Send(conn gnet.Conn, msg []byte) gnet.Action {
//	sendConn := ms.connManager.LookupConn1(conn.Fd())
//	if sendConn == nil {
//		ms.logger.Error("invalid connection")
//		return gnet.Close
//	}
//	if sendConn.closed.Load() {
//		ms.logger.Error("connection is closed")
//		return gnet.Close
//	}
//
//	req := new(pub.SendMsgReq)
//	plain, content, code, err := ms.decoder.DecodeS(&sendConn.secretKey, msg, req)
//	if err != nil {
//		ms.logger.Error("failed to decode message", zap.Error(err))
//		_ = helper.RespondStatus(ms.decoder, sendConn, code)
//		return gnet.Close
//	}
//
//	err = req.Validate()
//	if err != nil {
//		slicepool.Put(plain)
//		ms.logger.Error("failed to validate send request", zap.Error(err))
//		_ = helper.RespondStatus(ms.decoder, sendConn, pb.Code_ERR_INVALID_REQ)
//		return gnet.Close
//	}
//
//	ms.workerPool.Go(func() {
//		ms.send(conn, plain, content, req)
//	})
//
//	return gnet.None
//}
//
//func (ms *msgSender) Stop() {
//	ms.workerPool.Stop()
//}
//
//func (ms *msgSender) send(
//	conn gnet.Conn,
//	plain []byte,
//	content []byte,
//	req *pub.SendMsgReq,
//) {
//	msgID, err := ms.idGenerator.Gen()
//	if err != nil {
//		slicepool.Put(plain)
//		ms.logger.Error("failed to gen message ID", zap.Error(err))
//		_ = helper.AsyncRespondStatus(ms.logger, ms.decoder, conn, pub.Code_ERR_INTERNAL)
//		return
//	}
//
//	var key [8]byte
//	binary.LittleEndian.PutUint64(key[:], req.ChannelID)
//
//	err = ms.mqProducer.Push(ms.ctx, key[:], plain)
//	if err != nil {
//		slicepool.Put(plain)
//		ms.logger.Error("failed to store message", zap.Error(err))
//		_ = helper.AsyncRespondStatus(ms.logger, ms.decoder, conn, pub.Code_ERR_INTERNAL)
//		return
//	}
//
//	err = helper.AsyncRespondStatus(ms.logger, ms.decoder, conn, pub.Code_SUCCESS)
//	if err != nil {
//		ms.logger.Error("gnet.Conn.AsyncWrite failed", zap.Error(err))
//	}
//
//	cipher := ms.decoder.EncodeS(
//		&sendConn.SecretKey,
//		pb.MessageType_REQ_FORWARD_SEND_MSG,
//		&pb.ForwardSendMsgReq{
//			SenderID:  req.SenderID,
//			ChannelID: req.ChannelID,
//			MsgID:     msgID,
//		},
//		content,
//	)
//
//	count := atomic.Int32{}
//	count.Add(int32(len(req.RecvDeviceIDs)))
//
//	for _, recvDeviceID := range req.RecvDeviceIDs {
//		recvConn := ms.connManager.LookupConn(recvDeviceID)
//		if recvConn == nil {
//			if count.Add(-1) <= 0 {
//				slicepool.Put(plain)
//			}
//			continue
//		}
//
//		if recvConn.closed.Load() {
//			if count.Add(-1) <= 0 {
//				slicepool.Put(plain)
//			}
//			continue
//		}
//
//		err = recvConn.gconn.AsyncWrite(cipher, func(c gnet.Conn, err error) error {
//			if count.Add(-1) <= 0 {
//				slicepool.Put(cipher)
//			}
//			connection.HandleErrCallback(ms.logger, c, err)
//			return nil
//		})
//		if err != nil {
//			ms.logger.Error("gnet.Conn.AsyncWrite failed", zap.Error(err))
//		}
//	}
//}
//
//func (ms *msgSender) handlePanic(_ context.Context, r any) {
//	ms.logger.Panic(
//		"panic",
//		zap.String("pool_name", ms.workerPool.Name()),
//		zap.Any("panic_reason", r),
//		zap.Stack("stack_trace"),
//	)
//}
