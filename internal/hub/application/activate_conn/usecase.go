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

package activate_conn

import (
	"context"
	"time"

	"xxx/internal/hub/connection"
	"xxx/internal/hub/protocol"

	pbpub "xxx/api/hub/v1/proto/gen/pub"
	"xxx/pkg/log"
	"xxx/pkg/nio"
	slicepool "xxx/pkg/pool/slice"

	"go.uber.org/zap"
)

type Usecase interface {
	Activate(ctx context.Context, conn nio.Conn, packet []byte) nio.Action
	Deactivate(ctx context.Context, conn nio.Conn) nio.Action
	Tick() (time.Duration, nio.Action)
}

type connActivator struct {
	logger  *log.Logger
	decoder *protocol.Decoder
	router  *connection.Router
}

func New(
	logger *log.Logger,
	decoder *protocol.Decoder,
	router *connection.Router,
) Usecase {
	return &connActivator{
		logger:  logger,
		decoder: decoder,
		router:  router,
	}
}

func (ca *connActivator) Activate(ctx context.Context, conn nio.Conn, pack []byte) nio.Action {
	var req pbpub.ActiveConnReq
	if err := ca.decoder.DecodeActivePack(pack, &req); err != nil {
		ca.logger.Error("failed to decode active request, force close", zap.Error(err))
		return nio.Close
	}
	defer func() {
		slicepool.Put(req.Token)
		slicepool.Put(req.Sign)
	}()

	if err := req.Validate(); err != nil {
		ca.logger.Error("failed to validate activate connection request, force close", zap.Error(err))
		return nio.Close
	}

	deviceID, err := ca.router.ActivateConn(ctx, req.Token, req.Sign, conn)
	if err != nil {
		ca.logger.Error("failed to activate connection, force close", zap.Error(err))
		return nio.Close
	}

	err = ca.router.Send(
		deviceID,
		connection.SendData{
			PackType: pbpub.PacketType_RESP_ACTIVE_CONN,
			Msg:      &pbpub.ActiveConnResp{Code: pbpub.Code_SUCCESS},
		},
	)
	if err != nil {
		ca.logger.Error("failed to respond, force close", zap.Error(err))
		return nio.Close
	}
	return nio.None
}

func (ca *connActivator) Deactivate(ctx context.Context, conn nio.Conn) nio.Action {
	ca.router.RemoveConn(ctx, conn)
	return nio.None
}

func (ca *connActivator) Tick() (time.Duration, nio.Action) {
	return ca.router.Tick(), nio.None
}
