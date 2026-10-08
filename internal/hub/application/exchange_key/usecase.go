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

package exchange_key

import (
	"context"

	"xxx/api/hub/v1/json"
	"xxx/internal/hub/connection"
	"xxx/internal/hub/domain/device"
	"xxx/internal/hub/protocol"

	"xxx/pkg/database/sql"
	slicepool "xxx/pkg/pool/slice"
)

type Usecase interface {
	Exchange(ctx context.Context, req json.ExchangeKeyReq) (*json.ExchangeKeyResp, error)
}

var keyDerivationInfo = []byte("xxx/hub/exchange-key/v1")

type keyExchanger struct {
	db               sql.DB
	keyDeriver       KeyDeriver
	deviceMetaReader DeviceMetaReader
	router           *connection.Router
}

func New(
	db sql.DB,
	keyDeriver KeyDeriver,
	deviceMetaReader DeviceMetaReader,
	router *connection.Router,
) Usecase {
	return &keyExchanger{
		db:               db,
		deviceMetaReader: deviceMetaReader,
		keyDeriver:       keyDeriver,
		router:           router,
	}
}

func (ke *keyExchanger) Exchange(
	ctx context.Context,
	req json.ExchangeKeyReq,
) (*json.ExchangeKeyResp, error) {
	var (
		deviceID   = device.ID(req.DeviceID)
		deviceMeta *DeviceMeta
		err        error
	)

	if req.DeviceID == 0 || len(req.ClientPubKey) != 32 {
		return nil, ErrRequestInvalid
	}

	if ke.router.Exist(deviceID) {
		return nil, protocol.ErrSessionDuplicate
	}

	err = ke.db.Transaction(ctx, sql.TxSourcePrimary, func(dbCtx context.Context) error {
		deviceMeta, err = ke.deviceMetaReader.FindDeviceMeta(dbCtx, deviceID)
		return err
	})
	if err != nil {
		return nil, err
	}
	if deviceMeta == nil {
		return nil, protocol.ErrDeviceNotFound
	}

	var success bool
	defer func() {
		if !success {
			slicepool.Put(deviceMeta.IdentityPub)
		}
	}()

	if deviceMeta.State != device.StateActive {
		return nil, protocol.ErrDeviceUnactive
	}

	var (
		resp      = new(json.ExchangeKeyResp)
		secretKey [32]byte
		hmacKey   [32]byte
	)

	secretKey, hmacKey, resp.ServerPubKey, err = ke.keyDeriver.DeriveKeys(
		(*[32]byte)(req.ClientPubKey),
		keyDerivationInfo,
	)
	if err != nil {
		return nil, err
	}

	err = ke.router.CreateUnactiveConn(
		deviceID,
		&secretKey,
		&hmacKey,
		deviceMeta.IdentityPub,
	)
	if err == nil {
		success = true
		return resp, nil
	}
	return nil, err
}
