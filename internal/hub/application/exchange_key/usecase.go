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
	connManager      *connection.Router
}

func New(
	db sql.DB,
	keyDeriver KeyDeriver,
	deviceMetaReader DeviceMetaReader,
	connManager *connection.Router,
) Usecase {
	return &keyExchanger{
		db:               db,
		deviceMetaReader: deviceMetaReader,
		keyDeriver:       keyDeriver,
		connManager:      connManager,
	}
}

func (ke *keyExchanger) Exchange(ctx context.Context, req json.ExchangeKeyReq) (*json.ExchangeKeyResp, error) {
	var (
		deviceID   = device.ID(req.DeviceID)
		deviceMeta *DeviceMeta
		err        error
	)

	err = ke.db.Transaction(ctx, sql.TxSourcePrimary, func(dbCtx context.Context) error {
		deviceMeta, err = ke.deviceMetaReader.FindDeviceMeta(dbCtx, deviceID)
		return err
	})
	if err != nil {
		return nil, err
	}
	if deviceMeta == nil {
		return nil, protocol.ErrNotFoundDevice
	}
	if deviceMeta.State != device.StateActive {
		slicepool.Put(deviceMeta.IdentityPub)
		deviceMeta.IdentityPub = nil
		return nil, protocol.ErrUnactiveDevice
	}

	var (
		resp      = new(json.ExchangeKeyResp)
		secretKey [32]byte
	)

	secretKey, resp.ServerPubKey, err = ke.keyDeriver.DeriveKeys(
		&req.ClientPubKey,
		keyDerivationInfo,
	)
	if err != nil {
		slicepool.Put(deviceMeta.IdentityPub)
		return nil, err
	}

	ke.connManager.CreateUnactiveConn(
		deviceID,
		&secretKey,
		&req.ClientHmacKey,
		deviceMeta.IdentityPub,
	)
	return resp, nil
}
