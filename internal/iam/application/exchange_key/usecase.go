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
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"time"

	"xxx/api/iam/v1/json"
	"xxx/internal/iam/domain/device"
	shareddevice "xxx/internal/shared/device"
	sharedsession "xxx/internal/shared/session"

	"xxx/pkg/database/sql"
)

const ticketTTL = 15 * time.Second

var (
	keyDerivationInfo = []byte("xxx/iam/exchange-key/v1")
	errIdentityPub    = errors.New("invalid device identity key")
)

type Usecase interface {
	Exchange(ctx context.Context, subject string, req json.ExchangeKeyReq) (*json.ExchangeKeyResp, error)
}

type keyExchanger struct {
	db               sql.DB
	keyDeriver       KeyDeriver
	deviceMetaReader DeviceMetaReader
	ticketIssuer     TicketIssuer
}

func New(
	db sql.DB,
	keyDeriver KeyDeriver,
	deviceMetaReader DeviceMetaReader,
	ticketIssuer TicketIssuer,
) Usecase {
	return &keyExchanger{
		db:               db,
		keyDeriver:       keyDeriver,
		deviceMetaReader: deviceMetaReader,
		ticketIssuer:     ticketIssuer,
	}
}

func (ke *keyExchanger) Exchange(
	ctx context.Context,
	subject string,
	req json.ExchangeKeyReq,
) (*json.ExchangeKeyResp, error) {
	if req.DeviceID == 0 || len(req.ClientPubKey) != 32 || subject == "" {
		return nil, ErrRequestInvalid
	}

	var (
		meta *DeviceMeta
		err  error
	)
	err = ke.db.Transaction(ctx, sql.TxSourcePrimary, func(dbCtx context.Context) error {
		meta, err = ke.deviceMetaReader.FindDeviceMeta(dbCtx, shareddevice.ID(req.DeviceID))
		return err
	})
	if err != nil {
		return nil, err
	}
	// Same error for "not found" and "not yours": no device enumeration.
	if meta == nil || meta.ExternalID != subject {
		return nil, ErrDeviceNotFound
	}
	if meta.State != device.StateActive {
		return nil, ErrDeviceUnactive
	}
	if len(meta.IdentityPub) != ed25519.PublicKeySize {
		return nil, errIdentityPub
	}

	info := make([]byte, 0, len(keyDerivationInfo)+8)
	info = append(info, keyDerivationInfo...)
	info = binary.LittleEndian.AppendUint64(info, req.DeviceID)

	var (
		resp    = new(json.ExchangeKeyResp)
		derived DerivedKeys
	)

	derived, resp.ServerPubKey, err = ke.keyDeriver.DeriveKeys((*[32]byte)(req.ClientPubKey), info)
	if err != nil {
		return nil, err
	}

	t := &sharedsession.Ticket{
		DeviceID:      req.DeviceID,
		ExpiresAt:     time.Now().Add(ticketTTL).Unix(),
		Keys:          derived.Session,
		ActivationKey: derived.Activation,
	}
	copy(t.IdentityPub[:], meta.IdentityPub)

	resp.Ticket, err = ke.ticketIssuer.Seal(t)

	// Delete temporary key copies after sealing.
	clear(derived.Session.RecvEnc[:])
	clear(derived.Session.SendEnc[:])
	clear(derived.Session.RecvMac[:])
	clear(derived.Session.SendMac[:])
	clear(derived.Activation[:])

	clear(t.Keys.RecvEnc[:])
	clear(t.Keys.SendEnc[:])
	clear(t.Keys.RecvMac[:])
	clear(t.Keys.SendMac[:])
	clear(t.ActivationKey[:])

	if err != nil {
		return nil, err
	}
	return resp, nil
}
