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

package mysql

import (
	"context"
	"errors"

	"xxx/internal/iam/application/exchange_key"
	"xxx/internal/iam/infrastructure/datastore/mysql/entity"
	shareddevice "xxx/internal/shared/device"

	"xxx/pkg/database/sql"

	"gorm.io/gorm"
)

type deviceMetaReader struct{}

func NewDeviceMetaReader() exchange_key.DeviceMetaReader {
	return new(deviceMetaReader)
}

func (d *deviceMetaReader) FindDeviceMeta(
	dbCtx context.Context,
	deviceID shareddevice.ID,
) (*exchange_key.DeviceMeta, error) {
	val, err := sql.ExtractDB(dbCtx)
	if err != nil {
		return nil, err
	}

	var (
		tx        = val.(*gorm.DB)
		entDevice = &entity.Device{ID: deviceID}
	)

	err = tx.
		WithContext(dbCtx).
		Select("external_id", "state", "identity_pub").
		Take(entDevice).
		Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &exchange_key.DeviceMeta{
		ExternalID:  entDevice.ExternalID,
		State:       entDevice.State,
		IdentityPub: entDevice.IdentityPub,
	}, nil
}
