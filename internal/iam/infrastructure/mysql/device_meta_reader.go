/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package mysql

import (
	"context"
	"errors"

	"xxx/internal/iam/application/connticket"
	"xxx/internal/iam/infrastructure/mysql/model"
	"xxx/internal/shared/device"

	"xxx/pkg/database/sql"

	"gorm.io/gorm"
)

type deviceMetaReader struct{}

// NewDeviceMetaReader creates the MySQL reader used to load device authorization metadata.
func NewDeviceMetaReader() connticket.DeviceMetaReader {
	return new(deviceMetaReader)
}

// FindDeviceMeta reads the device owner, state, and identity public key using
// the database handle in dbCtx. It returns nil, nil when the device is absent.
func (d *deviceMetaReader) FindDeviceMeta(
	dbCtx context.Context,
	deviceID device.ID,
) (*connticket.DeviceMeta, error) {
	val, err := sql.ExtractDB(dbCtx)
	if err != nil {
		return nil, err
	}

	var (
		db      = val.(*gorm.DB)
		mDevice = &model.Device{ID: deviceID}
	)

	err = db.
		WithContext(dbCtx).
		Select("external_id", "state", "identity_pub").
		Take(mDevice).
		Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &connticket.DeviceMeta{
		ExternalID:  mDevice.ExternalID,
		State:       mDevice.State,
		IdentityPub: mDevice.IdentityPub,
	}, nil
}
