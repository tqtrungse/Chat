/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package mysql

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"xxx/internal/iam/application/connticket"
	"xxx/internal/shared/device"

	"xxx/pkg/database/sql"
	"xxx/pkg/database/sql/mocks"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/google/go-cmp/cmp"
	"go.uber.org/mock/gomock"
)

func Test_DeviceMetaReader_FindDeviceMeta(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	type fields struct {
		ctx     context.Context
		sqlMock sqlmock.Sqlmock
	}

	type testcase struct {
		prepare  func(f *fields)
		args     device.ID
		expected *connticket.DeviceMeta
		wantErr  bool
	}

	testCases := map[string]testcase{
		"Success (existence)": {
			prepare: func(f *fields) {
				f.sqlMock.
					ExpectQuery(fmt.Sprintf("^%s$", regexp.QuoteMeta(
						strings.Join([]string{
							"SELECT `external_id`,`state`,`identity_pub`",
							"FROM `devices`",
							"WHERE `devices`.`deleted_at` IS NULL AND `devices`.`id` = ?",
							" LIMIT ?",
						}, " "),
					))).
					WithArgs(
						device.ID(1),
						1,
					).
					WillReturnRows(
						sqlmock.
							NewRows([]string{
								"external_id",
								"state",
								"identity_pub",
							}).
							AddRow(
								"abc-123",
								device.StateActive,
								[]byte("123456789"),
							),
					)
			},
			args: device.ID(1),
			expected: &connticket.DeviceMeta{
				ExternalID:  "abc-123",
				State:       device.StateActive,
				IdentityPub: []byte("123456789"),
			},
			wantErr: false,
		},
		"Success (non-existence)": {
			prepare: func(f *fields) {
				f.sqlMock.
					ExpectQuery(fmt.Sprintf("^%s$", regexp.QuoteMeta(
						strings.Join([]string{
							"SELECT `external_id`,`state`,`identity_pub`",
							"FROM `devices`",
							"WHERE `devices`.`deleted_at` IS NULL AND `devices`.`id` = ?",
							" LIMIT ?",
						}, " "),
					))).
					WithArgs(
						device.ID(2),
						1,
					).
					WillReturnRows(sqlmock.NewRows(nil))
			},
			args:     device.ID(2),
			expected: nil,
			wantErr:  false,
		},
		"Error": {
			prepare: func(f *fields) {
				f.sqlMock.
					ExpectQuery(fmt.Sprintf("^%s$", regexp.QuoteMeta(
						strings.Join([]string{
							"SELECT `external_id`,`state`,`identity_pub`",
							"FROM `devices`",
							"WHERE `devices`.`deleted_at` IS NULL AND `devices`.`id` = ?",
							" LIMIT ?",
						}, " "),
					))).
					WithArgs(
						device.ID(3),
						1,
					).
					WillReturnError(errors.New("expected error"))
			},
			args:     device.ID(3),
			expected: nil,
			wantErr:  true,
		},
	}

	for description, tc := range testCases {
		t.Run(description, func(t *testing.T) {
			db, sqlMock := mocks.NewMysql(t)
			mocks.Setup(t, db)

			f := &fields{
				ctx:     t.Context(),
				sqlMock: sqlMock,
			}

			doneSqlMock := mocks.ExpectSqlMock(
				t,
				f.sqlMock,
				func() {
					tc.prepare(f)
				},
			)
			defer doneSqlMock()

			var (
				err    error
				resp   *connticket.DeviceMeta
				reader = NewDeviceMetaReader()
			)

			err = db.WithDB(f.ctx, sql.TxSourcePrimary, func(dbCtx context.Context) error {
				resp, err = reader.FindDeviceMeta(dbCtx, tc.args)
				return err
			})
			if (err != nil) != tc.wantErr {
				t.Errorf("[datastore] deviceMetaReader.FindDeviceMeta error = %v, wantErr %v", err, tc.wantErr)
			}

			diff := cmp.Diff(resp, tc.expected)
			if diff != "" {
				t.Errorf("[datastore] deviceMetaReader.FindDeviceMeta value is mismatch (-actual +expected):\n%s", diff)
			}
		})
	}
}
