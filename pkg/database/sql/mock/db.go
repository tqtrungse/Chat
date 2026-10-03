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

package mock

import (
	"testing"
	"time"
	"xxx/pkg/database/sql"

	"github.com/DATA-DOG/go-sqlmock"
)

func Setup(t *testing.T, db sql.DB) {
	testConfig := sql.Config{
		Pool: sql.PoolConfig{
			MaxOpenConns:    100,
			MaxIdleConns:    10,
			ConnMaxIdleTime: 5 * time.Minute,
			ConnMaxLifetime: 5 * time.Minute,
		},
	}

	err := db.Open(t.Context(), testConfig)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		err = db.Close()
		if err != nil {
			t.Fatal(err)
		}
	})
}

func ExpectSqlMock(
	t *testing.T,
	sqlMock sqlmock.Sqlmock,
	prepare func(),
	wantErr bool,
) func() {
	sqlMock.ExpectBegin()

	if prepare != nil {
		prepare()
	}

	if wantErr {
		sqlMock.ExpectRollback()
	} else {
		sqlMock.ExpectCommit()
	}

	return func() {
		err := sqlMock.ExpectationsWereMet()
		if err != nil {
			t.Errorf("there were unfulfilled expectations: %s", err)
		}
		sqlMock.ExpectClose()
	}
}
