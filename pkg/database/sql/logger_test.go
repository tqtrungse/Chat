/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package sql

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"xxx/pkg/log"
	"xxx/pkg/log/logtest"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// logtest.New replaces os.Stdout for the call, so no test below runs parallel.

const testStatement = "SELECT * FROM `devices` WHERE `id` = ?"

func readEntries(t *testing.T, path string) []map[string]any {
	t.Helper()

	data, err := os.ReadFile(path)
	require.NoError(t, err)

	content := strings.TrimSpace(string(data))
	if content == "" {
		return nil
	}

	var entries []map[string]any
	for line := range strings.SplitSeq(content, "\n") {
		var entry map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &entry))
		entries = append(entries, entry)
	}
	return entries
}

func TestGormLoggerTrace(t *testing.T) {
	const slowThreshold = 200 * time.Millisecond

	for _, tc := range []struct {
		name      string
		elapsed   time.Duration
		err       error
		level     zapcore.Level
		wantLevel string
		wantMsg   string
	}{
		{
			name:      "failure",
			err:       errors.New("boom"),
			level:     zapcore.InfoLevel,
			wantLevel: "error",
			wantMsg:   msgFailedStatement,
		},
		{
			name:      "slow",
			elapsed:   2 * slowThreshold,
			level:     zapcore.InfoLevel,
			wantLevel: "warn",
			wantMsg:   msgSlowStatement,
		},
		{
			name:      "normal at info writes nothing",
			elapsed:   0,
			err:       nil,
			level:     zapcore.InfoLevel,
			wantLevel: "",
			wantMsg:   "",
		},
		{
			name:      "normal at debug",
			elapsed:   0,
			err:       nil,
			level:     zapcore.DebugLevel,
			wantLevel: "debug",
			wantMsg:   msgStatement,
		},
		{
			name:      "missing record is not a failure",
			elapsed:   0,
			err:       gormlogger.ErrRecordNotFound,
			level:     zapcore.InfoLevel,
			wantLevel: "",
			wantMsg:   "",
		},
		{
			name:      "missing record still reports slow",
			err:       gormlogger.ErrRecordNotFound,
			elapsed:   2 * slowThreshold,
			level:     zapcore.InfoLevel,
			wantLevel: "warn",
			wantMsg:   msgSlowStatement,
		},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				logger, closeLogger, path := logtest.New(t, log.Config{})
				t.Cleanup(func() { _ = closeLogger() })
				logger.SetLevel(tc.level)

				rendered := 0
				fc := func() (string, int64) {
					rendered++
					return testStatement, 3
				}

				bridge := newGormLogger(logger, slowThreshold, false)
				bridge.Trace(t.Context(), time.Now().Add(-tc.elapsed), fc, tc.err)
				require.NoError(t, closeLogger())

				entries := readEntries(t, path)
				if tc.wantMsg == "" {
					require.Empty(t, entries)
					require.Zero(t, rendered, "Explain costs a pass over the statement")
					return
				}

				require.Len(t, entries, 1)
				require.Equal(t, 1, rendered)

				entry := entries[0]
				require.Equal(t, tc.wantLevel, entry["level"])
				require.Equal(t, tc.wantMsg, entry["msg"])
				require.Equal(t, "gorm", entry["component"])
				require.Equal(t, testStatement, entry["sql"])
				require.Equal(t, float64(3), entry["rows"])
				require.Contains(t, entry, "elapsed")
				require.Contains(t, entry["caller"], "logger_test.go:")

				if tc.wantLevel == "error" {
					require.Equal(t, tc.err.Error(), entry["error"])
				}
			},
		)
	}
}

func TestGormLoggerParamsFilter(t *testing.T) {
	logger, closeLogger, _ := logtest.New(t, log.Config{})
	t.Cleanup(func() { _ = closeLogger() })

	params := []any{"token-secret"}

	// GORM applies the filter only when the logger carries this interface.
	redacting, ok := newGormLogger(logger, 0, false).(gorm.ParamsFilter)
	require.True(t, ok)

	statement, got := redacting.ParamsFilter(t.Context(), testStatement, params...)
	require.Equal(t, testStatement, statement)
	require.Nil(t, got)

	verbose := newGormLogger(logger, 0, true).(gorm.ParamsFilter)
	statement, got = verbose.ParamsFilter(t.Context(), testStatement, params...)
	require.Equal(t, testStatement, statement)
	require.Equal(t, params, got)
}

func TestGormLoggerLogModeKeepsReceiver(t *testing.T) {
	logger, closeLogger, path := logtest.New(t, log.Config{})
	t.Cleanup(func() { _ = closeLogger() })
	logger.SetLevel(zapcore.DebugLevel)

	bridge := newGormLogger(logger, 0, false)
	silent := bridge.LogMode(gormlogger.Silent)
	require.Equal(t, gormlogger.Silent, silent.(*gormLogger).level)
	require.Equal(t, gormlogger.Info, bridge.(*gormLogger).level)

	rendered := 0
	fc := func() (string, int64) {
		rendered++
		return testStatement, 1
	}

	silent.Trace(t.Context(), time.Now(), fc, nil)
	bridge.Trace(t.Context(), time.Now(), fc, nil)
	require.NoError(t, closeLogger())

	entries := readEntries(t, path)
	require.Len(t, entries, 1)
	require.Equal(t, msgStatement, entries[0]["msg"])
	require.Equal(t, 1, rendered)
}

// TestGormLoggerReachesGorm covers the wiring that the other tests assume:
// gorm.Open takes the bridge, and GORM asks it for ParamsFilter, so the
// statement in the log keeps its placeholder instead of the value.
func TestGormLoggerReachesGorm(t *testing.T) {
	logger, closeLogger, path := logtest.New(t, log.Config{})
	t.Cleanup(func() { _ = closeLogger() })
	logger.SetLevel(zapcore.DebugLevel)

	primary, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)

	// The GORM handle and dbresolver each detect the version once.
	for range 2 {
		mock.
			ExpectQuery("^SELECT VERSION\\(\\)$").
			WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("8.0.36"))
	}

	db := NewMySql(logger).(*mysql)
	db.openPool = func(context.Context, string, string, PoolConfig) (*stdsql.DB, error) {
		return primary, nil
	}

	closeDB, err := db.Open(t.Context(), Config{PrimaryAddr: "primary:3306"})
	require.NoError(t, err)

	const statement = "UPDATE devices SET name = ? WHERE id = ?"
	mock.
		ExpectExec("^UPDATE devices SET name = \\? WHERE id = \\?$").
		WithArgs("hub-1", 7).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectClose()
	t.Cleanup(func() {
		require.NoError(t, closeDB())
		require.NoError(t, mock.ExpectationsWereMet())
	})

	require.NoError(
		t,
		db.WithDB(t.Context(), TxSourcePrimary, func(ctx context.Context) error {
			handle, extractErr := ExtractDB(ctx)
			if extractErr != nil {
				return extractErr
			}
			return handle.(*gorm.DB).Exec(statement, "hub-1", 7).Error
		}),
	)
	require.NoError(t, closeLogger())

	entries := readEntries(t, path)
	require.NotEmpty(t, entries)

	entry := entries[len(entries)-1]
	require.Equal(t, "debug", entry["level"])
	require.Equal(t, msgStatement, entry["msg"])
	require.Equal(t, "gorm", entry["component"])
	require.Equal(t, float64(1), entry["rows"])
	// ParamsFilter keeps "hub-1" and 7 out of the logged statement.
	require.Equal(t, statement, entry["sql"])
}

func TestGormLoggerNotice(t *testing.T) {
	logger, closeLogger, path := logtest.New(t, log.Config{})
	t.Cleanup(func() { _ = closeLogger() })

	newGormLogger(logger, 0, false).
		Warn(t.Context(), "replica %s lags %ds", "r1", 3)
	require.NoError(t, closeLogger())

	entries := readEntries(t, path)
	require.Len(t, entries, 1)
	require.Equal(t, "warn", entries[0]["level"])
	require.Equal(t, msgNotice, entries[0]["msg"])
	require.Equal(t, "replica r1 lags 3s", entries[0]["detail"])
	require.Equal(t, "gorm", entries[0]["component"])
}
