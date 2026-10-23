/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package log

import "errors"

var (
	// ErrBufferSizeTooSmall shows that BufferSize is less than the smallest
	// buffer of a bufio.Writer. NewLogger returns this error.
	ErrBufferSizeTooSmall = errors.New("log: buffer size too small")

	// ErrSyncTimeout shows that the writer goroutine did not write each entry
	// of the queue before Config.SyncTimeout. Logger.Sync and the shutdown
	// function from NewLogger return this error. The shutdown function also
	// waits for that goroutine to exit, and a goroutine that stays active
	// gives the same error.
	//
	// A stdout that no program reads is the common cause. In a container,
	// stdout is a pipe to the log collector. If the collector stops, the pipe
	// becomes full, and the writer goroutine waits in a write to that pipe.
	//
	// This error is not a failure of a write. The sink keeps such a failure
	// separately and reports it on stderr one time. This error hides that
	// failure, because it replaces the result of the call. The report on
	// stderr stays.
	//
	// The entries stay in the queue and in the buffer. After Logger.Sync, the
	// writer goroutine is still active, and those entries can reach stdout
	// later. A second call to the shutdown function gives this same error and
	// does not wait again. Therefore, a process that exits after this error
	// loses those entries.
	ErrSyncTimeout = errors.New("log: timed out waiting for the log sink to drain")
)
