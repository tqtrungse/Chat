/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

// Package logtest gives a log.Logger. A test can read the entries of that
// Logger.
//
// The logger of this project writes to stdout, because a container collects
// the logs from that stream. A test that asserts on an entry needs the bytes
// of that stream. New gives such a logger and the file that holds the bytes.
package logtest

import (
	"os"
	"path/filepath"
	"testing"

	"xxx/pkg/log"
)

// New makes a log.Logger that writes to a file under t.TempDir() and gives
// the shutdown function and the path of that file.
//
// log.NewLogger reads os.Stdout one time, and the sink keeps that file.
// Therefore, New puts the file of the test in os.Stdout only for that call
// and puts the former value back at once. The tests that use New must not
// call t.Parallel, because os.Stdout is a value of the process.
//
// The sink holds each entry in a queue and in a buffer. Therefore, call
// log.Logger.Sync or the shutdown function before a read of the file.
//
// Sampling is off unless the test asks for it with cfg.SampleFirst: a test
// that counts entries cannot accept a sampler that thins out one message
// that repeats.
func New(t *testing.T, cfg log.Config) (logger *log.Logger, closeFn func() error, path string) {
	t.Helper()

	path = filepath.Join(t.TempDir(), "test.log")
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("logtest: create %s: %v", path, err)
	}
	t.Cleanup(func() { _ = file.Close() })

	if cfg.SampleFirst <= 0 {
		cfg.DisableSampling = true
	}

	stdout := os.Stdout
	os.Stdout = file
	logger, closeFn, err = log.NewLogger(cfg)
	os.Stdout = stdout
	if err != nil {
		t.Fatalf("logtest: new logger: %v", err)
	}
	return logger, closeFn, path
}
