// Copyright (c) 2016 Uber Technologies, Inc.
// SPDX-License-Identifier: MIT
//
// Forked from go.uber.org/zap/buffer/buffer.go at v1.28.0.

// Modified from go.uber.org/zap/zapcore/reflected_encoder.go.
//
// The ReflectedEncoder interface is not here. It is an alias of the interface
// of upstream in alias.go, because EncoderConfig.NewReflectedEncoder is a
// field of the EncoderConfig of upstream and returns that interface.
//
// Only defaultReflectedEncoder remains, because it is not exported and the
// forked encoder calls it.

package zapcore

import (
	"encoding/json"
	"io"
)

func defaultReflectedEncoder(w io.Writer) ReflectedEncoder {
	enc := json.NewEncoder(w)
	// For consistency with our custom JSON encoder.
	enc.SetEscapeHTML(false)
	return enc
}
