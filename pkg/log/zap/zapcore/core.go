// Copyright (c) 2016 Uber Technologies, Inc.
// SPDX-License-Identifier: MIT
//
// Forked from go.uber.org/zap/buffer/buffer.go at v1.28.0.

// Modified from go.uber.org/zap/zapcore/core.go.
//
// The Core interface is not here. It is an alias of the Core of upstream in
// alias.go. Therefore, zap.New, zapcore.NewTee and
// zapcore.NewSamplerWithOptions accept the core of this file. nopCore and
// NewNopCore are also not here, because the versions of upstream serve.
//
// The one change of behavior is in Write. The original always frees the
// buffer of the encoder:
//
//	_, err = c.out.Write(buf.Bytes())
//	buf.Free()
//
// The free is correct only because the write is complete when Write returns.
// An asynchronous sink breaks that condition. Such a sink puts the bytes in a
// queue, and a free at this position would give the buffer to the next entry
// before the writer goroutine reads it.
//
// Therefore, this version gives the complete buffer to a sink that accepts
// the ownership, and that sink frees the buffer after the write. The sink
// then needs no copy of the bytes. See BufferWriteSyncer.

package zapcore

import (
	"go.uber.org/zap/zapcore"

	"xxx/pkg/log/zap/buffer"
)

// BufferWriteSyncer is a WriteSyncer that accepts the ownership of the buffer
// of the encoder. A sink implements it to avoid a copy of each entry.
//
// The contract has three parts, and each part is necessary:
//
// 1. WriteBuffer is the owner of buf after the call. The core keeps no
// reference and frees nothing.
//
// 2. WriteBuffer must call buf.Free one time, on each path. This includes the
// path that discards the entry. A path without a Free loses the bytes from
// the pool. A path with two calls of Free gives one buffer to two callers.
//
// 3. WriteBuffer must not keep buf after Free, and must not keep the result
// of buf.Bytes.
//
// A sink that does not implement this interface continues to work. The core
// then writes the bytes and frees the buffer, as the original does.
type BufferWriteSyncer interface {
	WriteSyncer

	// WriteBuffer writes one encoded entry and frees buf.
	WriteBuffer(buf *buffer.Buffer) error
}

// NewCore creates a Core that writes logs to a WriteSyncer.
//
// If ws implements BufferWriteSyncer, the core gives the buffer of the
// encoder to ws and ws frees it. If ws does not, the core writes the bytes
// and frees the buffer.
func NewCore(enc Encoder, ws WriteSyncer, enab LevelEnabler) Core {
	core := &ioCore{
		LevelEnabler: enab,
		enc:          enc,
		out:          ws,
	}

	// Make the test of the type one time, and not one time for each entry.
	core.bufOut, _ = ws.(BufferWriteSyncer)
	return core
}

type ioCore struct {
	LevelEnabler
	enc Encoder
	out WriteSyncer

	// bufOut is out, if out accepts the ownership of the buffer. If out does
	// not accept it, bufOut is nil.
	bufOut BufferWriteSyncer
}

var _ Core = (*ioCore)(nil)

func (c *ioCore) Level() Level {
	return zapcore.LevelOf(c.LevelEnabler)
}

func (c *ioCore) With(fields []Field) Core {
	clone := c.clone()
	addFields(clone.enc, fields)
	return clone
}

func (c *ioCore) Check(ent Entry, ce *CheckedEntry) *CheckedEntry {
	if c.Enabled(ent.Level) {
		return ce.AddCore(ent, c)
	}
	return ce
}

func (c *ioCore) Write(ent Entry, fields []Field) error {
	buf, err := c.enc.EncodeEntry(ent, fields)
	if err != nil {
		return err
	}

	if c.bufOut != nil {
		// The ownership of buf moves to the sink. The sink frees buf after
		// the write, so no free is here.
		err = c.bufOut.WriteBuffer(buf)
	} else {
		_, err = c.out.Write(buf.Bytes())
		buf.Free()
	}
	if err != nil {
		return err
	}

	if ent.Level > ErrorLevel {
		// Since we may be crashing the program, sync the output.
		// Ignore Sync errors, pending a clean solution to issue #370.
		_ = c.Sync()
	}
	return nil
}

func (c *ioCore) Sync() error {
	return c.out.Sync()
}

func (c *ioCore) clone() *ioCore {
	return &ioCore{
		LevelEnabler: c.LevelEnabler,
		enc:          c.enc.Clone(),
		out:          c.out,
		bufOut:       c.bufOut,
	}
}
