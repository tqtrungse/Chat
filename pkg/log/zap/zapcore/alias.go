/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package zapcore

import (
	"go.uber.org/zap/zapcore"
)

// This package is a partial fork of go.uber.org/zap/zapcore. It forks the
// JSON encoder and the core, because both must know the type of the buffer,
// and this fork changes that type. See FORK.md in the parent directory.
//
// Each type below is an alias of the type of the upstream package, and not a
// copy. Therefore, a value of this package and a value of the upstream
// package are the same type.
//
// The alias has two results. First, a Core of this package satisfies
// zapcore.Core of upstream. Therefore, zap.New, zapcore.NewTee and
// zapcore.NewSamplerWithOptions accept it, and this fork needs no copy of
// those files. Second, each package that logs continues to use zap.String,
// zap.Error and the other functions of upstream. Therefore, the fork changes
// no call site.
//
// Only the Encoder interface is not an alias, because EncodeEntry must return
// the buffer type of this fork. No exported part of zap accepts an Encoder,
// so this difference stays inside the package log.
type (
	// Entry, Field and the two level types travel between zap and a Core.
	// Therefore, they must be the types of upstream.
	Entry        = zapcore.Entry
	EntryCaller  = zapcore.EntryCaller
	Field        = zapcore.Field
	Level        = zapcore.Level
	LevelEnabler = zapcore.LevelEnabler
	CheckedEntry = zapcore.CheckedEntry

	// Core and WriteSyncer are the two interfaces that connect this fork to
	// upstream zap.
	Core        = zapcore.Core
	WriteSyncer = zapcore.WriteSyncer

	// The encoder interfaces and the configuration stay the types of
	// upstream, so a Field of upstream can add itself to the encoder of this
	// fork.
	EncoderConfig         = zapcore.EncoderConfig
	ObjectEncoder         = zapcore.ObjectEncoder
	ArrayEncoder          = zapcore.ArrayEncoder
	PrimitiveArrayEncoder = zapcore.PrimitiveArrayEncoder
	ObjectMarshaler       = zapcore.ObjectMarshaler
	ArrayMarshaler        = zapcore.ArrayMarshaler
	ReflectedEncoder      = zapcore.ReflectedEncoder
)

const (
	// ErrorLevel is the level above which ioCore flushes the sink.
	ErrorLevel = zapcore.ErrorLevel

	// DefaultLineEnding is the line ending that the forked encoder uses when
	// EncoderConfig gives none.
	DefaultLineEnding = zapcore.DefaultLineEnding
)

// FullNameEncoder is the encoder of the logger name that the forked encoder
// uses when EncoderConfig gives none.
var FullNameEncoder = zapcore.FullNameEncoder
