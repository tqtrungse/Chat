/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package protocol

import (
	"bytes"
	"testing"

	slicepool "xxx/pkg/pool/slice"

	"github.com/stretchr/testify/require"
)

func TestFrameRoundTripAndOwnsBody(t *testing.T) {
	for _, tc := range []struct {
		name       string
		bodySize   int
		maxFrame   int
		wantPrefix []byte
	}{
		{"smallest body", 1, 1, []byte{1, 0}},
		{"little endian length", 258, 258, []byte{2, 1}},
		{"configured limit excludes prefix", 512, 512, []byte{0, 2}},
		{"wire limit excludes prefix", 65535, 65535, []byte{255, 255}},
		{"configured limit above wire limit", 65535, 65536, []byte{255, 255}},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				body := make([]byte, tc.bodySize)
				for i := range body {
					body[i] = byte(i)
				}
				wantBody := bytes.Clone(body)
				frame, err := EncodeFrame(body, tc.maxFrame)
				defer func() {
					clear(frame[:cap(frame)])
					slicepool.Put(frame)
				}()
				require.NoError(t, err)
				require.Len(t, frame, 2+tc.bodySize)
				require.Equal(t, tc.wantPrefix, frame[:2])
				require.Equal(t, wantBody, body, "encoding must not change caller data")

				clear(body)
				body[0] = 0xFF
				decoded, err := FrameBody(frame, tc.maxFrame)
				require.NoError(t, err)
				require.Equal(t, wantBody, decoded, "frame must own a copy of body")
			},
		)
	}
}

func TestEncodeFrameRejectsInvalidSize(t *testing.T) {
	for _, tc := range []struct {
		name     string
		body     []byte
		maxFrame int
	}{
		{"nil body", nil, 65535},
		{"empty body", []byte{}, 65535},
		{"exceeds wire limit", make([]byte, 65536), 65536},
		{"exceeds configured limit", make([]byte, 513), 512},
		{"zero limit", []byte{1}, 0},
		{"negative limit", []byte{1}, -1},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				frame, err := EncodeFrame(tc.body, tc.maxFrame)
				defer func() {
					clear(frame[:cap(frame)])
					slicepool.Put(frame)
				}()
				require.ErrorIs(t, err, ErrPkgTooLarge)
				require.Nil(t, frame)
			},
		)
	}
}

func TestFrameBodyRejectsInvalidFrame(t *testing.T) {
	for _, tc := range []struct {
		name     string
		frame    []byte
		maxFrame int
	}{
		{"nil frame", nil, 65535},
		{"empty frame", []byte{}, 65535},
		{"truncated prefix", []byte{1}, 65535},
		{"empty body", []byte{0, 0}, 65535},
		{"missing body", []byte{1, 0}, 65535},
		{"zero length with body", []byte{0, 0, 1}, 65535},
		{"truncated body", []byte{2, 0, 1}, 65535},
		{"trailing bytes", []byte{1, 0, 1, 2}, 65535},
		{"exceeds configured limit", []byte{2, 0, 1, 2}, 1},
		{"zero limit", []byte{1, 0, 1}, 0},
		{"negative limit", []byte{1, 0, 1}, -1},
		{"exceeds wire limit", make([]byte, 2+65536), 65536},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				body, err := FrameBody(tc.frame, tc.maxFrame)
				require.ErrorIs(t, err, ErrPkgSizeInvalid)
				require.Nil(t, body)
			},
		)
	}
}

func TestFrameBodyReturnsView(t *testing.T) {
	frame := []byte{3, 0, 1, 2, 3}
	body, err := FrameBody(frame, 3)
	require.NoError(t, err)
	require.Equal(t, []byte{1, 2, 3}, body)

	body[0] = 4
	require.Equal(t, []byte{3, 0, 4, 2, 3}, frame)
	frame[4] = 5
	require.Equal(t, []byte{4, 2, 5}, body)
}
