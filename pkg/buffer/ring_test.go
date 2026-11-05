/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package buffer

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// Ring and Elastic follow the io conventions. Then io.ReadAll and io.Copy work on them.

func TestRing_ReadEmptyP_ReturnsNil(t *testing.T) {
	var rb Ring
	n, err := rb.Read(nil)
	require.NoError(t, err)
	require.Zero(t, n)
}

func TestRing_ReadNoData_ReturnsEOF(t *testing.T) {
	var rb Ring
	_, err := rb.Read(make([]byte, 8))
	require.ErrorIs(t, err, io.EOF)
	_, err = rb.ReadByte()
	require.ErrorIs(t, err, io.EOF)
}

func TestRing_ReadAll(t *testing.T) {
	var rb Ring
	_, _ = rb.WriteString("hello")
	got, err := io.ReadAll(&rb)
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), got)
}

func TestRing_EmptyWriteToAndDiscard_ReturnNil(t *testing.T) {
	var rb Ring
	var dst bytes.Buffer
	n, err := io.Copy(&dst, &rb)
	require.NoError(t, err)
	require.Zero(t, n)

	d, err := rb.Discard(10)
	require.NoError(t, err)
	require.Zero(t, d)
}

func TestElastic_ReadEmptyP_ReturnsNil(t *testing.T) {
	e, _ := New(64)
	n, err := e.Read(nil)
	require.NoError(t, err)
	require.Zero(t, n)
}

func TestElastic_New_InvalidSize(t *testing.T) {
	for _, size := range []int{0, -1} {
		_, err := New(size)
		require.ErrorIs(t, err, ErrElasticInvalidSize)
	}
}

func TestRing_ReadFromNoDataReturnsRingToPool(t *testing.T) {
	var rb Ring
	n, err := rb.ReadFrom(bytes.NewReader(nil))
	require.NoError(t, err)
	require.Zero(t, n)
	require.Nil(t, rb.rb, "an empty ring must go back to the pool")
}

func TestRing_ReadFromKeepsRingWithData(t *testing.T) {
	var rb Ring
	_, err := rb.ReadFrom(bytes.NewReader([]byte("abc")))
	require.NoError(t, err)
	require.NotNil(t, rb.rb)
	require.Equal(t, []byte("abc"), rb.Bytes())
}

func TestRing_ReadFromNoProgress(t *testing.T) {
	var rb Ring
	z := &zeroReader{}
	_, err := rb.ReadFrom(z)
	require.ErrorIs(t, err, io.ErrNoProgress)
	require.Equal(t, maxConsecutiveEmptyReads, z.calls)
	require.Nil(t, rb.rb)
}

func TestElastic_ReadFromNoProgress(t *testing.T) {
	e, _ := New(4096)
	_, err := e.ReadFrom(&zeroReader{})
	require.ErrorIs(t, err, io.ErrNoProgress)
	require.True(t, e.IsEmpty())
}

// A reader that gives some data between empty reads is not stopped. The count starts again after each data.
func TestRing_ReadFromEmptyReadsBetweenData(t *testing.T) {
	var rb Ring
	r := &gapReader{data: []byte("abcdef"), gap: maxConsecutiveEmptyReads - 1}
	n, err := rb.ReadFrom(r)
	require.NoError(t, err)
	require.Equal(t, int64(6), n)
	require.Equal(t, []byte("abcdef"), rb.Bytes())
}

// gapReader returns gap empty reads before each byte, and io.EOF at the end.
type gapReader struct {
	data  []byte
	gap   int
	empty int
}

func (g *gapReader) Read(p []byte) (int, error) {
	if len(g.data) == 0 {
		return 0, io.EOF
	}
	if g.empty < g.gap {
		g.empty++
		return 0, nil
	}
	g.empty = 0
	p[0] = g.data[0]
	g.data = g.data[1:]
	return 1, nil
}
