/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package buffer

import (
	"bytes"
	crand "crypto/rand"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func randBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = crand.Read(b)
	return b
}

// ------------
// Construction
// ------------

func TestElastic_New_RejectsZero(t *testing.T) {
	_, err := New(0)
	require.Error(t, err)
}

func TestElastic_New_FreshIsEmpty(t *testing.T) {
	e, err := New(1024)
	require.NoError(t, err)
	require.True(t, e.IsEmpty())
	require.Zero(t, e.Buffered())
}

// ---------------------------------------------------------------------
// Write routing: ring first, spill to list once the cap is reached, and
// stay spilling (sticky) until the list fully drains again.
// ---------------------------------------------------------------------

func TestElastic_Write_StaysInRingUnderCap(t *testing.T) {
	e, _ := New(4096)
	data := randBytes(1000)
	n, err := e.Write(data)
	require.NoError(t, err)
	require.Equal(t, 1000, n)
	require.True(t, e.list.IsEmpty())
	require.Equal(t, 1000, e.ring.Buffered())
}

func TestElastic_Write_SplitsAtCapThenSpillsToList(t *testing.T) {
	e, _ := New(1000)
	first := randBytes(1000) // fills the ring exactly to the cap
	_, err := e.Write(first)
	require.NoError(t, err)
	require.True(t, e.list.IsEmpty())

	second := randBytes(500) // ring already at cap -> all 500 must go to list
	n, err := e.Write(second)
	require.NoError(t, err)
	require.Equal(t, 500, n)
	require.False(t, e.list.IsEmpty())
	require.Equal(t, 500, e.list.Buffered())
	require.Equal(t, 1500, e.Buffered())
}

func TestElastic_Write_StickySpillEvenIfRingDrainsBelowCap(t *testing.T) {
	e, _ := New(1000)
	_, _ = e.Write(randBytes(1000)) // ring at cap
	_, _ = e.Write(randBytes(100))  // spills, list now non-empty

	// drain the ring portion down to 0 without touching the list
	buf := make([]byte, 1000)
	_, _ = e.ring.Read(buf)
	require.Zero(t, e.ring.Buffered())

	// even though the ring now has plenty of room, list is still non-empty,
	// so this write must still go to the list, not the ring, to preserve
	// ordering.
	n, err := e.Write(randBytes(50))
	require.NoError(t, err)
	require.Equal(t, 50, n)
	require.Equal(t, 150, e.list.Buffered())
}

// --------------------------------------------
// Read / Peek / Discard spanning both segments
// --------------------------------------------

func TestElastic_Read_SpansRingAndList(t *testing.T) {
	e, _ := New(100)
	ringPart := randBytes(100)
	listPart := randBytes(50)
	_, _ = e.Write(ringPart)
	_, _ = e.Write(listPart) // spills

	out := make([]byte, 150)
	n, err := e.Read(out)
	require.NoError(t, err)
	require.Equal(t, 150, n)

	want := append(append([]byte{}, ringPart...), listPart...)
	require.True(t, bytes.Equal(want, out))
	require.True(t, e.IsEmpty())
}

func TestElastic_Peek_ExactAndPartial(t *testing.T) {
	e, _ := New(100)
	ringPart := randBytes(100)
	listPart := randBytes(50)
	_, _ = e.Write(ringPart)
	_, _ = e.Write(listPart)

	// 0 means "everything".
	bs, err := e.Peek(0)
	require.NoError(t, err)

	var got []byte
	for _, b := range bs {
		got = append(got, b...)
	}
	want := append(append([]byte{}, ringPart...), listPart...)
	require.True(t, bytes.Equal(want, got))
	// Peek must not consume anything.
	require.Equal(t, 150, e.Buffered())

	// partial peek smaller than the ring's own share.
	bs, err = e.Peek(30)
	require.NoError(t, err)

	got = got[:0]
	for _, b := range bs {
		got = append(got, b...)
	}
	require.True(t, bytes.Equal(got, ringPart[:30]))

	// too much: must error, not panic or silently truncate.
	_, err = e.Peek(1000)
	require.Error(t, err)
}

func TestElastic_Discard_SpansRingAndList(t *testing.T) {
	e, _ := New(100)
	_, _ = e.Write(randBytes(100))
	_, _ = e.Write(randBytes(50))

	discarded, err := e.Discard(120)
	require.NoError(t, err)
	require.Equal(t, 120, discarded)
	require.Equal(t, 30, e.Buffered())
}

func TestCoreRing_DiscardNonPositiveIsNoop(t *testing.T) {
	rb := newCoreRing(16)
	_, _ = rb.Write([]byte("abcd"))
	for _, n := range []int{0, -1, -100} {
		d, err := rb.Discard(n)
		require.NoError(t, err)
		require.Zero(t, d)
		require.Zero(t, rb.r)
		require.Equal(t, []byte("abcd"), rb.Bytes())
	}
}

// ----------------------------------------------------------------------
// Writev: split point can fall in the middle of one of the given slices.
// ----------------------------------------------------------------------

func TestElastic_Writev_SplitsMidSlice(t *testing.T) {
	e, _ := New(100)
	a := randBytes(60)
	b := randBytes(80) // this one straddles the 100-byte cap (60+80=140)
	c := randBytes(20)

	n, err := e.Writev([][]byte{a, b, c})
	require.NoError(t, err)
	require.Equal(t, 160, n)
	require.Equal(t, 100, e.ring.Buffered())
	require.Equal(t, 60, e.list.Buffered())

	out := make([]byte, 160)
	rn, rErr := e.Read(out)
	require.NoError(t, rErr)
	require.Equal(t, 160, rn)

	want := append(append(append([]byte{}, a...), b...), c...)
	require.True(t, bytes.Equal(out, want))
}

// --------------------------
// ReadFrom / WriteTo routing
// --------------------------

func TestElastic_ReadFrom_UsesRingUnderCapThenList(t *testing.T) {
	e, _ := New(100)
	r1 := bytes.NewReader(randBytes(50))
	n, err := e.ReadFrom(r1)
	require.NoError(t, err)
	require.Equal(t, int64(50), n)
	require.True(t, e.list.IsEmpty())

	r2 := bytes.NewReader(randBytes(100))
	n, err = e.ReadFrom(r2)
	require.NoError(t, err)
	require.Equal(t, int64(100), n)
	require.Equal(t, 150, e.Buffered())
}

func TestElastic_WriteTo_DrainsRingThenList(t *testing.T) {
	e, _ := New(100)
	ringPart := randBytes(100)
	listPart := randBytes(70)
	_, _ = e.Write(ringPart)
	_, _ = e.Write(listPart)

	var out bytes.Buffer
	n, err := e.WriteTo(&out)
	require.NoError(t, err)
	require.Equal(t, int64(170), n)

	want := append(append([]byte{}, ringPart...), listPart...)
	require.True(t, bytes.Equal(out.Bytes(), want))
	require.True(t, e.IsEmpty())
}

func TestElastic_WriteTo_DrainsListAfterRingIsConsumed(t *testing.T) {
	for _, method := range []string{"Discard", "Read"} {
		t.Run(method, func(t *testing.T) {
			e, err := New(1024)
			require.NoError(t, err)
			t.Cleanup(e.Release)

			ringPart := bytes.Repeat([]byte{'A'}, 1024)
			listPart := []byte("hello")
			_, err = e.Write(ringPart)
			require.NoError(t, err)

			_, err = e.Write(listPart)
			require.NoError(t, err)

			var consumed int
			if method == "Discard" {
				consumed, err = e.Discard(len(ringPart))
			} else {
				out := make([]byte, len(ringPart))
				consumed, err = e.Read(out)
				require.Equal(t, ringPart, out)
			}
			require.NoError(t, err)
			require.Equal(t, len(ringPart), consumed)
			require.True(t, e.ring.IsEmpty())
			require.Equal(t, len(listPart), e.Buffered())

			var out bytes.Buffer
			n, err := e.WriteTo(&out)
			require.NoError(t, err)
			require.Equal(t, int64(len(listPart)), n)
			require.Equal(t, listPart, out.Bytes())
			require.True(t, e.IsEmpty())
		})
	}
}

func TestElastic_WriteTo_Empty(t *testing.T) {
	e, err := New(1024)
	require.NoError(t, err)
	t.Cleanup(e.Release)

	var out bytes.Buffer
	n, err := e.WriteTo(&out)
	require.NoError(t, err)
	require.Zero(t, n)
	require.Zero(t, out.Len())
}

type elasticWriterFunc func([]byte) (int, error)

func (f elasticWriterFunc) Write(p []byte) (int, error) { return f(p) }

func TestElastic_WriteTo_RetriesAfterPartialError(t *testing.T) {
	for _, segment := range []string{"ring", "list"} {
		t.Run(segment, func(t *testing.T) {
			e, err := New(1024)
			require.NoError(t, err)
			t.Cleanup(e.Release)

			ringPart := bytes.Repeat([]byte{'A'}, 1024)
			listPart := []byte("hello")
			_, err = e.Write(ringPart)
			require.NoError(t, err)

			_, err = e.Write(listPart)
			require.NoError(t, err)

			var (
				out      bytes.Buffer
				want     = append(append([]byte(nil), ringPart...), listPart...)
				failAt   = 1
				accepted = 2
				calls    = 0
			)

			if segment == "list" {
				failAt = 2
				accepted += len(ringPart)
			}

			w := elasticWriterFunc(func(p []byte) (int, error) {
				calls++
				if calls == failAt {
					n, err := out.Write(p[:min(2, len(p))])
					if err != nil {
						return n, err
					}
					return n, errBoom
				}
				return out.Write(p)
			})

			n, err := e.WriteTo(w)
			require.ErrorIs(t, err, errBoom)
			require.Equal(t, failAt, calls)
			require.Equal(t, int64(accepted), n)
			require.Equal(t, want[:accepted], out.Bytes())
			require.Equal(t, len(want)-accepted, e.Buffered())

			m, err := e.WriteTo(&out)
			require.NoError(t, err)
			require.Equal(t, int64(len(want)), n+m)
			require.Equal(t, want, out.Bytes())
			require.True(t, e.IsEmpty())

			m, err = e.WriteTo(io.Discard)
			require.NoError(t, err)
			require.Zero(t, m)
		})
	}
}

// ---------------
// Reset / Release
// ---------------

func TestElastic_Reset_ClearsAndOptionallyResizesCap(t *testing.T) {
	e, _ := New(100)
	_, _ = e.Write(randBytes(100))
	_, _ = e.Write(randBytes(50))

	e.Reset(0) // 0 means "keep current cap"
	require.True(t, e.IsEmpty())
	require.Zero(t, e.Buffered())
	require.Equal(t, 100, e.maxStaticBytes)

	e.Reset(4096)
	require.Equal(t, 4096, e.maxStaticBytes)
}

func TestElastic_Release_LeavesBufferEmptyAndReusable(t *testing.T) {
	e, _ := New(100)
	_, _ = e.Write(randBytes(100))
	_, _ = e.Write(randBytes(50))

	e.Release()
	require.True(t, e.IsEmpty())
	require.Zero(t, e.Buffered())

	// must still be usable afterward.
	n, err := e.Write(randBytes(10))
	require.NoError(t, err)
	require.Equal(t, 10, n)
}
