/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package buffer

import (
	"bytes"
	crand "crypto/rand"
	"io"
	"math"
	"math/rand"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/require"
)

func TestLinkedListBuffer_Basic(t *testing.T) {
	const maxBlocks = 100

	var (
		llb LinkedList
		cum int
		buf bytes.Buffer
	)

	for range maxBlocks {
		n := rand.Intn(1024) + 128
		cum += n
		data := make([]byte, n)
		_, err := crand.Read(data)
		require.NoError(t, err)

		llb.PushBack(data)
		buf.Write(data)
	}
	require.Equal(t, maxBlocks, llb.Len())
	require.Equal(t, cum, llb.Buffered())

	bs, err := llb.Peek(cum / 4)
	require.NoError(t, err)

	var p []byte
	for _, b := range bs {
		p = append(p, b...)
	}
	pn := len(p)
	require.Equal(t, pn, cum/4)
	require.Equal(t, buf.Bytes()[:pn], p)

	tmpA := make([]byte, cum/16)
	tmpB := make([]byte, cum/16)
	_, err = crand.Read(tmpA)
	require.NoError(t, err)

	_, err = crand.Read(tmpB)
	require.NoError(t, err)

	bs, err = llb.PeekWithBytes(cum/4, tmpA, tmpB)
	require.NoError(t, err)

	p = p[:0]
	for _, b := range bs {
		p = append(p, b...)
	}
	pn = len(p)
	require.Equal(t, pn, cum/4)

	var tmpBuf bytes.Buffer
	tmpBuf.Write(tmpA)
	tmpBuf.Write(tmpB)
	tmpBuf.Write(buf.Bytes()[:pn-len(tmpA)-len(tmpB)])
	require.Equal(t, tmpBuf.Bytes(), p)

	pm, _ := llb.Discard(pn)
	buf.Next(pm)
	p = make([]byte, cum-pm)
	n, err := llb.Read(p)
	require.NoError(t, err)
	require.Equal(t, cum-pm, n)
	require.Equal(t, buf.Bytes(), p)
	require.True(t, llb.IsEmpty())
}

func TestLinkedListBuffer_ReadFrom(t *testing.T) {
	const dataLen = 4 * 1024

	data := make([]byte, dataLen)
	_, err := crand.Read(data)
	require.NoError(t, err)

	var llb LinkedList
	r := bytes.NewReader(data)
	n, err := llb.ReadFrom(r)
	require.NoError(t, err)
	require.Equal(t, dataLen, int(n))
	require.Equal(t, dataLen, llb.Buffered())

	llb.Reset()
	const headLen = 256
	head := make([]byte, headLen)
	_, err = crand.Read(head)
	require.NoError(t, err)

	llb.PushBack(head)
	_, err = crand.Read(data)
	require.NoError(t, err)

	r.Reset(data)
	n, err = llb.ReadFrom(r)
	require.NoError(t, err)
	require.Equal(t, dataLen, int(n))
	require.Equal(t, headLen+dataLen, llb.Buffered())

	var m int
	buf := make([]byte, headLen+dataLen)
	m, err = llb.Read(buf)
	require.NoError(t, err)
	require.Equal(t, headLen+dataLen, m)
	require.Equal(t, append(head, data...), buf)
	require.True(t, llb.IsEmpty())
}

func TestLinkedListBuffer_WriteTo(t *testing.T) {
	const maxBlocks = 20

	var (
		llb LinkedList
		cum int
		buf bytes.Buffer
	)

	for range maxBlocks {
		n := rand.Intn(1024) + 128
		cum += n
		data := make([]byte, n)
		_, err := crand.Read(data)
		require.NoError(t, err)

		llb.PushBack(data)
		buf.Write(data)
	}
	require.Equal(t, maxBlocks, llb.Len())
	require.Equal(t, cum, llb.Buffered())

	newBuf := bytes.NewBuffer(nil)
	n, err := llb.WriteTo(newBuf)
	require.NoError(t, err)
	require.Equal(t, int64(cum), n)
	require.Equal(t, buf.Bytes(), newBuf.Bytes())

	llb.Reset()
	buf.Reset()
	newBuf.Reset()
	cum = 0
	for range maxBlocks {
		n := rand.Intn(1024) + 128
		cum += n
		data := make([]byte, n)
		_, err := crand.Read(data)
		require.NoError(t, err)
		llb.PushBack(data)
		buf.Write(data)
	}
	require.Equal(t, maxBlocks, llb.Len())
	require.Equal(t, cum, llb.Buffered())

	var discarded int
	discarded, err = llb.Discard(cum / 2)
	require.NoError(t, err)

	buf.Next(discarded)
	n, err = llb.WriteTo(newBuf)
	require.NoError(t, err)
	require.Equal(t, cum-discarded, int(n))
	require.Equal(t, buf.Bytes(), newBuf.Bytes())

	llb.Reset()
	buf.Reset()
	newBuf.Reset()
}

func TestLinkedListBuffer_EmptyList(t *testing.T) {
	var llb LinkedList
	require.True(t, llb.IsEmpty())
	require.Equal(t, 0, llb.Len())
	require.Equal(t, 0, llb.Buffered())
	require.Nil(t, llb.Pop())

	n, err := llb.Read(make([]byte, 10))
	require.Equal(t, 0, n)
	require.ErrorIs(t, err, io.EOF)

	written, err := llb.WriteTo(&bytes.Buffer{})
	require.NoError(t, err)
	require.Equal(t, int64(0), written)

	discarded, err := llb.Discard(5)
	require.NoError(t, err)
	require.Equal(t, 0, discarded)

	bs, err := llb.Peek(0)
	require.NoError(t, err)
	require.Equal(t, 0, len(bs))
}

func TestLinkedListBuffer_ZeroLengthInputsAreNoops(t *testing.T) {
	var llb LinkedList
	llb.PushBack(nil)
	llb.PushBack([]byte{})
	llb.PushFront(nil)
	llb.Append(nil, false)
	llb.Append(nil, true)
	require.True(t, llb.IsEmpty())
	require.Equal(t, 0, llb.Len())
	require.Equal(t, 0, llb.Buffered())
}

func TestLinkedListBuffer_PushFrontOrdering(t *testing.T) {
	var llb LinkedList
	llb.PushBack([]byte("BB"))
	llb.PushBack([]byte("CC"))
	llb.PushFront([]byte("AA")) // must land before BB, not after

	out := make([]byte, 6)
	n, err := llb.Read(out)
	require.NoError(t, err)
	require.Equal(t, 6, n)
	require.Equal(t, []byte("AABBCC"), out)
}

func TestLinkedListBuffer_Pop(t *testing.T) {
	var llb LinkedList
	llb.PushBack([]byte("AA"))
	llb.PushBack([]byte("BB"))
	llb.PushBack([]byte("CC"))
	require.Equal(t, 3, llb.Len())

	require.Equal(t, []byte("AA"), llb.Pop())
	require.Equal(t, []byte("BB"), llb.Pop())
	require.Equal(t, 1, llb.Len())
	require.Equal(t, []byte("CC"), llb.Pop())
	require.True(t, llb.IsEmpty())
	require.Nil(t, llb.Pop())
}

// AllocNode + Append is the documented zero-copy write path (as opposed to
// PushBack/PushFront, which always copy). Mutating the slice after Append
// must be visible through the list -- that's the only black-box way to
// prove no internal copy happened.
func TestLinkedListBuffer_AllocNodeAppendRoundTrip(t *testing.T) {
	var llb LinkedList
	buf := llb.AllocNode(8)
	require.Len(t, buf, 8)
	for i := range buf {
		buf[i] = byte('a' + i)
	}
	llb.Append(buf, false)
	require.Equal(t, 8, llb.Buffered())

	buf[0] = 'Z' // mutate after Append
	out := make([]byte, 8)
	n, err := llb.Read(out)
	require.NoError(t, err)
	require.Equal(t, 8, n)
	require.Equal(t, byte('Z'), out[0]) // must see the mutation
}

// Append with clone set to true copies p. A change to p after Append must not change the data in the list.
// The caller keeps p, so p can be memory that does not come from slicepool.
func TestLinkedListBuffer_AppendCloneCopies(t *testing.T) {
	var llb LinkedList
	buf := []byte("abcdefgh")
	llb.Append(buf, true)
	require.Equal(t, 8, llb.Buffered())

	buf[0] = 'Z'
	out := make([]byte, 8)
	n, err := llb.Read(out)
	require.NoError(t, err)
	require.Equal(t, 8, n)
	require.Equal(t, []byte("abcdefgh"), out)
}

func TestLinkedListBuffer_PeekZeroMeansAll(t *testing.T) {
	var llb LinkedList
	llb.PushBack([]byte("hello "))
	llb.PushBack([]byte("world"))

	bs, err := llb.Peek(0)
	require.NoError(t, err)

	var got []byte
	for _, b := range bs {
		got = append(got, b...)
	}
	require.Equal(t, []byte("hello world"), got)
	// Peek must not consume anything.
	require.Equal(t, 11, llb.Buffered())
	require.False(t, llb.IsEmpty())
}

func TestLinkedListBuffer_PeekShortBuffer(t *testing.T) {
	var llb LinkedList
	llb.PushBack(make([]byte, 10))
	_, err := llb.Peek(11)
	require.ErrorIs(t, err, io.ErrShortBuffer)
}

func TestLinkedListBuffer_DiscardMoreThanBuffered(t *testing.T) {
	var llb LinkedList
	llb.PushBack(make([]byte, 10))
	llb.PushBack(make([]byte, 20))

	discarded, err := llb.Discard(1000)
	require.NoError(t, err)
	require.Equal(t, 30, discarded)
	require.True(t, llb.IsEmpty())
}

// shortWriter accepts fewer bytes than given but returns a nil error --
// LinkedList.WriteTo has explicit code for this exact case (as opposed to
// a short write bundled with a non-nil error, see the regression test
// below), so it should already work; this locks that path in.
type shortWriter struct {
	accept int
}

func (w *shortWriter) Write(p []byte) (int, error) {
	n := min(w.accept, len(p))
	return n, nil
}

func TestLinkedListBuffer_WriteToShortWriteRequeuesRemainder(t *testing.T) {
	var llb LinkedList
	llb.PushBack([]byte("0123456789"))

	w := &shortWriter{accept: 4}
	n, err := llb.WriteTo(w)
	require.ErrorIs(t, err, io.ErrShortWrite)
	require.Equal(t, int64(4), n)
	require.Equal(t, 6, llb.Buffered())

	out := make([]byte, 6)
	m, rerr := llb.Read(out)
	require.NoError(t, rerr)
	require.Equal(t, 6, m)
	require.Equal(t, []byte("456789"), out)
}

// PeekWithBytes is documented to treat bs... as a prefix placed before the
// list's own data (matching how elastic.Buffer.Peek calls it: with the
// ring-buffer's already-peeked head/tail as bs, and the *total* desired
// length as maxBytes). But its bounds check only compares maxBytes against
// ll.Buffered(), ignoring len(bs) entirely, so it spuriously rejects any
// maxBytes that falls between ll.Buffered() and len(bs)+ll.Buffered() --
// which is the normal case whenever the ring-buffer portion covers only
// part of the request.
func TestLinkedListBuffer_PeekWithBytes_MustCountPrefixBytes(t *testing.T) {
	var llb LinkedList
	llb.PushBack(make([]byte, 50)) // only 50 bytes actually in the list

	prefixA := make([]byte, 60)
	prefixB := make([]byte, 40)
	// total available = 60 + 40 + 50 = 150; asking for 120 must succeed,
	// even though 120 > llb.Buffered() (50) taken alone.
	bs, err := llb.PeekWithBytes(120, prefixA, prefixB)
	require.NoError(t, err)

	var total int
	for _, b := range bs {
		total += len(b)
	}
	require.Equal(t, 120, total)
}

// partialErrWriter returns a partial count together with a non-nil error,
// which io.Writer implementations are explicitly permitted to do.
type partialErrWriter struct {
	accept int
	err    error
}

func (w *partialErrWriter) Write(p []byte) (int, error) {
	n := min(w.accept, len(p))
	return n, w.err
}

var errBoom = boomError{}

type boomError struct{}

func (boomError) Error() string {
	return "boom"
}

// When the writer returns (partial n, non-nil err) for a node, WriteTo's
// "if err != nil" branch returns before ever checking whether the write
// was short, so the unwritten remainder of that node is dropped instead
// of being requeued the way the "short write, nil err" branch (tested
// above) already does correctly. The bytes are gone: not written to w,
// not left in the list for a retry, yet still counted in the returned n.
func TestLinkedListBuffer_WriteTo_MustPreserveRemainderOnError(t *testing.T) {
	var llb LinkedList
	llb.PushBack(make([]byte, 100))

	w := &partialErrWriter{
		accept: 40,
		err:    errBoom,
	}
	_, err := llb.WriteTo(w)
	require.Error(t, err)
	require.Equal(t, 60, llb.Buffered())
}

// dataThenEOFReader delivers its final chunk of real data together with
// io.EOF in the same Read call. The io.Reader contract explicitly allows
// this ("it may return the error from the same call"), but bytes.Reader
// (used by the existing ReadFrom test) never does it, which is why this
// gap wasn't caught before.
type dataThenEOFReader struct {
	data []byte
	sent bool
}

func (r *dataThenEOFReader) Read(p []byte) (int, error) {
	if r.sent {
		return 0, io.EOF
	}
	n := copy(p, r.data)
	r.sent = true
	return n, io.EOF
}

// When Read returns (m>0, io.EOF) in one call, ReadFrom adds m to the
// returned n but then frees that chunk via slicepool.Put without ever
// pushing it onto the list -- the final chunk is silently lost even
// though the return value claims it was read.
func TestLinkedListBuffer_ReadFrom_MustNotDropFinalChunkWithEOF(t *testing.T) {
	data := make([]byte, 100)
	_, _ = crand.Read(data)
	r := &dataThenEOFReader{data: data}

	var llb LinkedList
	n, err := llb.ReadFrom(r)
	require.NoError(t, err)
	require.Equal(t, int64(100), n)
	require.Equal(t, 100, llb.Buffered())
}

// ------------------------------------------------------------
// Peek with n <= 0 returns all bytes. There is no MaxInt32 value.
// ------------------------------------------------------------

func TestLinkedListBuffer_PeekNegativeMeansAll(t *testing.T) {
	var llb LinkedList
	llb.PushBack([]byte("hello "))
	llb.PushBack([]byte("world"))

	bs, err := llb.Peek(-1)
	require.NoError(t, err)
	require.Equal(t, []byte("hello world"), bytes.Join(bs, nil))
}

func TestLinkedListBuffer_PeekEmpty(t *testing.T) {
	var llb LinkedList
	bs, err := llb.Peek(0)
	require.NoError(t, err)
	require.Empty(t, bs)

	bs, err = llb.PeekWithBytes(-1)
	require.NoError(t, err)
	require.Empty(t, bs)
}

func TestLinkedListBuffer_PeekMaxInt32IsShortBuffer(t *testing.T) {
	var llb LinkedList
	llb.PushBack([]byte("abc"))
	_, err := llb.Peek(math.MaxInt32)
	require.ErrorIs(t, err, io.ErrShortBuffer)
}

func TestLinkedListBuffer_PeekWithBytesNegativeMeansAll(t *testing.T) {
	var llb LinkedList
	llb.PushBack([]byte("world"))

	bs, err := llb.PeekWithBytes(-1, []byte("hello "), nil)
	require.NoError(t, err)
	require.Equal(t, []byte("hello world"), bytes.Join(bs, nil))
}

func TestLinkedListBuffer_PeekWithBytesLimitCountsPrefix(t *testing.T) {
	var llb LinkedList
	llb.PushBack([]byte("world"))

	_, err := llb.PeekWithBytes(12, []byte("hello "))
	require.ErrorIs(t, err, io.ErrShortBuffer)

	bs, err := llb.PeekWithBytes(8, []byte("hello "))
	require.NoError(t, err)
	require.Equal(t, []byte("hello wo"), bytes.Join(bs, nil))
}

// ------------------------------------------------------------------
// ReadFrom: the Read size grows, and a small Read gets a small buffer.
// ------------------------------------------------------------------

func TestLinkedListBuffer_ReadFromLargeSourceGivesFewNodes(t *testing.T) {
	data := make([]byte, 1<<20)
	_, _ = crand.Read(data)

	var llb LinkedList
	n, err := llb.ReadFrom(bytes.NewReader(data))
	require.NoError(t, err)
	require.Equal(t, int64(len(data)), n)
	// 512 + 1K + ... + 32K is less than 64K. Thus, 1MB needs fewer than 7 + 1MB/64KB nodes.
	require.Less(t, llb.Len(), 7+len(data)/listMaxRead+1)

	out := make([]byte, len(data))
	_, err = llb.Read(out)
	require.NoError(t, err)
	require.Equal(t, data, out)
}

func TestLinkedListBuffer_ReadFromSmallReadGetsSmallBuffer(t *testing.T) {
	var llb LinkedList
	_, err := llb.ReadFrom(iotest.OneByteReader(bytes.NewReader([]byte("abc"))))
	require.NoError(t, err)
	require.Equal(t, 3, llb.Len())
	for iter := llb.head; iter != nil; iter = iter.next {
		require.Equal(t, 1, cap(iter.buf))
	}
}

// tooLongReader returns a count that is more than len(p). This breaks the io.Reader contract.
type tooLongReader struct{}

func (tooLongReader) Read(p []byte) (int, error) { return len(p) + 1, nil }

func TestLinkedListBuffer_ReadFromInvalidCountPanics(t *testing.T) {
	var llb LinkedList
	require.Panics(t, func() { _, _ = llb.ReadFrom(tooLongReader{}) })
}

func TestCoreRing_ReadFromInvalidCountPanics(t *testing.T) {
	rb := newCoreRing(0)
	require.Panics(t, func() { _, _ = rb.ReadFrom(tooLongReader{}) })
}

// ---------------------------------------------------------------------------
// A partial consume moves the offset. The node keeps the slice from slicepool.
// ---------------------------------------------------------------------------

func newFilledList(t *testing.T, data []byte) *LinkedList {
	t.Helper()
	var llb LinkedList
	_, err := llb.ReadFrom(bytes.NewReader(data))
	require.NoError(t, err)
	require.Equal(t, 1, llb.Len())
	return &llb
}

func TestLinkedListBuffer_PartialDiscardKeepsPoolSlice(t *testing.T) {
	data := []byte("0123456789abcdefghij")
	llb := newFilledList(t, data)
	poolCap := cap(llb.head.buf)

	d, err := llb.Discard(10)
	require.NoError(t, err)
	require.Equal(t, 10, d)
	require.Equal(t, poolCap, cap(llb.head.buf), "the node must keep the slice that slicepool gave")
	require.Equal(t, 10, llb.Buffered())

	bs, err := llb.Peek(-1)
	require.NoError(t, err)
	require.Equal(t, data[10:], bytes.Join(bs, nil))
}

func TestLinkedListBuffer_PartialReadKeepsPoolSlice(t *testing.T) {
	data := []byte("0123456789abcdefghij")
	llb := newFilledList(t, data)
	poolCap := cap(llb.head.buf)

	out := make([]byte, 4)
	n, err := llb.Read(out)
	require.NoError(t, err)
	require.Equal(t, 4, n)
	require.Equal(t, poolCap, cap(llb.head.buf))
	require.Equal(t, data[4:], llb.Pop())
}

// limitWriter keeps the bytes that it writes. It writes only limit bytes in each call.
type limitWriter struct {
	bytes.Buffer
	limit int
}

func (w *limitWriter) Write(p []byte) (int, error) {
	return w.Buffer.Write(p[:min(len(p), w.limit)])
}

func TestLinkedListBuffer_PartialWriteToKeepsPoolSlice(t *testing.T) {
	data := []byte("0123456789abcdefghij")
	llb := newFilledList(t, data)
	poolCap := cap(llb.head.buf)

	w := &limitWriter{limit: 7}
	n, err := llb.WriteTo(w)
	require.ErrorIs(t, err, io.ErrShortWrite)
	require.Equal(t, int64(7), n)
	require.Equal(t, poolCap, cap(llb.head.buf))

	w.limit = len(data)
	_, err = llb.WriteTo(w)
	require.NoError(t, err)
	require.Equal(t, data, w.Bytes())
}

// zeroReader returns 0, nil in each call. This breaks the advice of io.Reader.
type zeroReader struct{ calls int }

func (z *zeroReader) Read([]byte) (int, error) {
	z.calls++
	return 0, nil
}

func TestLinkedListBuffer_ReadFromNoProgress(t *testing.T) {
	var llb LinkedList
	z := &zeroReader{}
	n, err := llb.ReadFrom(z)
	require.ErrorIs(t, err, io.ErrNoProgress)
	require.Zero(t, n)
	require.Equal(t, maxConsecutiveEmptyReads, z.calls)
	require.True(t, llb.IsEmpty())
}
