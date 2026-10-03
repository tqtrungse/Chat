/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package buffer

import (
	"bytes"
	crand "crypto/rand"
	"io"
	"math/rand"
	"testing"

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
	var llb LinkedList
	const dataLen = 4 * 1024
	data := make([]byte, dataLen)
	_, err := crand.Read(data)
	require.NoError(t, err)
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
	buf := make([]byte, headLen+dataLen)
	var m int
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
	llb.Append(nil)
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
	llb.Append(buf)
	require.Equal(t, 8, llb.Buffered())

	buf[0] = 'Z' // mutate after Append
	out := make([]byte, 8)
	n, err := llb.Read(out)
	require.NoError(t, err)
	require.Equal(t, 8, n)
	require.Equal(t, byte('Z'), out[0]) // must see the mutation
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
type shortWriter struct{ accept int }

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

func (boomError) Error() string { return "boom" }

// When the writer returns (partial n, non-nil err) for a node, WriteTo's
// "if err != nil" branch returns before ever checking whether the write
// was short, so the unwritten remainder of that node is dropped instead
// of being requeued the way the "short write, nil err" branch (tested
// above) already does correctly. The bytes are gone: not written to w,
// not left in the list for a retry, yet still counted in the returned n.
func TestLinkedListBuffer_WriteTo_MustPreserveRemainderOnError(t *testing.T) {
	var llb LinkedList
	llb.PushBack(make([]byte, 100))

	w := &partialErrWriter{accept: 40, err: errBoom}
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
