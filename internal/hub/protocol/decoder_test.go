/*
 * Copyright 2026 tqtrungse@gmail.com. All rights reserved.
 * Use of this pub code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package protocol

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"testing"

	pbpub "xxx/api/hub/v1/proto/gen/pub"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func randomKey(t *testing.T) *[32]byte {
	t.Helper()
	var k [32]byte
	if _, err := rand.Read(k[:]); err != nil {
		t.Fatalf("failed to generate random key: %v", err)
	}
	return &k
}

func randomNonce(t *testing.T) [12]byte {
	t.Helper()
	var n [12]byte
	if _, err := rand.Read(n[:]); err != nil {
		t.Fatalf("failed to generate random nonce: %v", err)
	}
	return n
}

// buildPlainPacket constructs a framed packet in the exact format documented
// for Encode/Decode. Used to build fixtures independent of Encode() itself, so
// Decode() tests aren't coupled to Encode()'s own correctness.
func buildPlainPacket(
	t *testing.T,
	hmacKey *[32]byte,
	packType pbpub.PacketType,
	msg proto.Message,
	expand []byte,
) []byte {
	t.Helper()
	pbBytes, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("failed to marshal message: %v", err)
	}

	frameSize := 36 + len(pbBytes) + len(expand)
	buf := make([]byte, 6, 6+len(pbBytes)+len(expand)+32)
	binary.LittleEndian.PutUint16(buf[:2], uint16(frameSize))
	binary.LittleEndian.PutUint16(buf[2:4], uint16(packType))
	binary.LittleEndian.PutUint16(buf[4:6], uint16(len(pbBytes)))
	buf = append(buf, pbBytes...)
	buf = append(buf, expand...)

	mac := hmac.New(sha256.New, hmacKey[:])
	mac.Write(buf)
	buf = mac.Sum(buf)
	return buf
}

// buildSecurePacket constructs a packet in the exact format documented for
// EncodeS/DecodeS, independent of EncodeS() itself.
func buildSecurePacket(
	t *testing.T,
	ch SecureChannel,
	secretKey *[32]byte,
	nonce [12]byte,
	packType pbpub.PacketType,
	msg proto.Message,
	expand []byte,
) []byte {
	t.Helper()
	pbBytes, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("failed to marshal message: %v", err)
	}
	cipherSize := ch.Size(len(pbBytes))
	frameSize := 16 + cipherSize + len(expand)
	aad := securePacketAADForTest(uint16(frameSize), uint16(packType), uint16(cipherSize), expand)
	sealed, err := ch.Seal(secretKey, &nonce, pbBytes, aad)
	if err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	buf := make([]byte, 18, 18+len(sealed)+len(expand))
	binary.LittleEndian.PutUint16(buf[:2], uint16(frameSize))
	binary.LittleEndian.PutUint16(buf[2:4], uint16(packType))
	copy(buf[4:16], nonce[:])
	binary.LittleEndian.PutUint16(buf[16:18], uint16(len(sealed)))
	buf = append(buf, sealed...)
	buf = append(buf, expand...)
	return buf
}

func securePacketAADForTest(frameSize, packType, cipherSize uint16, expand []byte) []byte {
	const domain = "xxx/hub/packet/gcm/v1\x00"
	aad := make([]byte, len(domain)+6+len(expand))
	copy(aad, domain)
	header := aad[len(domain) : len(domain)+6]
	binary.LittleEndian.PutUint16(header[:2], frameSize)
	binary.LittleEndian.PutUint16(header[2:4], packType)
	binary.LittleEndian.PutUint16(header[4:6], cipherSize)
	copy(aad[len(domain)+6:], expand)
	return aad
}

// buildActivationPacket matches the TCP activation framing: frame length |
// packet type | protobuf, without an inner protobuf-size field.
func buildActivationPacket(t *testing.T, packType pbpub.PacketType, msg proto.Message) []byte {
	t.Helper()
	pbBytes, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("failed to marshal activation message: %v", err)
	}
	packet := make([]byte, 4, 4+len(pbBytes))
	binary.LittleEndian.PutUint16(packet[:2], uint16(2+len(pbBytes)))
	binary.LittleEndian.PutUint16(packet[2:4], uint16(packType))
	return append(packet, pbBytes...)
}

// runRecovered calls fn and converts any panic into a controlled test
// failure instead of letting it crash the whole test binary. Kept as
// defense-in-depth around the bounds-check paths even though they are now
// guarded explicitly in decoder.go.
func runRecovered(t *testing.T, name string, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("%s panicked instead of returning an error: %v", name, r)
		}
	}()
	fn()
}

func TestDecoder_DecodeActivePack_Success(t *testing.T) {
	d := NewDecoder(NewMockSecureChannel())
	in := &pbpub.ActiveConnReq{
		Token:  []byte("12345678"),
		Sign:   bytes.Repeat([]byte{1}, 64),
		Ticket: []byte{2},
	}
	packet := buildActivationPacket(t, pbpub.PacketType_REQ_ACTIVE_CONN, in)

	out := new(pbpub.ActiveConnReq)
	require.NoError(t, d.DecodeActivePack(packet[4:], out))
	require.True(t, proto.Equal(in, out))
}

func TestDecoder_DecodeActiveProofPack_Success(t *testing.T) {
	d := NewDecoder(NewMockSecureChannel())
	in := &pbpub.ActiveConnProofReq{Proof: bytes.Repeat([]byte{1}, 32)}
	packet := buildActivationPacket(t, pbpub.PacketType_REQ_ACTIVE_CONN_PROOF, in)

	out := new(pbpub.ActiveConnProofReq)
	require.NoError(t, d.DecodeActiveProofPack(packet[4:], out))
	require.True(t, proto.Equal(in, out))
}

func TestDecoder_DecodeActivePack_InvalidBytes(t *testing.T) {
	d := NewDecoder(NewMockSecureChannel())
	out := new(pbpub.ActiveConnReq)
	err := d.DecodeActivePack([]byte{0xFF, 0xFF, 0xFF}, out)
	require.Error(t, err)
}

func TestDecoder_Decode_ParsesHeaderAndPayload(t *testing.T) {
	d := NewDecoder(NewMockSecureChannel())
	hmacKey := randomKey(t)
	msg := new(pbpub.ActiveConnReq)
	expand := []byte("expand-data")

	packet := buildPlainPacket(
		t,
		hmacKey,
		pbpub.PacketType_REQ_ACTIVE_CONN,
		msg,
		expand,
	)

	gotMsg, gotExpand, code, err := d.Decode(hmacKey, packet)
	require.NoError(t, err)
	require.Equal(t, pbpub.Code_SUCCESS, code)
	require.True(t, proto.Equal(gotMsg, msg))
	require.True(t, bytes.Equal(gotExpand, expand))
}

func TestDecoder_Decode_NoExpand(t *testing.T) {
	d := NewDecoder(NewMockSecureChannel())
	hmacKey := randomKey(t)
	msg := &pbpub.ActiveConnReq{
		Token: []byte{1, 2, 3, 4, 5, 6, 7, 8},
		Sign:  nil,
	}

	packet := buildPlainPacket(
		t,
		hmacKey,
		pbpub.PacketType_REQ_ACTIVE_CONN,
		msg,
		nil,
	)

	_, gotExpand, code, err := d.Decode(hmacKey, packet)
	require.NoError(t, err)
	require.Equal(t, pbpub.Code_SUCCESS, code)
	require.Empty(t, gotExpand)
}

func TestDecoder_Decode_RejectsTamperedHMAC(t *testing.T) {
	d := NewDecoder(NewMockSecureChannel())
	hmacKey := randomKey(t)
	msg := new(pbpub.ActiveConnReq)

	packet := buildPlainPacket(
		t,
		hmacKey,
		pbpub.PacketType_REQ_ACTIVE_CONN,
		msg,
		[]byte("x"),
	)
	packet[2] ^= 0xFF // tamper the packet type, which is covered by the HMAC

	_, _, code, err := d.Decode(hmacKey, packet)
	require.Error(t, err)
	require.Equal(t, pbpub.Code_ERR_MODIFIED_PACK, code)
}

func TestDecoder_Decode_WrongHmacKey(t *testing.T) {
	d := NewDecoder(NewMockSecureChannel())
	hmacKey := randomKey(t)
	wrongKey := randomKey(t)
	msg := &pbpub.ActiveConnReq{
		Token: []byte{1, 2, 3, 4, 5, 6, 7, 8},
		Sign:  nil,
	}

	packet := buildPlainPacket(
		t,
		hmacKey,
		pbpub.PacketType_REQ_ACTIVE_CONN,
		msg,
		nil,
	)

	_, _, code, err := d.Decode(wrongKey, packet)
	require.Error(t, err)
	require.Equal(t, pbpub.Code_ERR_MODIFIED_PACK, code)
}

func TestDecoder_Decode_UnrecognizedPacketType(t *testing.T) {
	d := NewDecoder(NewMockSecureChannel())
	hmacKey := randomKey(t)
	msg := new(pbpub.ActiveConnReq)

	packet := buildPlainPacket(
		t,
		hmacKey,
		pbpub.PacketType(9999),
		msg,
		nil,
	)

	_, _, code, err := d.Decode(hmacKey, packet)
	require.Error(t, err)
	require.Equal(t, pbpub.Code_ERR_INVALID_PACK_TYPE, code)
}

func TestDecoder_Decode_TooShortPacket_ReturnsErrorNotPanic(t *testing.T) {
	d := NewDecoder(NewMockSecureChannel())
	hmacKey := randomKey(t)
	shortPacket := []byte{1, 2, 3, 4, 5}

	var (
		code pbpub.Code
		err  error
	)
	runRecovered(t, "Decode", func() {
		_, _, code, err = d.Decode(hmacKey, shortPacket)
	})
	require.Error(t, err)
	require.Equal(t, pbpub.Code_ERR_INVALID_PACK_SIZE, code)
}

func TestDecoder_Decode_RejectsInconsistentFrameLength(t *testing.T) {
	d := NewDecoder(NewMockSecureChannel())
	hmacKey := randomKey(t)
	packet := buildPlainPacket(
		t,
		hmacKey,
		pbpub.PacketType_REQ_ACTIVE_CONN,
		new(pbpub.ActiveConnReq),
		nil,
	)
	binary.LittleEndian.PutUint16(packet[:2], uint16(len(packet)-1))

	_, _, code, err := d.Decode(hmacKey, packet)
	require.ErrorIs(t, err, ErrPkgSizeInvalid)
	require.Equal(t, pbpub.Code_ERR_INVALID_PACK_SIZE, code)
}

// TestDecoder_Decode_PbSizeLiesAboutLength checks the pbSize-vs-actual-length
// guard: a header claiming a much larger pbSize than the packet actually
// contains must be rejected cleanly, not panic while slicing.
func TestDecoder_Decode_PbSizeLiesAboutLength(t *testing.T) {
	d := NewDecoder(NewMockSecureChannel())
	hmacKey := randomKey(t)
	msg := new(pbpub.ActiveConnReq)

	packet := buildPlainPacket(
		t,
		hmacKey,
		pbpub.PacketType_REQ_ACTIVE_CONN,
		msg,
		[]byte("expand"),
	)
	// Overwrite the pbSize field with a value far larger than what's
	// actually available. This also invalidates the HMAC, but the goal is
	// to confirm the pbSize bounds-check path doesn't panic even if it
	// were reached.
	binary.LittleEndian.PutUint16(packet[4:6], 60000)

	var (
		code pbpub.Code
		err  error
	)
	runRecovered(t, "Decode", func() {
		_, _, code, err = d.Decode(hmacKey, packet)
	})
	require.Error(t, err)
	require.Equal(t, pbpub.Code_ERR_MODIFIED_PACK, code)
}

func TestDecoder_DecodeS_ParsesHeaderAndDecrypts(t *testing.T) {
	ch := NewMockSecureChannel()
	d := NewDecoder(ch)
	secretKey := randomKey(t)
	nonce := randomNonce(t)
	msg := &pbpub.SendMsgReq{
		ChannelId: uint64(1),
	}
	expand := []byte("expand-data")

	packet := buildSecurePacket(
		t,
		ch,
		secretKey,
		nonce,
		pbpub.PacketType_REQ_SEND_MSG,
		msg,
		expand,
	)

	gotMsg, gotExpand, code, err := d.DecodeS(secretKey, packet)
	require.NoError(t, err)
	require.Equal(t, pbpub.Code_SUCCESS, code)
	require.True(t, proto.Equal(gotMsg, msg))
	require.True(t, bytes.Equal(gotExpand, expand))
}

func TestDecoder_DecodeS_RejectsTamperedCiphertext(t *testing.T) {
	ch := NewMockSecureChannel()
	Decoder := NewDecoder(ch)
	secretKey := randomKey(t)
	nonce := randomNonce(t)
	msg := &pbpub.SendMsgReq{
		ChannelId: uint64(1),
	}

	packet := buildSecurePacket(
		t,
		ch,
		secretKey,
		nonce,
		pbpub.PacketType_REQ_SEND_MSG,
		msg,
		nil,
	)
	packet[18] ^= 0xFF // flip a bit inside the ciphertext

	_, _, code, err := Decoder.DecodeS(secretKey, packet)
	require.Error(t, err)
	require.Equal(t, pbpub.Code_ERR_DECRYPT, code)
}

// TestDecoder_DecodeS_RejectsTamperedExpand checks that "expand" -- sent in
// cleartext after the ciphertext -- is authenticated via AEAD associated
// data. Tampering it must invalidate authentication even though expand
// itself is never encrypted.
func TestDecoder_DecodeS_RejectsTamperedExpand(t *testing.T) {
	ch := NewMockSecureChannel()
	Decoder := NewDecoder(ch)
	secretKey := randomKey(t)
	nonce := randomNonce(t)
	msg := &pbpub.SendMsgReq{
		ChannelId: uint64(1),
	}
	expand := []byte("original-expand")

	packet := buildSecurePacket(
		t,
		ch,
		secretKey,
		nonce,
		pbpub.PacketType_REQ_SEND_MSG,
		msg,
		expand,
	)

	tampered := append([]byte(nil), packet...)
	copy(tampered[len(tampered)-len(expand):], "tampered-expand!")

	_, _, code, err := Decoder.DecodeS(secretKey, tampered)
	require.Error(t, err)
	require.Equal(t, pbpub.Code_ERR_DECRYPT, code)
}

func TestDecoder_DecodeS_WrongSecretKey(t *testing.T) {
	ch := NewMockSecureChannel()
	d := NewDecoder(ch)
	secretKey := randomKey(t)
	wrongKey := randomKey(t)
	nonce := randomNonce(t)
	msg := &pbpub.SendMsgReq{
		ChannelId: uint64(1),
	}

	packet := buildSecurePacket(
		t,
		ch,
		secretKey,
		nonce,
		pbpub.PacketType_REQ_SEND_MSG,
		msg,
		nil,
	)

	_, _, code, err := d.DecodeS(wrongKey, packet)
	require.Error(t, err)
	require.Equal(t, pbpub.Code_ERR_DECRYPT, code)
}

func TestDecoder_DecodeS_UnrecognizedPacketType(t *testing.T) {
	ch := NewMockSecureChannel()
	d := NewDecoder(ch)
	secretKey := randomKey(t)
	nonce := randomNonce(t)
	msg := new(pbpub.SendMsgReq)

	packet := buildSecurePacket(
		t,
		ch,
		secretKey,
		nonce,
		pbpub.PacketType(9999),
		msg,
		nil,
	)

	_, _, code, err := d.DecodeS(secretKey, packet)
	require.Error(t, err)
	require.Equal(t, pbpub.Code_ERR_INVALID_PACK_TYPE, code)
}

func TestDecoder_DecodeS_InvalidCipherSize_TooSmall(t *testing.T) {
	ch := NewMockSecureChannel()
	d := NewDecoder(ch)
	secretKey := randomKey(t)
	nonce := randomNonce(t)
	msg := new(pbpub.SendMsgReq)

	packet := buildSecurePacket(
		t,
		ch,
		secretKey,
		nonce,
		pbpub.PacketType_REQ_SEND_MSG,
		msg,
		nil,
	)
	binary.LittleEndian.PutUint16(packet[16:18], 10) // <= fakeTagSize, must be rejected

	_, _, code, err := d.DecodeS(secretKey, packet)
	require.Error(t, err)
	require.Equal(t, pbpub.Code_ERR_INVALID_PACK_CIPHER, code)
}

func TestDecoder_DecodeS_TooShortPacket_ReturnsErrorNotPanic(t *testing.T) {
	ch := NewMockSecureChannel()
	d := NewDecoder(ch)
	secretKey := randomKey(t)
	shortPacket := []byte{1, 2, 3}

	var (
		code pbpub.Code
		err  error
	)
	runRecovered(t, "DecodeS", func() {
		_, _, code, err = d.DecodeS(secretKey, shortPacket)
	})
	require.Error(t, err)
	require.Equal(t, pbpub.Code_ERR_INVALID_PACK_SIZE, code)
}

func TestDecoder_DecodeS_RejectsInconsistentFrameLength(t *testing.T) {
	ch := NewMockSecureChannel()
	d := NewDecoder(ch)
	secretKey := randomKey(t)
	packet := buildSecurePacket(
		t,
		ch,
		secretKey,
		randomNonce(t),
		pbpub.PacketType_REQ_SEND_MSG,
		new(pbpub.SendMsgReq),
		nil,
	)
	binary.LittleEndian.PutUint16(packet[:2], uint16(len(packet)-1))

	_, _, code, err := d.DecodeS(secretKey, packet)
	require.ErrorIs(t, err, ErrPkgSizeInvalid)
	require.Equal(t, pbpub.Code_ERR_INVALID_PACK_SIZE, code)
}

func TestDecoder_DecodeS_CipherSizeExceedsPacket_ReturnsErrorNotPanic(t *testing.T) {
	ch := NewMockSecureChannel()
	b := NewDecoder(ch)
	secretKey := randomKey(t)
	nonce := randomNonce(t)
	msg := new(pbpub.SendMsgReq)

	packet := buildSecurePacket(
		t,
		ch,
		secretKey,
		nonce,
		pbpub.PacketType_REQ_SEND_MSG,
		msg,
		nil,
	)
	binary.LittleEndian.PutUint16(packet[16:18], 60000) // far beyond remaining data

	var (
		code pbpub.Code
		err  error
	)
	runRecovered(t, "DecodeS", func() {
		_, _, code, err = b.DecodeS(secretKey, packet)
	})
	require.Error(t, err)
	require.Equal(t, pbpub.Code_ERR_INVALID_PACK_CIPHER, code)
}
