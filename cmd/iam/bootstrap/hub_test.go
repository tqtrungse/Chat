/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package bootstrap

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
	"time"

	"xxx/internal/shared/hubendpoint"

	"github.com/flynn/noise"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

// Message prefixes produced by loadHubEndpoint. Each one identifies which
// branch rejected the input, so a case can't pass because of a different error.
const (
	msgInvalidJSON = "invalid hub descriptor JSON"
	msgUnexpected  = "hub descriptor has unexpected field"
	msgDuplicate   = "hub descriptor has duplicate field"
	msgValidate    = "validate hub descriptor"
	msgExceeds     = "hub descriptor exceeds"

	// leakMarker is placed inside values that must never appear in an error.
	leakMarker = "do-not-log"

	notRegularMsg = "hub descriptor must be a regular file"
)

// -------
// helpers
// -------

func newDescriptor(t *testing.T) hubendpoint.Descriptor {
	t.Helper()

	pair, err := noise.DH25519.GenerateKeypair(nil)
	require.NoError(t, err)

	clear(pair.Private[:cap(pair.Private)])
	return hubendpoint.Descriptor{
		Address:   "hub.example.com:9000",
		Audience:  "hub.chat",
		KeyID:     7,
		PublicKey: pair.Public,
		Protocol:  hubendpoint.Protocol,
		Version:   hubendpoint.WireVersion,
	}
}

func mustMarshal(t *testing.T, v any, opts ...json.Options) []byte {
	t.Helper()

	data, err := json.Marshal(v, opts...)
	require.NoError(t, err)

	return data
}

func loaderForPath(path string) *viper.Viper {
	loader := viper.New()
	loader.Set("HUB_NOISE_DESCRIPTOR_FILE", path)
	return loader
}

func writeDescriptorFile(t *testing.T, data []byte) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "descriptor.json")
	require.NoError(t, os.WriteFile(path, data, 0o600))

	return path
}

func loaderFor(t *testing.T, data []byte) *viper.Viper {
	t.Helper()

	return loaderForPath(writeDescriptorFile(t, data))
}

// withMember inserts `,"<rawName>":<rawValue>` before the closing brace.
// rawName is inserted verbatim so callers can pass escaped names.
func withMember(t *testing.T, data []byte, rawName, rawValue string) []byte {
	t.Helper()

	i := bytes.LastIndexByte(data, '}')
	require.GreaterOrEqual(t, i, 0)

	member := fmt.Sprintf(`,"%s":%s`, rawName, rawValue)
	return slices.Concat(data[:i], []byte(member), data[i:])
}

// padTo appends spaces so the result is exactly n bytes.
func padTo(t *testing.T, data []byte, n int) []byte {
	t.Helper()

	require.LessOrEqual(t, len(data), n)
	return append(slices.Clone(data), bytes.Repeat([]byte(" "), n-len(data))...)
}

// ----------
// happy path
// ----------

func TestLoadHubEndpointAcceptsValidDescriptor(t *testing.T) {
	want := newDescriptor(t)
	compact := mustMarshal(t, want)

	maxKeyID := want
	maxKeyID.KeyID = math.MaxUint32

	for _, tc := range []struct {
		name string
		data []byte
		want hubendpoint.Descriptor
	}{
		{"compact", compact, want},
		{"trailing newline", append(slices.Clone(compact), '\n'), want},
		{"trailing CRLF", append(slices.Clone(compact), '\r', '\n'), want},
		{"leading whitespace", append([]byte("\n\t "), compact...), want},
		{"multiline", mustMarshal(t, want, jsontext.Multiline(true)), want},
		{"max uint32 key_id", mustMarshal(t, maxKeyID), maxKeyID},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				got, err := loadHubEndpoint(loaderFor(t, tc.data))
				require.NoError(t, err)
				require.Equal(t, &tc.want, got)
			},
		)
	}
}

func TestLoadHubEndpointTrimsPathWhitespace(t *testing.T) {
	want := newDescriptor(t)
	path := writeDescriptorFile(t, mustMarshal(t, want))
	got, err := loadHubEndpoint(loaderForPath("  " + path + "\n"))
	require.NoError(t, err)
	require.Equal(t, &want, got)
}

// -------------
// config errors
// -------------

func TestLoadHubEndpointConfigErrors(t *testing.T) {
	t.Run(
		"nil loader",
		func(t *testing.T) {
			got, err := loadHubEndpoint(nil)
			require.Nil(t, got)
			require.ErrorContains(t, err, "config loader is nil")
		},
	)

	t.Run(
		"unset",
		func(t *testing.T) {
			got, err := loadHubEndpoint(viper.New())
			require.Nil(t, got)
			require.ErrorContains(t, err, "HUB_NOISE_DESCRIPTOR_FILE is required")
		},
	)

	// Guards the `len(path) == 0` condition: a wrong condition would let these
	// fall through to os.Open("") and still return *an* error.
	for _, v := range []string{"", " ", "\t\n "} {
		t.Run(
			fmt.Sprintf("blank %q", v),
			func(t *testing.T) {
				got, err := loadHubEndpoint(loaderForPath(v))
				require.Nil(t, got)
				require.ErrorContains(t, err, "HUB_NOISE_DESCRIPTOR_FILE is required")
			},
		)
	}

	t.Run(
		"missing file",
		func(t *testing.T) {
			got, err := loadHubEndpoint(loaderForPath(filepath.Join(t.TempDir(), "nope.json")))
			require.Nil(t, got)
			require.ErrorContains(t, err, "open hub descriptor")
			require.ErrorIs(t, err, fs.ErrNotExist)
		},
	)
}

// --------
// size cap
// --------

func TestLoadHubEndpointSizeLimit(t *testing.T) {
	valid := mustMarshal(t, newDescriptor(t))

	t.Run(
		"exactly at limit",
		func(t *testing.T) {
			got, err := loadHubEndpoint(loaderFor(t, padTo(t, valid, hubendpoint.MaxDescriptorBytes)))
			require.NoError(t, err)
			require.NotNil(t, got)
		},
	)

	t.Run(
		"one byte over",
		func(t *testing.T) {
			got, err := loadHubEndpoint(loaderFor(t, padTo(t, valid, hubendpoint.MaxDescriptorBytes+1)))
			require.Nil(t, got)
			require.ErrorContains(t, err, msgExceeds)
		},
	)

	t.Run(
		"far over",
		func(t *testing.T) {
			got, err := loadHubEndpoint(loaderFor(t, padTo(t, valid, 1<<20)))
			require.Nil(t, got)
			require.ErrorContains(t, err, msgExceeds)
		},
	)
}

// -----------------
// malformed / typed
// -----------------

func TestLoadHubEndpointRejectsMalformedJSON(t *testing.T) {
	valid := string(mustMarshal(t, newDescriptor(t)))

	for _, tc := range []struct {
		name  string
		input string
		want  string // "" = only require some error
	}{
		{"empty file", ``, msgInvalidJSON},
		{"only whitespace", " \n\t ", msgInvalidJSON},
		{"truncated", `{"address":`, msgInvalidJSON},
		{"syntax error", `{"address":!}`, msgInvalidJSON},
		{"trailing value", valid + ` {}`, msgInvalidJSON},
		{"trailing garbage", valid + ` !`, msgInvalidJSON},
		{"top-level array", `[]`, msgInvalidJSON},
		{"top-level string", `"x"`, msgInvalidJSON},
		{"top-level number", `1`, msgInvalidJSON},
		{"UTF-8 BOM", "\xEF\xBB\xBF" + valid, msgInvalidJSON},
		{"invalid UTF-8 in string", "{\"address\":\"\xff\"}", msgInvalidJSON},
		{"key_id as string", `{"key_id":"7"}`, msgInvalidJSON},
		{"key_id overflows uint32", `{"key_id":4294967296}`, msgInvalidJSON},
		{"key_id overflows uint64", `{"key_id":18446744073709551616}`, msgInvalidJSON},
		{"key_id negative", `{"key_id":-1}`, msgInvalidJSON},
		{"key_id fractional", `{"key_id":1.5}`, msgInvalidJSON},
		{"public_key invalid base64", `{"public_key":"%%%"}`, msgInvalidJSON},
		// An empty object decodes fine; only Validate can reject it.
		{"empty object", `{}`, msgValidate},
		// v2 decodes null into a struct as its zero value. Either branch is
		// acceptable as long as the result is an error and no descriptor.
		{"null", `null`, ""},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				got, err := loadHubEndpoint(loaderFor(t, []byte(tc.input)))
				require.Nil(t, got)
				require.Error(t, err)
				if tc.want != "" {
					require.ErrorContains(t, err, tc.want)
				}
			},
		)
	}
}

// ---------------------
// private key / unknown
// ---------------------

func TestLoadHubEndpointRejectsUnknownFields(t *testing.T) {
	valid := mustMarshal(t, newDescriptor(t))

	for _, tc := range []struct {
		name  string
		input []byte
	}{
		{
			"private_key beside valid fields",
			withMember(t, valid, "private_key", `"`+leakMarker+`"`),
		},
		{
			"private_key alone",
			[]byte(`{"private_key":"` + leakMarker + `"}`),
		},
		{
			"keyring with private keys",
			[]byte(`{"keys":[{"key_id":7,"private_key":"` + leakMarker + `"}]}`),
		},
		{
			"unrelated extra field",
			withMember(t, valid, "extra", `1`),
		},
		// v2 matches names case-sensitively; "Address" is unknown, not an alias.
		{
			"wrong-case field name",
			withMember(t, valid, "Address", `"other.example.com:9000"`),
		},
		{
			"upper-case field name",
			withMember(t, valid, "ADDRESS", `"other.example.com:9000"`),
		},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				got, err := loadHubEndpoint(loaderFor(t, tc.input))
				require.Nil(t, got)
				require.ErrorContains(t, err, msgUnexpected)
				require.ErrorIs(t, err, hubendpoint.ErrUnknownField)
				require.NotContains(t, err.Error(), leakMarker)
			},
		)
	}
}

// ----------
// duplicates
// ----------

func TestLoadHubEndpointRejectsDuplicateFields(t *testing.T) {
	data := mustMarshal(t, newDescriptor(t))

	var fields map[string]jsontext.Value
	require.NoError(t, json.Unmarshal(data, &fields))
	require.NotEmpty(t, fields)

	// Every field, written a second time with the *same* value, both literally
	// and with its first letter \u-escaped. An implementation that only fails
	// on conflicting values, or only compares raw bytes of the name, is caught.
	for name, raw := range fields {
		escaped := fmt.Sprintf(`\u%04x`, name[0]) + name[1:]
		for _, tc := range []struct{ label, rawName string }{
			{"literal", name},
			{"escaped", escaped},
		} {
			t.Run(
				name+"/"+tc.label,
				func(t *testing.T) {
					input := withMember(t, data, tc.rawName, string(raw))
					got, err := loadHubEndpoint(loaderFor(t, input))
					require.Nil(t, got)
					require.ErrorContains(t, err, msgDuplicate)
					require.ErrorIs(t, err, hubendpoint.ErrDuplicateField)
				},
			)
		}
	}

	// The attack that matters: a second, different, but fully valid key that
	// would silently win under "last value wins" semantics.
	t.Run(
		"public_key swap with a different valid key",
		func(t *testing.T) {
			other := newDescriptor(t)
			otherKey := string(mustMarshal(t, other.PublicKey))
			got, err := loadHubEndpoint(loaderFor(t, withMember(t, data, "public_key", otherKey)))
			require.Nil(t, got)
			require.ErrorIs(t, err, hubendpoint.ErrDuplicateField)
		},
	)

	t.Run(
		"key_id swap",
		func(t *testing.T) {
			got, err := loadHubEndpoint(loaderFor(t, withMember(t, data, "key_id", `8`)))
			require.Nil(t, got)
			require.ErrorIs(t, err, hubendpoint.ErrDuplicateField)
		},
	)
}

// --------
// Validate
// --------

func TestLoadHubEndpointCallsValidate(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(d *hubendpoint.Descriptor)
	}{
		{
			"unsupported version",
			func(d *hubendpoint.Descriptor) { d.Version++ },
		},
		{
			"public key 31 bytes",
			func(d *hubendpoint.Descriptor) { d.PublicKey = d.PublicKey[:hubendpoint.X25519PublicKeySize-1] },
		},
		{
			"public key 33 bytes",
			func(d *hubendpoint.Descriptor) { d.PublicKey = append(slices.Clone(d.PublicKey), 0) },
		},
		{
			"public key empty",
			func(d *hubendpoint.Descriptor) { d.PublicKey = nil },
		},
		{
			"empty address",
			func(d *hubendpoint.Descriptor) { d.Address = "" },
		},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				d := newDescriptor(t)
				tc.mutate(&d)
				got, err := loadHubEndpoint(loaderFor(t, mustMarshal(t, d)))
				require.Nil(t, got)
				require.ErrorContains(t, err, msgValidate)
			},
		)
	}
}

// ------------------
// no value in errors
// ------------------

func TestLoadHubEndpointErrorsDoNotEchoValues(t *testing.T) {
	valid := mustMarshal(t, newDescriptor(t))

	for _, tc := range []struct {
		name   string
		input  []byte
		secret string
	}{
		{
			"unknown field value",
			[]byte(`{"private_key":"` + leakMarker + `"}`),
			leakMarker,
		},
		{
			"wrong type",
			[]byte(`{"key_id":"` + leakMarker + `"}`),
			leakMarker,
		},
		{
			"bad base64",
			[]byte(`{"public_key":"%%%` + leakMarker + `"}`),
			leakMarker,
		},
		{
			"invalid UTF-8",
			[]byte("{\"address\":\"\xff" + leakMarker + "\"}"),
			leakMarker,
		},
		{
			"duplicate value",
			withMember(t, valid, "address", `"`+leakMarker+`"`),
			leakMarker,
		},
		{
			"trailing value",
			slices.Concat(valid, []byte(` "`+leakMarker+`"`)),
			leakMarker,
		},
		{
			"uint32 overflow",
			[]byte(`{"key_id":4294967296}`),
			"4294967296",
		},
		{
			"uint64 overflow",
			[]byte(`{"key_id":18446744073709551616}`),
			"18446744073709551616",
		},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				got, err := loadHubEndpoint(loaderFor(t, tc.input))
				require.Nil(t, got)
				require.Error(t, err)
				require.NotContains(t, err.Error(), tc.secret)
			},
		)
	}
}

// Field *names* are echoed by v2 ("unknown object member name ..."), and
// hub.go wraps with %w. If you consider names non-sensitive, delete this test;
// if not, return a fixed message in the ErrUnknownName / ErrDuplicateName
// branches instead of wrapping.
func TestLoadHubEndpointErrorsDoNotEchoFieldNames(t *testing.T) {
	valid := mustMarshal(t, newDescriptor(t))

	for _, tc := range []struct {
		name  string
		input []byte
	}{
		{
			"unknown field name",
			[]byte(`{"` + leakMarker + `":1}`),
		},
		{
			"unknown field name beside valid fields",
			withMember(t, valid, leakMarker, `1`),
		},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				got, err := loadHubEndpoint(loaderFor(t, tc.input))
				require.Nil(t, got)
				require.Error(t, err)
				require.NotContains(t, err.Error(), leakMarker)
			},
		)
	}
}

// --------
// symlinks
// --------

// Kubernetes ConfigMap/Secret volumes expose files as symlinks
// (file -> ..data/file). Adding O_NOFOLLOW to the open must break this test.
func TestLoadHubEndpointFollowsSymlinkToRegularFile(t *testing.T) {
	want := newDescriptor(t)
	target := writeDescriptorFile(t, mustMarshal(t, want))

	link := filepath.Join(t.TempDir(), "projected-descriptor.json")
	err := os.Symlink(target, link)
	if err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	got, err := loadHubEndpoint(loaderForPath(link))
	require.NoError(t, err)
	require.Equal(t, &want, got)
}

func TestLoadHubEndpointDanglingSymlink(t *testing.T) {
	link := filepath.Join(t.TempDir(), "dangling.json")
	err := os.Symlink(filepath.Join(t.TempDir(), "gone.json"), link)
	if err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	got, err := loadHubEndpoint(loaderForPath(link))
	require.Nil(t, got)
	require.ErrorContains(t, err, "open hub descriptor")
	require.ErrorIs(t, err, fs.ErrNotExist)
}

// ------------
// Open on FIFO
// ------------

func TestLoadHubEndpointRequiresRegularFile(t *testing.T) {
	dir := t.TempDir()

	dirLink := filepath.Join(t.TempDir(), "link-to-dir")
	require.NoError(t, os.Symlink(dir, dirLink))

	for _, tc := range []struct{ name, path string }{
		{"directory", dir},
		{"symlink to directory", dirLink},
		{"char device", os.DevNull},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				got, err := loadHubEndpoint(loaderForPath(tc.path))
				require.Nil(t, got)
				require.ErrorContains(t, err, notRegularMsg)
			},
		)
	}
}

// The open() itself must not block on a FIFO that has no writer; the type check
// happens afterward on the fd. A regression (plain os.Open) hangs here, so the
// call runs in a goroutine with a deadline.
func TestLoadHubEndpointFIFODoesNotBlock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "descriptor.fifo")
	require.NoError(t, syscall.Mkfifo(path, 0o600))

	// If loadHubEndpoint is stuck in a blocking open(O_RDONLY), opening the
	// FIFO O_RDWR supplies a "writer" and releases the goroutine.
	t.Cleanup(func() {
		f, err := os.OpenFile(path, os.O_RDWR|syscall.O_NONBLOCK, 0)
		if err == nil {
			_ = f.Close()
		}
	})

	type result struct {
		d   *hubendpoint.Descriptor
		err error
	}

	done := make(chan result, 1)
	go func() {
		d, err := loadHubEndpoint(loaderForPath(path))
		done <- result{d, err}
	}()

	select {
	case r := <-done:
		require.Nil(t, r.d)
		require.ErrorContains(t, r.err, notRegularMsg)
	case <-time.After(2 * time.Second):
		require.Fail(t, "loadHubEndpoint blocked on a FIFO")
	}
}

func TestLoadHubEndpointUnreadableFile(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses file permissions")
	}
	path := writeDescriptorFile(t, mustMarshal(t, newDescriptor(t)))
	require.NoError(t, os.Chmod(path, 0o000))

	got, err := loadHubEndpoint(loaderForPath(path))
	require.Nil(t, got)
	require.ErrorContains(t, err, "open hub descriptor")
	require.ErrorIs(t, err, fs.ErrPermission)
}
