/*
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

package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json/v2"
	"flag"
	"io"
	"os"
	"path/filepath"
	"testing"

	hubnoise "xxx/internal/hub/infrastructure/noise"
	"xxx/internal/shared/hubendpoint"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/curve25519"
)

func TestProvisioningCreatesMatchingFilesWithoutPrintingSecrets(t *testing.T) {
	dir := t.TempDir()
	privatePath := filepath.Join(dir, "keyring.json")
	publicPath := filepath.Join(dir, "descriptor.json")
	args := []string{
		"-address",
		"hub.example.com:9000",
		"-audience",
		"hub.chat",
		"-key-id",
		"7",
		"-private-out",
		privatePath,
		"-public-out",
		publicPath,
	}

	var output bytes.Buffer
	require.NoError(t, run(args, &output, io.Discard))

	ring, err := hubnoise.LoadKeyring(privatePath, 7)
	require.NoError(t, err)
	defer ring.Close()
	require.Equal(t, uint32(7), ring.CurrentKeyID())
	pair, ok := ring.Keypair(7)
	require.True(t, ok)
	defer clear(pair.Private)
	data, err := os.ReadFile(publicPath)
	require.NoError(t, err)

	// Use the exact parser used by the IAM file loader, not copied JSON options.
	descriptor, err := hubendpoint.Parse(data)
	require.NoError(t, err)
	require.Equal(t, "hub.example.com:9000", descriptor.Address)
	require.Equal(t, "hub.chat", descriptor.Audience)
	require.Equal(t, uint32(7), descriptor.KeyID)
	require.Equal(t, pair.Public, descriptor.PublicKey)
	require.NotContains(t, string(data), "private_key")
	require.NotContains(t, output.String(), base64.StdEncoding.EncodeToString(pair.Private))

	before, err := os.ReadFile(privatePath)
	require.NoError(t, err)
	defer clear(before)
	var keyFile hubnoise.KeyFile
	require.NoError(t, json.Unmarshal(before, &keyFile))
	defer func() {
		for _, entry := range keyFile.Keys {
			clear(entry.PrivateKey)
		}
	}()
	require.Len(t, keyFile.Keys, 1)
	entry := keyFile.Keys[0]
	require.Equal(t, uint32(7), entry.KeyID)

	// Independently compare the stored public key with a fresh X25519 result.
	derived, err := curve25519.X25519(entry.PrivateKey, curve25519.Basepoint)
	require.NoError(t, err)
	require.Equal(t, derived, entry.PublicKey)
	require.Equal(t, entry.PublicKey, descriptor.PublicKey)
	require.Contains(t, output.String(), privatePath)
	require.Contains(t, output.String(), publicPath)
	require.NotContains(t, output.String(), base64.StdEncoding.EncodeToString(entry.PrivateKey))
	require.Error(t, run(args, &output, io.Discard), "never overwrite an existing service key")

	after, err := os.ReadFile(privatePath)
	require.NoError(t, err)
	defer clear(after)
	require.Equal(t, before, after)
	afterPublic, err := os.ReadFile(publicPath)
	require.NoError(t, err)
	require.Equal(t, data, afterPublic)
}

func TestProvisioningDoesNotCreatePublicFileWhenPrivateDestinationExists(t *testing.T) {
	dir := t.TempDir()
	privatePath := filepath.Join(dir, "keyring.json")
	publicPath := filepath.Join(dir, "descriptor.json")
	require.NoError(t, os.WriteFile(privatePath, []byte("existing"), 0o600))
	err := run(
		[]string{
			"-address",
			"hub.example.com:9000",
			"-audience",
			"hub.chat",
			"-private-out",
			privatePath,
			"-public-out",
			publicPath,
		},
		io.Discard,
		io.Discard,
	)
	require.ErrorIs(t, err, os.ErrExist)
	require.ErrorContains(t, err, "create private keyring:")
	_, err = os.Stat(publicPath)
	require.ErrorIs(t, err, os.ErrNotExist)
	data, err := os.ReadFile(privatePath)
	require.NoError(t, err)
	require.Equal(t, "existing", string(data))
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1, "no staging files remain")
}

func TestProvisioningDoesNotLeavePrivateFileWhenPublicDestinationExists(t *testing.T) {
	dir := t.TempDir()
	privatePath := filepath.Join(dir, "keyring.json")
	publicPath := filepath.Join(dir, "descriptor.json")
	require.NoError(t, os.WriteFile(publicPath, []byte("existing"), 0o644))
	err := run(
		[]string{
			"-address",
			"hub.example.com:9000",
			"-audience",
			"hub.chat",
			"-private-out",
			privatePath,
			"-public-out",
			publicPath,
		},
		&bytes.Buffer{},
		io.Discard,
	)
	require.ErrorIs(t, err, os.ErrExist)
	require.ErrorContains(t, err, "create public descriptor:")
	_, err = os.Stat(privatePath)
	require.True(t, os.IsNotExist(err))
	data, err := os.ReadFile(publicPath)
	require.NoError(t, err)
	require.Equal(t, "existing", string(data))
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1, "private output and staging files are removed")
}

func TestProvisioningRejectsBadArguments(t *testing.T) {
	for _, tc := range []struct {
		name     string
		extra    []string
		samePath bool
		want     string
	}{
		{"zero key ID", []string{"-key-id", "0"}, false, "key ID in 1..4294967295"},
		{"overflow key ID", []string{"-key-id", "4294967296"}, false, "key ID in 1..4294967295"},
		{"negative key ID", []string{"-key-id", "-1"}, false, "invalid value"},
		{"empty address", []string{"-address", ""}, false, "validate descriptor:"},
		{"same paths", nil, true, "private and public output paths must be different"},
		{"extra positional argument", []string{"unexpected"}, false, "provide -address"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			privatePath := filepath.Join(dir, "keyring.json")
			publicPath := filepath.Join(dir, "descriptor.json")
			if tc.samePath {
				publicPath = privatePath
			}
			args := []string{
				"-address",
				"hub.example.com:9000",
				"-audience",
				"hub.chat",
				"-private-out",
				privatePath,
				"-public-out",
				publicPath,
			}
			var output bytes.Buffer
			err := run(append(args, tc.extra...), &output, io.Discard)
			require.ErrorContains(t, err, tc.want)
			require.Empty(t, output.String())
			entries, err := os.ReadDir(dir)
			require.NoError(t, err)
			require.Empty(t, entries, "invalid arguments create no outputs or staging files")
		})
	}
	require.Error(t, run(nil, &bytes.Buffer{}, io.Discard))
}

func TestProvisioningDiagnosticsUseErrorOutput(t *testing.T) {
	for _, arg := range []string{"-help", "-unknown-flag"} {
		t.Run(arg, func(t *testing.T) {
			var output, diagnostics bytes.Buffer
			err := run([]string{arg}, &output, &diagnostics)
			require.Error(t, err)
			if arg == "-help" {
				require.ErrorIs(t, err, flag.ErrHelp)
			}
			require.Empty(t, output.String())
			require.NotEmpty(t, diagnostics.String())
		})
	}
}

func TestProvisioningValidationHasContextAndCreatesNoOutputs(t *testing.T) {
	dir := t.TempDir()
	privatePath := filepath.Join(dir, "keyring.json")
	publicPath := filepath.Join(dir, "descriptor.json")
	err := run(
		[]string{
			"-address",
			"invalid",
			"-audience",
			"hub.chat",
			"-private-out",
			privatePath,
			"-public-out",
			publicPath,
		},
		io.Discard,
		io.Discard,
	)
	require.ErrorContains(t, err, "validate descriptor:")
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestCreateExclusiveConcurrentWritersPublishOneCompleteFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "keyring.json")
	type result struct {
		data []byte
		err  error
	}
	const writers = 8
	results := make(chan result, writers)
	start := make(chan struct{})
	for i := range writers {
		data := bytes.Repeat([]byte{byte('a' + i)}, 4096)
		go func() {
			<-start
			results <- result{
				data,
				createExclusive(path, data, 0o600),
			}
		}()
	}
	close(start)
	var winner []byte
	successes := 0
	for range writers {
		r := <-results
		if r.err == nil {
			successes++
			winner = r.data
		} else {
			require.ErrorIs(t, r.err, os.ErrExist)
		}
	}
	require.Equal(t, 1, successes)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, append(winner, '\n'), data)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1, "clean up staging files after success and EEXIST")
}

func TestCreateExclusiveDoesNotFollowExistingSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "existing.json")
	require.NoError(t, os.WriteFile(target, []byte("existing"), 0o600))
	path := filepath.Join(dir, "keyring.json")
	require.NoError(t, os.Symlink(target, path))
	require.ErrorIs(t, createExclusive(path, []byte("replacement"), 0o600), os.ErrExist)
	data, err := os.ReadFile(target)
	require.NoError(t, err)
	require.Equal(t, "existing", string(data))
}
