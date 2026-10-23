/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package noise_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	hubnoise "xxx/internal/hub/infrastructure/noise"

	"github.com/flynn/noise"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func noiseTestConfig(t *testing.T) *viper.Viper {
	t.Helper()

	pair, err := noise.DH25519.GenerateKeypair(nil)
	require.NoError(t, err)
	defer clear(pair.Private[:cap(pair.Private)])

	data, err := json.Marshal(
		hubnoise.KeyFile{
			Keys: []hubnoise.KeyEntry{
				{
					KeyID:      7,
					PrivateKey: pair.Private,
					PublicKey:  pair.Public,
				},
			},
		},
	)
	require.NoError(t, err)
	defer clear(data[:cap(data)])

	path := filepath.Join(t.TempDir(), "keyring.json")
	require.NoError(t, os.WriteFile(path, data, 0o600))

	loader := viper.New()
	loader.Set("HUB_NOISE_KEYRING_FILE", path)
	loader.Set("HUB_NOISE_CURRENT_KEY_ID", "7")
	loader.Set("HUB_NOISE_AUDIENCE", "hub.chat")
	loader.Set("HUB_NOISE_HANDSHAKE_TIMEOUT", "5s")

	return loader
}

func TestLoadNoiseKeyringAndConfig(t *testing.T) {
	loader := noiseTestConfig(t)

	var cfg hubnoise.Config
	err := cfg.Load(loader)
	require.NoError(t, err)
	require.Equal(t, loader.GetString("HUB_NOISE_KEYRING_FILE"), cfg.KeyringFile)
	require.Equal(t, uint32(7), cfg.CurrentKeyID)
	require.Equal(t, "hub.chat", cfg.Audience)
	require.Equal(t, 5*time.Second, cfg.HandshakeTimeout)

	ring, err := hubnoise.LoadKeyring(cfg.KeyringFile, cfg.CurrentKeyID)
	require.NoError(t, err)
	defer ring.Close()
	require.Equal(t, uint32(7), ring.CurrentKeyID())

	// TCP settings and the legacy timeout key must not affect Noise config.
	want := cfg
	loader.Set("TCP_SERVER_MAX_FRAME_BYTES", "invalid")
	loader.Set("NOISE_HANDSHAKE_TIMEOUT", "invalid")
	err = cfg.Load(loader)
	require.NoError(t, err)
	require.Equal(t, want, cfg)
}

func TestLoadNoiseRejectsInvalidConfig(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"HUB_NOISE_KEYRING_FILE", ""},
		{"HUB_NOISE_CURRENT_KEY_ID", "0"},
		{"HUB_NOISE_CURRENT_KEY_ID", "4294967296"},
		{"HUB_NOISE_CURRENT_KEY_ID", "-1"},
		{"HUB_NOISE_CURRENT_KEY_ID", "7.1"},
		{"HUB_NOISE_AUDIENCE", ""},
		{"HUB_NOISE_AUDIENCE", "hub chat"},
	} {
		t.Run(tc.key+"/"+tc.value, func(t *testing.T) {
			loader := noiseTestConfig(t)
			loader.Set(tc.key, tc.value)
			var cfg hubnoise.Config
			err := cfg.Load(loader)
			require.Error(t, err)
		})
	}
	var cfg hubnoise.Config
	err := cfg.Load(nil)
	require.Error(t, err)
}

func TestLoadNoiseHandshakeTimeout(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  time.Duration
	}{
		{"2s", 2 * time.Second},
		{" 250ms ", 250 * time.Millisecond},
		{"1m30s", 90 * time.Second},
		{"0s", 5 * time.Second},
		{"-1s", 5 * time.Second},
	} {
		t.Run(tc.value, func(t *testing.T) {
			loader := noiseTestConfig(t)
			loader.Set("HUB_NOISE_HANDSHAKE_TIMEOUT", tc.value)
			var cfg hubnoise.Config
			err := cfg.Load(loader)
			require.NoError(t, err)
			require.Equal(t, tc.want, cfg.HandshakeTimeout)
		})
	}
}

func TestLoadNoiseRejectsMissingOrMalformedHandshakeTimeout(t *testing.T) {
	for _, value := range []string{
		"",
		" ",
		"oops",
		"5",
		"999999999999999999999h",
	} {
		t.Run(value, func(t *testing.T) {
			loader := noiseTestConfig(t)
			loader.Set("HUB_NOISE_HANDSHAKE_TIMEOUT", value)
			var cfg hubnoise.Config
			err := cfg.Load(loader)
			require.ErrorContains(t, err, "HUB_NOISE_HANDSHAKE_TIMEOUT")
		})
	}
}

func TestLoadNoiseDoesNotFallBackToLegacyTimeout(t *testing.T) {
	loader := noiseTestConfig(t)
	loader.Set("HUB_NOISE_HANDSHAKE_TIMEOUT", "")
	loader.Set("NOISE_HANDSHAKE_TIMEOUT", "5s")
	var cfg hubnoise.Config
	err := cfg.Load(loader)
	require.ErrorContains(t, err, "HUB_NOISE_HANDSHAKE_TIMEOUT")
}
