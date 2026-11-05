/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Loader interface {
	// IsSet distinguishes an absent key from an explicitly configured zero value.
	IsSet(key string) bool

	GetString(key string) string

	GetInt(key string) int
	GetUint8(key string) uint8
	GetUint16(key string) uint16
	GetUint32(key string) uint32
	GetUint64(key string) uint64

	GetFloat64(key string) float64

	GetBool(key string) bool

	GetTime(key string) time.Time
	GetDuration(key string) time.Duration
}

func Load(serviceName string) (Loader, error) {
	path, err := findPath(serviceName)
	if err != nil {
		return nil, err
	}

	return defaultLoader(path)
}

func SplitList(s string) []string {
	var out []string
	for p := range strings.SplitSeq(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func defaultLoader(path string) (Loader, error) {
	v := viper.New()
	v.AutomaticEnv()
	if len(path) == 0 {
		return v, nil
	}

	// SetConfigFile reads the exact file at path and gets the format from its extension.
	// A search by base name selects the first match in the extension list of viper.
	// Thus app.json replaces app.yaml when the two files are in the same folder.
	v.SetConfigFile(path)
	// Read in config file
	err := v.ReadInConfig()
	if err != nil {
		return nil, err
	}

	// Watch config dynamically
	v.WatchConfig()
	return v, nil
}

func findPath(serviceName string) (string, error) {
	var (
		envConfigFile = fmt.Sprintf("%s_CONFIG_FILE", strings.ToUpper(serviceName))
		path          = os.Getenv(envConfigFile)
	)

	if len(path) > 0 {
		absPath, err := filepath.Abs(path)
		if err != nil {
			return "", err
		}
		if _, err = os.Stat(absPath); err != nil {
			return "", fmt.Errorf("%s %q: %w", envConfigFile, absPath, err)
		}
		return absPath, nil
	}

	serviceName = strings.ToLower(serviceName)
	candidates := []string{
		filepath.Join("cmd", serviceName, ".prod.env"),
		".prod.env",
	}
	if executable, err := os.Executable(); err == nil {
		candidates = append(candidates, filepath.Join(filepath.Dir(executable), ".prod.env"))
	}

	for _, candidate := range candidates {
		absPath, err := filepath.Abs(candidate)
		if err != nil {
			return "", err
		}

		if _, err = os.Stat(absPath); err == nil {
			return absPath, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("inspect %s config %q: %w", serviceName, absPath, err)
		}
	}
	return "", fmt.Errorf(
		"%s config not found; set %s or provide cmd/%s/.prod.env",
		serviceName,
		envConfigFile,
		serviceName,
	)
}
