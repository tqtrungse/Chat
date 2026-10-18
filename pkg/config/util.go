/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

	fileName := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	return defaultLoader(filepath.Dir(path), fileName)
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

func defaultLoader(folderPath, fileName string) (Loader, error) {
	v := viper.New()
	v.AutomaticEnv()
	if len(folderPath) == 0 || len(fileName) == 0 {
		return v, nil
	}

	v.AddConfigPath(folderPath)
	v.SetConfigName(fileName)
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
