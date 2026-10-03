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

package pkg

import (
	"time"

	"github.com/spf13/viper"
)

type ConfigLoader interface {
	GetString(key string) string
	GetStringSlice(key string) []string

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

func DefaultConfigLoader(folderPath, fileName string) (ConfigLoader, error) {
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
