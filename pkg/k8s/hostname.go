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

package k8s

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

const ordinalBits = uint64(8) // max 256 replicas per service

// MachineIDFromHostname get machine ID from K8S StatefullSet host name.
func MachineIDFromHostname(serviceID uint64) (uint64, error) {
	h, err := os.Hostname()
	if err != nil {
		return 0, err
	}

	i := strings.LastIndex(h, "-")
	if i < 0 {
		return 0, fmt.Errorf("no ordinal in hostname %q", h)
	}

	n, err := strconv.Atoi(h[i+1:])
	if err != nil || n < 0 || n >= 1<<ordinalBits {
		return 0, fmt.Errorf("invalid ordinal in hostname %q", h)
	}
	return serviceID<<ordinalBits | uint64(n), nil
}
