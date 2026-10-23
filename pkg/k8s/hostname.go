/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
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
