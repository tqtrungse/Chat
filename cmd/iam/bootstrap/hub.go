/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package bootstrap

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"

	"xxx/internal/shared/hubendpoint"

	"xxx/pkg/config"
)

// loadHubEndpoint reads public data only. It intentionally rejects a private
// keyring or extra private_key field. It is wired into ticket issuance in step 4.
func loadHubEndpoint(loader config.Loader) (*hubendpoint.Descriptor, error) {
	if loader == nil {
		return nil, fmt.Errorf("config loader is nil")
	}

	path := strings.TrimSpace(loader.GetString("HUB_NOISE_DESCRIPTOR_FILE"))
	if len(path) == 0 {
		return nil, fmt.Errorf("HUB_NOISE_DESCRIPTOR_FILE is required")
	}

	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, fmt.Errorf("open hub descriptor: %w", err)
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat hub descriptor: %w", err)
	}

	if !info.Mode().IsRegular() {
		return nil, errors.New("hub descriptor must be a regular file")
	}

	data, err := io.ReadAll(io.LimitReader(f, hubendpoint.MaxDescriptorBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read hub descriptor: %w", err)
	}

	return hubendpoint.Parse(data)
}
