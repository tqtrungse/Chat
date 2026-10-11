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

package bootstrap

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	iamhttp "xxx/internal/iam/adapter/http"
	sharedsession "xxx/internal/shared/session"

	"xxx/pkg"
)

func validateHTTPConfig(cfg iamhttp.Config) error {
	if cfg.Addr == "" {
		return errors.New("HTTP_SERVER_ADDR is required")
	}
	if cfg.CertFile == "" || cfg.KeyFile == "" {
		return errors.New("HTTP_SERVER_CERT_FILE and HTTP_SERVER_KEY_FILE are required; IAM serves HTTPS")
	}
	return nil
}

func loadTicketSigner(loader pkg.ConfigLoader) (*sharedsession.TicketSigner, error) {
	keys, err := sharedsession.ParseTicketEncryptKeys(
		strings.Split(loader.GetString("TICKET_ENCRYPT_KEYS"), ","),
	)
	if err != nil {
		return nil, fmt.Errorf("parse ticket encryption keys: %w", err)
	}
	defer func() {
		for _, key := range keys {
			clear(key)
		}
	}()

	sealer, err := sharedsession.NewTicketSealer(loader.GetUint8("TICKET_ENCRYPT_CURRENT_KEY_ID"), keys)
	if err != nil {
		return nil, fmt.Errorf("initialize ticket encryption: %w", err)
	}

	id, err := strconv.ParseUint(strings.TrimSpace(loader.GetString("TICKET_SIGNING_KEY_ID")), 10, 8)
	if err != nil {
		return nil, fmt.Errorf("parse TICKET_SIGNING_KEY_ID: %w", err)
	}

	seed, err := sharedsession.ParseTicketSigningPrivate(loader.GetString("TICKET_SIGNING_PRIVATE_KEY"))
	if err != nil {
		return nil, err
	}

	signer, err := sharedsession.NewTicketSigner(sealer, byte(id), seed)
	clear(seed)
	if err != nil {
		return nil, fmt.Errorf("initialize ticket signer: %w", err)
	}

	return signer, nil
}
