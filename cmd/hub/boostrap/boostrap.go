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

package boostrap

import (
	"xxx/internal/hub/infrastructure/crypto"

	"xxx/pkg"
	"xxx/pkg/log"

	"go.uber.org/zap"
)

func Run() {
	logger, closeLogger := log.NewLogger(nil)
	defer closeLogger()

	cfgLoader, err := pkg.DefaultConfigLoader(".", "./prod.env")
	if err != nil {
		logger.Error("failed to init config loader", zap.Error(err))
		return
	}

	hubID := cfgLoader.GetUint64("HUB_ID")
	if hubID == 0 {
		serviceID := cfgLoader.GetUint64("SERVICE_ID")
		if serviceID == 0 {
			logger.Error("zero service ID")
			return
		}

		hubID, err = pkg.MachineIDFromHostname(serviceID)
		if err != nil {
			logger.Error("failed to get machine ID from hostname", zap.Error(err))
			return
		}
	}

	keys, err := crypto.ParseTicketKeys(cfgLoader.GetStringSlice("TICKET_KEYS"))
	if err != nil {
		logger.Error("failed to load ticket keys", zap.Error(err))
		return
	}

	_, err = crypto.NewTicketSealer(cfgLoader.GetUint8("TICKET_CURRENT_KEY_ID"), keys)
	if err != nil {
		logger.Error("failed to load ticket current key ID", zap.Error(err))
		return
	}

	//router := connection.NewRouter(ctx, cfg.Connection, signer, dCache, cb, logger, pool, encoder, tickets)
	//keyExchanger := exchange_key.New(db, crypto.NewHkdfDeriver(), deviceMetaReader, tickets)
}
