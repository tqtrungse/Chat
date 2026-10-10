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

package http

import (
	"errors"
	"fmt"
	"net/http"

	"xxx/api/iam/v1/json"
	"xxx/internal/iam/application/exchange_key"

	"xxx/pkg"
	slicepool "xxx/pkg/pool/slice"

	"github.com/mailru/easyjson"
	"go.uber.org/zap"
)

func (s *Server) exchangeKey(w http.ResponseWriter, r *http.Request) {
	// ====================================
	// CLIENT MUST GUARANTEE CONTRACT:
	//
	// 1. raw = X25519(clientPriv, serverPub), prk = HKDF-Extract(SHA256, raw, salt = clientPub‖serverPub).
	// 2. Each key: HKDF-Expand(prk, info = "xxx/iam/exchange-key/v1" ‖ deviceID(8 bytes LE) ‖ "/" ‖ label),
	//    label ∈ c2s/enc, s2c/enc, c2s/mac, s2c/mac.
	// 3. Client: send using c2s/*, receive using s2c/*.
	// 4. Within 20 seconds: TCP sends ActiveConnReq{token=deviceID LE, ticket, sign=Ed25519(identityPriv, ticket)}.
	// ======================================

	var (
		err  error
		req  json.ExchangeKeyReq
		resp *json.ExchangeKeyResp
	)

	claims := r.Context().Value(claimsKey).(*pkg.JWTClaims)
	r.Body = http.MaxBytesReader(w, r.Body, 1<<10)
	err = easyjson.UnmarshalFromReader(r.Body, &req)
	if err != nil {
		s.logger.Error("failed to unmarshal request", zap.Error(err))
		s.respondErr(w, http.StatusBadRequest, err)
		return
	}

	resp, err = s.keyExchanger.Exchange(r.Context(), claims.Subject, req)
	defer slicepool.Put(req.ClientPubKey)
	if err == nil {
		s.respond(w, resp)
		return
	}

	s.logger.Error("failed to exchange key", zap.Error(err))

	if errors.Is(err, exchange_key.ErrDeviceNotFound) {
		s.respondErr(w, http.StatusNotFound, err)
		return
	}
	if errors.Is(err, exchange_key.ErrDeviceUnactive) {
		s.respondErr(w, http.StatusForbidden, err)
		return
	}
	if errors.Is(err, exchange_key.ErrRequestInvalid) {
		s.respondErr(w, http.StatusBadRequest, err)
		return
	}
	s.respondErr(w, http.StatusInternalServerError, fmt.Errorf("internal error"))
}
