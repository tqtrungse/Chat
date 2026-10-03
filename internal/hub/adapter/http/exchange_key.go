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
	"net/http"

	"xxx/api/hub/v1/json"

	"github.com/mailru/easyjson"
	"go.uber.org/zap"
)

func (s *Server) exchangeKey(w http.ResponseWriter, r *http.Request) {
	var (
		err  error
		req  json.ExchangeKeyReq
		resp *json.ExchangeKeyResp
	)

	//claims := r.Context().Value(claimsKey).(*pkg.Auth0Claims)
	err = easyjson.UnmarshalFromReader(r.Body, &req)
	if err != nil {
		s.logger.Error("failed to unmarshal request", zap.Error(err))
		s.respondErr(w, http.StatusBadRequest, err)
		return
	}

	resp, err = s.keyExchanger.Exchange(r.Context(), req)
	if err != nil {
		s.logger.Error("failed to exchange key", zap.Error(err))
		s.respondErr(w, http.StatusInternalServerError, err)
		return
	}
	s.respond(w, resp)
}
