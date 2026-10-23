/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package http

import (
	"errors"
	"net/http"
	"strconv"

	"xxx/api/iam/v2/json"
	"xxx/internal/iam/application/connticket"
	"xxx/internal/iam/core/rejection"
	"xxx/internal/iam/core/ticketlimit"

	"xxx/pkg/jwt"
	slicepool "xxx/pkg/pool/slice"

	"github.com/mailru/easyjson"
	"go.uber.org/zap"
)

// issueConnectionTicket applies subject admission before parsing the bounded
// request body, then device admission before ticket issuance. It writes the
// response, releases the pooled ticket, and maps issuance errors to HTTP status codes.
func (s *Server) issueConnectionTicket(w http.ResponseWriter, r *http.Request) {
	claims, ok := r.Context().Value(claimsContextKey).(*jwt.Claims)
	if !ok || claims == nil || claims.Subject == "" {
		s.rejections.Record(rejection.AuthToken, errAuthVerify)
		s.respondErr(w, http.StatusUnauthorized, errAuthVerify)
		return
	}

	decision := s.ticketLimiter.AllowSubject(claims.Subject)
	if decision.Reason != ticketlimit.Allowed {
		s.respondTicketRateLimited(w, decision)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1<<10)

	var req json.ConnectionTicketReq
	err := easyjson.UnmarshalFromReader(r.Body, &req)
	if err != nil {
		// Decoder errors can contain request values; log only the rejection category.
		s.rejections.Record(rejection.RequestBody, errReqBodyInvalid)
		s.respondErr(w, http.StatusBadRequest, errReqBodyInvalid)
		return
	}

	// Match the use case's numeric identity so alternate spellings such as
	// "42" and "042" cannot create separate device quotas.
	id, err := strconv.ParseUint(req.DeviceID, 10, 64)
	if err != nil || id == 0 {
		s.rejections.Record(rejection.RequestDevice, connticket.ErrRequestInvalid)
		s.respondErr(w, http.StatusBadRequest, connticket.ErrRequestInvalid)
		return
	}

	decision = s.ticketLimiter.AllowDevice(claims.Subject, id)
	if decision.Reason != ticketlimit.Allowed {
		s.respondTicketRateLimited(w, decision)
		return
	}

	resp, err := s.ticketIssuer.Issue(r.Context(), claims.Subject, req)
	if err == nil {
		defer func() {
			clear(resp.Ticket[:cap(resp.Ticket)])
			slicepool.Put(resp.Ticket)
		}()
		s.respond(w, resp)
		return
	}

	if errors.Is(err, connticket.ErrDeviceNotFound) {
		s.rejections.Record(rejection.DeviceNotFound, connticket.ErrDeviceNotFound)
		s.respondErr(w, http.StatusNotFound, err)
		return
	}

	if errors.Is(err, connticket.ErrDeviceUnactive) {
		s.rejections.Record(rejection.DeviceUnactive, connticket.ErrDeviceUnactive)
		s.respondErr(w, http.StatusForbidden, err)
		return
	}

	if errors.Is(err, connticket.ErrRequestInvalid) {
		s.rejections.Record(rejection.TicketRequest, connticket.ErrRequestInvalid)
		s.respondErr(w, http.StatusBadRequest, err)
		return
	}

	s.logger.Error(
		"failed to issue connection ticket",
		zap.String("device_id", req.DeviceID),
		zap.Error(err),
	)

	s.respondErr(w, http.StatusInternalServerError, errInternal)
}
