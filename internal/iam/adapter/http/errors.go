/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package http

import "errors"

var (
	errAuthHeaderNotFound    = errors.New("missing or invalid Authorization header")
	errAuthVerify            = errors.New("failed to verify token")
	errJsonMarshall          = errors.New("failed to marshall JSON")
	errInternal              = errors.New("internal error")
	errReqBodyInvalid        = errors.New("invalid request body")
	errTicketLimiterRequired = errors.New("ticket limiter is required")
	errTicketRateLimited     = errors.New("too many connection ticket requests")
	errTicketCapacity        = errors.New("connection ticket service temporarily overloaded")
)
