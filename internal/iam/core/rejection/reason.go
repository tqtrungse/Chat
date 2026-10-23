/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package rejection

type Reason uint8

// Append only; labels are used by metrics, dashboards, and alerts.
const (
	AuthHeader Reason = iota
	AuthToken
	RequestBody
	RequestDevice
	DeviceNotFound
	DeviceUnactive
	TicketRequest
	ResponseWrite

	numReasons
)

var names = [numReasons]string{
	AuthHeader:     "iam_auth_header",
	AuthToken:      "iam_auth_token",
	RequestBody:    "iam_request_body",
	RequestDevice:  "iam_request_device",
	DeviceNotFound: "iam_device_not_found",
	DeviceUnactive: "iam_device_unactive",
	TicketRequest:  "iam_ticket_request",
	ResponseWrite:  "iam_response_write",
}

// Names returns a copy of the stable labels indexed by Reason.
func Names() []string {
	return names[:]
}
