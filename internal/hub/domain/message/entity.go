/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package message

import (
	"xxx/internal/hub/domain/channel"
	"xxx/internal/shared/device"
)

// Date
// Format yyyymmdd
type Date uint32

type Offline struct {
	DeviceID       device.ID
	SenderDeviceID device.ID
	DayBucket      Date
	MsgID          ID
	ChannelID      channel.ID
	DedupKey       uint64
	Payload        []byte
}
