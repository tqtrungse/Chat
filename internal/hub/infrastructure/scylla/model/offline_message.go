/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package model

type OfflineMessage struct {
	DeviceID       int64  `db:"device_id"`
	SenderDeviceID int64  `db:"sender_device_id"`
	DayBucket      int32  `db:"day_bucket"`
	MsgID          int64  `db:"msg_id"`
	ChannelID      int64  `db:"channel_id"`
	DedupKey       int64  `db:"dedup_key"`
	Payload        []byte `db:"payload"`
}
