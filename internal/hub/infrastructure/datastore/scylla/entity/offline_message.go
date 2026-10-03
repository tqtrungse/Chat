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

package entity

type OfflineMessage struct {
	DeviceID       int64  `db:"device_id"`
	SenderDeviceID int64  `db:"sender_device_id"`
	DayBucket      int32  `db:"day_bucket"`
	MsgID          int64  `db:"msg_id"`
	ChannelID      int64  `db:"channel_id"`
	Payload        []byte `db:"payload"`
}
