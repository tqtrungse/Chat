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

package scylla

import (
	"context"
	"sync"

	"xxx/internal/hub/connection"
	"xxx/internal/hub/domain/channel"
	"xxx/internal/hub/domain/device"
	"xxx/internal/hub/domain/message"
	"xxx/internal/hub/infrastructure/datastore/scylla/entity"

	"xxx/pkg/database/scylla"

	"github.com/scylladb/gocqlx/v3/qb"
)

const (
	maxPayloadBytes = 64 << 10 // entire blob: key + content + nonce/tag/framing
	maxPageLimit    = 100
)

type msgReader struct {
	buildQueryOne sync.Once
	queryStmt     string
	queryNames    []string
	scylla        *scylla.Scylla
	pageSize      int
}

// NewMsgReader
//
// pageSize is an upper bound on the number of messages returned, not a
// guarantee. A page may contain fewer than page limit messages, or even none,
// while more data is still available. This can happen when the server caps
// the page size in bytes or has to skip over deleted (tombstoned) messages.
// A pageSize of zero or above the implementation's maximum is clamped to that
// maximum. Default maximum is 100.
func NewMsgReader(
	scylla *scylla.Scylla,
	pageSize uint32,
) connection.MsgReader {
	if pageSize == 0 || pageSize > maxPageLimit {
		pageSize = maxPageLimit
	}
	return &msgReader{
		scylla:   scylla,
		pageSize: int(pageSize),
	}
}

func (m *msgReader) ListOfflineMsgs(
	ctx context.Context,
	state []byte,
	deviceID device.ID,
	dayBucket message.Date,
) ([]message.Offline, []byte, error) {
	m.buildQueryOne.Do(m.buildQuery)

	q := m.scylla.
		Session().
		ContextQuery(ctx, m.queryStmt, m.queryNames).
		BindMap(qb.M{"device_id": int64(deviceID), "day_bucket": int32(dayBucket)})
	q.PageSize(m.pageSize)
	q.Prefetch(0) // do not pre-fetch the next page
	q.PageState(state)

	iter := q.Iter()
	next := iter.PageState() // Get BEFORE scanning: resume point after this page
	numRows := iter.NumRows()
	msgs := make([]message.Offline, 0, numRows)
	for range numRows {
		var om entity.OfflineMessage
		if !iter.StructScan(&om) {
			break
		}
		msgs = append(msgs, message.Offline{
			DeviceID:       deviceID,
			SenderDeviceID: device.ID(om.SenderDeviceID),
			DayBucket:      dayBucket,
			MsgID:          message.ID(om.MsgID),
			ChannelID:      channel.ID(om.ChannelID),
			Payload:        om.Payload,
		})
	}
	if err := iter.Close(); err != nil {
		return nil, nil, err
	}
	return msgs, next, nil
}

func (m *msgReader) buildQuery() {
	m.queryStmt, m.queryNames = qb.
		Select("offline_messages").
		Columns(
			"sender_device_id",
			"msg_id",
			"channel_id",
			"payload",
		).
		Where(qb.Eq("device_id"), qb.Eq("day_bucket")).
		OrderBy("msg_id", qb.ASC).
		ToCql()
}
