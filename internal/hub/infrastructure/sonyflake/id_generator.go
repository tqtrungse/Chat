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

package sonyflake

import (
	"time"

	"xxx/internal/hub/application/send_message"

	"github.com/sony/sonyflake/v2"
)

var defaultStartTime = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

type idGen struct {
	sf *sonyflake.Sonyflake
}

func NewIDGen(hubID uint64) (send_message.GenID, error) {
	sf, err := sonyflake.New(
		sonyflake.Settings{
			StartTime: defaultStartTime,
			MachineID: func() (int, error) { return int(hubID), nil },
		},
	)
	if err != nil {
		return nil, err
	}

	return &idGen{sf: sf}, nil
}

func (i *idGen) NextID() (uint64, error) {
	id, err := i.sf.NextID()
	if err != nil {
		return 0, err
	}
	return uint64(id), nil
}
