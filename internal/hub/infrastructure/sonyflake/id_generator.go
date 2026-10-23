/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package sonyflake

import (
	"xxx/internal/hub/application/sendmsg"

	"github.com/sony/sonyflake/v2"
)

type idGen struct {
	sf *sonyflake.Sonyflake
}

func NewIDGen(hubID uint64) (sendmsg.GenID, error) {
	sf, err := sonyflake.New(
		sonyflake.Settings{
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
