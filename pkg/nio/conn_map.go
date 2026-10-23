/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package nio

import (
	"sync/atomic"

	"xxx/pkg/collection/swiss"
	"xxx/pkg/hash"
)

type connMatrix struct {
	connMap   *swiss.Table[int, *conn]
	connCount atomic.Int32
}

func (cm *connMatrix) init(size int) {
	if size == 0 {
		cm.connMap = swiss.NewTable[int, *conn](hash.Int)
	} else {
		cm.connMap = swiss.NewTableWithHintCap[int, *conn](size, hash.Int)
	}
}

func (cm *connMatrix) iterate(f func(*conn) bool) {
	cm.connMap.Range(func(_ int, value *conn) (goOn bool) {
		return f(value)
	})
}

func (cm *connMatrix) incCount(delta int32) {
	cm.connCount.Add(delta)
}

func (cm *connMatrix) loadCount() (n int32) {
	return cm.connCount.Load()
}

func (cm *connMatrix) addConn(c *conn) {
	//c.gfd = gfd.NewGFD(c.fd, index, 0, 0)
	cm.connMap.Set(c.fd, c)
	cm.incCount(1)
}

func (cm *connMatrix) getConn(fd int) *conn {
	c, _ := cm.connMap.Lookup(fd)
	return c
}

func (cm *connMatrix) delConn(c *conn) {
	cm.connMap.Delete(c.fd)
	cm.incCount(-1)
}
