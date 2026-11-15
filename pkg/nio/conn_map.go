/*
 * Copyright (c) 2023 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package nio

import "sync/atomic"

type connMatrix struct {
	connMap   map[int]*conn
	connCount atomic.Int32
}

func (cm *connMatrix) init(size int) {
	if size == 0 {
		cm.connMap = make(map[int]*conn)
	} else {
		cm.connMap = make(map[int]*conn, size)
	}
}

func (cm *connMatrix) iterate(f func(*conn) bool) {
	for _, c := range cm.connMap {
		f(c)
	}
}

func (cm *connMatrix) incCount(delta int32) {
	cm.connCount.Add(delta)
}

func (cm *connMatrix) loadCount() (n int32) {
	return cm.connCount.Load()
}

func (cm *connMatrix) addConn(c *conn) {
	//c.gfd = gfd.NewGFD(c.fd, index, 0, 0)
	cm.connMap[c.fd] = c
	cm.incCount(1)
}

func (cm *connMatrix) getConn(fd int) *conn {
	c, _ := cm.connMap[fd]
	return c
}

func (cm *connMatrix) delConn(c *conn) {
	delete(cm.connMap, c.fd)
	cm.incCount(-1)
}
