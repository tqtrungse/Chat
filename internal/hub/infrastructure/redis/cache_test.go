/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package redis

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"xxx/internal/hub/core/presence"
	"xxx/internal/shared/device"

	"xxx/pkg/bs"
	"xxx/pkg/log"
	slicepool "xxx/pkg/pool/slice"

	"github.com/alicebob/miniredis/v2"
	redisv9 "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// -------------
//  Test helpers
// -------------

const testBucketCount = uint16(4)

// newTestCache spins up a fresh miniredis instance and a *Cache wired
// directly to it (white-box: same package as cache.go), bypassing Start()
// so most tests don't need pub/sub plumbing.
func newTestCache(t *testing.T, hubID uint64) (*Cache, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redisv9.NewClient(&redisv9.Options{Addr: mr.Addr()})
	logger, closeLogger, loggerErr := log.NewLogger(nil)
	require.NoError(t, loggerErr)
	t.Cleanup(func() {
		_ = rdb.Close()
		_ = closeLogger()
	})

	c := &Cache{
		rdb:                        rdb,
		logger:                     logger,
		done:                       make(chan struct{}),
		hubID:                      hubID,
		deviceBucketCount:          testBucketCount,
		reapBatchSize:              500,
		heartbeatInterval:          20 * time.Millisecond,
		heartbeatTTL:               100 * time.Millisecond,
		verifyExpiredNotifications: false,
	}
	return c, mr
}

// sharedCache builds a second *Cache pointed at an already-running
// miniredis instance, simulating another hub process.
func sharedCache(t *testing.T, mr *miniredis.Miniredis, hubID uint64) *Cache {
	t.Helper()
	rdb := redisv9.NewClient(&redisv9.Options{Addr: mr.Addr()})
	logger, closeLogger, loggerErr := log.NewLogger(nil)
	require.NoError(t, loggerErr)
	t.Cleanup(func() {
		_ = rdb.Close()
		_ = closeLogger()
	})
	return &Cache{
		rdb:               rdb,
		logger:            logger,
		done:              make(chan struct{}),
		hubID:             hubID,
		deviceBucketCount: testBucketCount,
		reapBatchSize:     500,
		heartbeatInterval: 20 * time.Millisecond,
		heartbeatTTL:      100 * time.Millisecond,
	}
}

func ctxT(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// attachHandlerSubs wires up all three subscribers handleEvents expects
// (delHub, delDevice, expired) so it's safe to call handleEvents directly
// without going through Start().
// handleEvents expects all three subscribers to be initialized before it
// starts. This helper wires them directly so tests can exercise event handling
// without going through Start.
func attachHandlerSubs(t *testing.T, c *Cache, ctx context.Context) {
	t.Helper()
	mustSub := func(channel string) *redisv9.PubSub {
		sub := c.rdb.Subscribe(ctx, channel)
		_, err := sub.Receive(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = sub.Close() })
		return sub
	}
	c.subscribers = subscribers{
		delHub:    mustSub(eventDelHub),
		delDevice: mustSub(eventDelDevice),
		expired:   mustSub(fmt.Sprintf("__keyevent@%d__:expired", c.dbIndex)),
	}
}

// ---------------
// CheckSelfReaped
// ---------------

func TestCheckSelfReaped_StillPresent_NoCallback(t *testing.T) {
	c, mr := newTestCache(t, 7)
	err := c.addHub(ctxT(t))
	require.NoError(t, err)

	ok, err := mr.SIsMember(hubsKey, "7")
	require.NoError(t, err)
	require.True(t, ok)

	called := false
	c.callbacks.OnSelfReaped = func() { called = true }

	c.checkSelfReaped(ctxT(t))
	require.False(t, called)
}

func TestCheckSelfReaped_WasReaped_ReregistersAndCallsBack(t *testing.T) {
	c, mr := newTestCache(t, 7)
	// Never registered (simulates: another instance already reaped us).
	called := false
	c.callbacks.OnSelfReaped = func() { called = true }

	c.checkSelfReaped(ctxT(t))
	require.True(t, called)

	ok, err := mr.SIsMember(hubsKey, "7")
	require.NoError(t, err)
	require.True(t, ok)
}

// ---------------
// BatchAddDevices
// ---------------

func TestBatchAddDevices_Empty(t *testing.T) {
	c, _ := newTestCache(t, 1)
	pending, err := c.BatchAddDevices(ctxT(t), nil)
	require.NoError(t, err)
	require.Nil(t, pending)
}

// Regression test: the ARGV layout for batchAddDevicesScript used to be
// scrambled (counts appended to the tail instead of interleaved with
// their device IDs, plus a wrong buffer size leaving trailing nils). With
// devices spread across every bucket, every single device must still end
// up owned by the right hub and every result flag must line up with the
// exact input order.
func TestBatchAddDevices_MultipleBuckets_AllDevicesOwnedCorrectly(t *testing.T) {
	c, _ := newTestCache(t, 9)
	ctx := ctxT(t)

	// 4 buckets (testBucketCount), 3 devices per bucket, deliberately out
	// of bucket order in the input slice.
	var devices []device.ID
	for round := range uint64(3) {
		for b := range uint64(testBucketCount) {
			devices = append(devices, device.ID(b+round*uint64(testBucketCount)))
		}
	}

	_, err := c.BatchAddDevices(ctx, devices)
	require.NoError(t, err)

	for _, dev := range devices {
		bucket := c.bucketFor(dev.Uint64())
		deviceKey := bs.Concat(devicesPrefix, uint64(bucket))
		bDeviceID := bs.U64ToBytes(dev.Uint64())
		owner := ""
		owner, err = c.rdb.HGet(
			ctx,
			bs.BytesToString(deviceKey),
			bs.BytesToString(bDeviceID),
		).Result()

		require.NoError(t, err)
		require.Equal(t, "9", owner)

		inSet := false
		hubDeviceKey := bs.Concat(hubDevicesPrefix, uint64(9))
		inSet, err = c.rdb.SIsMember(
			ctx,
			bs.BytesToString(hubDeviceKey),
			bDeviceID,
		).Result()

		slicepool.Put(deviceKey)
		slicepool.Put(bDeviceID)
		slicepool.Put(hubDeviceKey)

		require.NoError(t, err)
		require.True(t, inSet)
	}
}

func TestBatchAddDevices_MovesDevicesFromOldHub(t *testing.T) {
	c, _ := newTestCache(t, 2)
	ctx := ctxT(t)
	devices := []device.ID{10, 11, 12, 13}

	for _, dev := range devices {
		bucket := c.bucketFor(dev.Uint64())
		deviceKey := bs.Concat(devicesPrefix, uint64(bucket))
		bDeviceID := bs.U64ToBytes(dev.Uint64())
		hubDeviceKey := bs.Concat(hubDevicesPrefix, uint64(1))

		require.NoError(t, c.rdb.HSet(
			ctx,
			bs.BytesToString(deviceKey),
			bs.BytesToString(bDeviceID),
			"1",
		).Err())

		require.NoError(t, c.rdb.SAdd(
			ctx,
			bs.BytesToString(hubDeviceKey),
			bDeviceID,
		).Err())

		slicepool.Put(deviceKey)
		slicepool.Put(bDeviceID)
		slicepool.Put(hubDeviceKey)
	}

	_, err := c.BatchAddDevices(ctx, devices)
	require.NoError(t, err)

	oldHubKey := bs.Concat(hubDevicesPrefix, uint64(1))
	defer slicepool.Put(oldHubKey)

	newHubKey := bs.Concat(hubDevicesPrefix, uint64(2))
	defer slicepool.Put(newHubKey)

	// The old hub must no longer own any device.
	oldCount, err := c.rdb.SCard(
		ctx,
		bs.BytesToString(oldHubKey),
	).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), oldCount)

	// The new hub must own every device.
	newCount, err := c.rdb.SCard(
		ctx,
		bs.BytesToString(newHubKey),
	).Result()
	require.NoError(t, err)
	require.Equal(t, int64(len(devices)), newCount)

	for _, dev := range devices {
		bucket := c.bucketFor(dev.Uint64())
		deviceKey := bs.Concat(devicesPrefix, uint64(bucket))
		bDeviceID := bs.U64ToBytes(dev.Uint64())

		owner, err := c.rdb.HGet(
			ctx,
			bs.BytesToString(deviceKey),
			bs.BytesToString(bDeviceID),
		).Result()

		slicepool.Put(deviceKey)
		slicepool.Put(bDeviceID)

		require.NoError(t, err)
		require.Equal(t, "2", owner)

		inNewHub, err := c.rdb.SIsMember(
			ctx,
			bs.BytesToString(newHubKey),
			bDeviceID,
		).Result()

		require.NoError(t, err)
		require.True(t, inNewHub)
	}
}

func TestBatchAddDevices_DuplicateDeviceIDs(t *testing.T) {
	c, _ := newTestCache(t, 9)
	ctx := ctxT(t)

	devices := []device.ID{1, 5, 1, 9, 5}

	_, err := c.BatchAddDevices(ctx, devices)
	require.NoError(t, err)

	for _, dev := range []device.ID{1, 5, 9} {
		bucket := c.bucketFor(dev.Uint64())
		key := bs.Concat(devicesPrefix, uint64(bucket))
		field := bs.U64ToBytes(dev.Uint64())

		owner, err := c.rdb.HGet(
			ctx,
			bs.BytesToString(key),
			bs.BytesToString(field),
		).Result()

		slicepool.Put(key)
		slicepool.Put(field)

		require.NoError(t, err)
		require.Equal(t, "9", owner)
	}
}

// -----------------
// ListHubsByDevices
// -----------------

func TestListHubsByDevices_Empty(t *testing.T) {
	c, _ := newTestCache(t, 1)
	result, err := c.ListHubsByDevices(ctxT(t), nil)
	require.NoError(t, err)
	require.Empty(t, result)
}

func TestListHubsByDevices_MixedFoundAndMissing_PreservesInputOrder(t *testing.T) {
	c, _ := newTestCache(t, 1)
	ctx := ctxT(t)

	// Owned devices spread across every bucket; one device (99) is never
	// registered anywhere, so it must come back as 0.
	owners := map[device.ID]uint64{0: 10, 1: 20, 2: 30, 3: 40, 5: 50}
	for dev, hub := range owners {
		bucket := c.bucketFor(dev.Uint64())
		deviceKey := bs.Concat(devicesPrefix, uint64(bucket))
		bDeviceID := bs.U64ToBytes(dev.Uint64())
		bHubID := bs.U64ToBytes(hub)

		err := c.rdb.HSet(
			ctx,
			bs.BytesToString(deviceKey),
			bs.BytesToString(bDeviceID),
			bHubID,
		).Err()

		slicepool.Put(deviceKey)
		slicepool.Put(bDeviceID)
		slicepool.Put(bHubID)

		require.NoError(t, err)
	}

	query := []device.ID{5, 99, 0, 3, 1, 2} // shuffled, includes the unregistered one
	result, err := c.ListHubsByDevices(ctx, query)
	require.NoError(t, err)
	require.Equal(t, len(query), len(result))

	for i, dev := range query {
		want := owners[dev] // zero value for the unregistered device — correct expectation
		require.Equal(t, want, result[i])
	}
}

// ---------
// DelDevice
// ---------

func TestDelDevice_OwnedByCaller_RemovesAndPublishes(t *testing.T) {
	c, _ := newTestCache(t, 1)
	ctx := ctxT(t)
	dev := device.ID(500)

	_, err := c.BatchAddDevices(ctx, []device.ID{dev})
	require.NoError(t, err)

	sub := c.rdb.Subscribe(ctx, eventDelDevice)
	_, err = sub.Receive(ctx)
	require.NoError(t, err)
	defer func() { _ = sub.Close() }()

	err = c.BatchDelDevices(ctx, []device.ID{dev})
	require.NoError(t, err)

	bucket := c.bucketFor(dev.Uint64())
	deviceKey := bs.Concat(devicesPrefix, uint64(bucket))
	bDeviceID := bs.U64ToBytes(dev.Uint64())
	defer func() {
		slicepool.Put(deviceKey)
		slicepool.Put(bDeviceID)
	}()

	exists, err := c.rdb.HExists(
		ctx,
		bs.BytesToString(deviceKey),
		bs.BytesToString(bDeviceID),
	).Result()
	require.NoError(t, err)
	require.False(t, exists)

	hubDeviceKey := bs.Concat(hubDevicesPrefix, uint64(1))
	inSet, err := c.rdb.SIsMember(
		ctx,
		bs.BytesToString(hubDeviceKey),
		bDeviceID,
	).Result()
	slicepool.Put(hubDeviceKey)
	require.NoError(t, err)
	require.False(t, inSet)

	select {
	case msg := <-sub.Channel():
		require.Len(t, msg.Payload, 16)

		buf := bs.StringToBytes(msg.Payload)
		gotDev := binary.LittleEndian.Uint64(buf[:8])
		gotHub := binary.LittleEndian.Uint64(buf[8:16])
		require.Equal(t, dev.Uint64(), gotDev)
		require.Equal(t, uint64(1), gotHub)
	case <-time.After(2 * time.Second):
		require.Fail(t, "timed out waiting for del_device_event")
	}
}

func TestDelDevice_NotOwner_NoOp(t *testing.T) {
	var (
		c, _         = newTestCache(t, 2) // caller is hub 2
		ctx          = ctxT(t)
		dev          = device.ID(600)
		bucket       = c.bucketFor(dev.Uint64())
		hubDeviceKey = bs.Concat(hubDevicesPrefix, uint64(1))
		deviceKey    = bs.Concat(devicesPrefix, uint64(bucket))
		bDeviceID    = bs.U64ToBytes(dev.Uint64())
	)
	defer func() {
		slicepool.Put(deviceKey)
		slicepool.Put(bDeviceID)
		slicepool.Put(hubDeviceKey)
	}()

	// Owned by hub 1, not by the caller.
	err := c.rdb.HSet(
		ctx,
		bs.BytesToString(deviceKey),
		bs.BytesToString(bDeviceID),
		"1",
	).Err()
	require.NoError(t, err)

	err = c.rdb.SAdd(
		ctx,
		bs.BytesToString(hubDeviceKey),
		bDeviceID,
	).Err()
	require.NoError(t, err)

	err = c.BatchDelDevices(ctx, []device.ID{dev})
	require.NoError(t, err)

	owner, err := c.rdb.HGet(
		ctx,
		bs.BytesToString(deviceKey),
		bs.BytesToString(bDeviceID),
	).Result()
	require.NoError(t, err)
	require.Equal(t, "1", owner)
}

func TestBatchDelDevices_Empty(t *testing.T) {
	c, _ := newTestCache(t, 1)

	require.NoError(t, c.BatchDelDevices(ctxT(t), nil))
}

func TestBatchDelDevices_MixedOwnershipDeletesOnlyOwnedDevices(t *testing.T) {
	c, mr := newTestCache(t, 1)
	ctx := ctxT(t)

	otherCache := sharedCache(t, mr, 2)

	owned1 := device.ID(1)
	other := device.ID(2)
	owned2 := device.ID(3)
	missing := device.ID(4)

	_, err := c.BatchAddDevices(ctx, []device.ID{owned1, owned2})
	require.NoError(t, err)

	_, err = otherCache.BatchAddDevices(ctx, []device.ID{other})
	require.NoError(t, err)

	require.NoError(t, c.BatchDelDevices(ctx, []device.ID{
		owned1,
		other,
		owned2,
		missing,
	}))

	result, err := c.ListHubsByDevices(
		ctx,
		[]device.ID{owned1, other, owned2, missing},
	)
	require.NoError(t, err)

	require.Equal(t, []uint64{
		0, // owned1 deleted
		2, // other hub's device untouched
		0, // owned2 deleted
		0, // missing
	}, result)
}

// -------
// reapHub
// -------

func TestReapHub_DrainsDevicesAcrossBuckets(t *testing.T) {
	c, _ := newTestCache(t, 1)
	ctx := ctxT(t)

	var devices []device.ID
	for i := range uint64(20) {
		devices = append(devices, device.ID(i))
	}

	_, err := c.BatchAddDevices(ctx, devices)
	require.NoError(t, err)

	err = c.addHub(ctx)
	require.NoError(t, err)

	err = c.reapHub(ctx, 1)
	require.NoError(t, err)

	isMember, err := c.rdb.SIsMember(
		ctx,
		hubsKey,
		"1",
	).Result()
	require.NoError(t, err)
	require.False(t, isMember)

	hubDeviceKey := bs.Concat(hubDevicesPrefix, uint64(1))
	remaining, err := c.rdb.SCard(
		ctx,
		bs.BytesToString(hubDeviceKey),
	).Result()
	slicepool.Put(hubDeviceKey)
	require.NoError(t, err)
	require.Equal(t, int64(0), remaining)

	for _, dev := range devices {
		bucket := c.bucketFor(dev.Uint64())
		deviceKey := bs.Concat(devicesPrefix, uint64(bucket))
		bDeviceID := bs.U64ToBytes(dev.Uint64())
		exists, err := c.rdb.HExists(
			ctx,
			bs.BytesToString(deviceKey),
			bs.BytesToString(bDeviceID),
		).Result()
		slicepool.Put(deviceKey)
		slicepool.Put(bDeviceID)

		require.NoError(t, err)
		require.False(t, exists)
	}
}

// Regression test for the ownership guard documented on
// reapDevicesInBucketScript: if a device was reclaimed by a live hub
// between the crashed hub's SRANDMEMBER read and the HDEL, the device
// must be left alone in devices:{bucket} (it belongs to the new owner
// now), even though it's still removed from the crashed hub's own set.
func TestReapHub_LeavesReclaimedDeviceAlone(t *testing.T) {
	var (
		c, _          = newTestCache(t, 1)
		ctx           = ctxT(t)
		dev           = device.ID(700)
		bucket        = c.bucketFor(dev.Uint64())
		hubDeviceKey  = bs.Concat(hubDevicesPrefix, uint64(1))
		hubDeviceKey2 = bs.Concat(hubDevicesPrefix, uint64(2))
		deviceKey     = bs.Concat(devicesPrefix, uint64(bucket))
		bDeviceID     = bs.U64ToBytes(dev.Uint64())
	)
	defer func() {
		slicepool.Put(deviceKey)
		slicepool.Put(bDeviceID)
		slicepool.Put(hubDeviceKey)
		slicepool.Put(hubDeviceKey2)
	}()

	// Device was owned by hub 1 (about to crash)...
	err := c.rdb.SAdd(
		ctx,
		bs.BytesToString(hubDeviceKey),
		bDeviceID,
	).Err()
	require.NoError(t, err)

	// ...but by the time we reap, hub 2 already reclaimed it in devices:{bucket}.
	err = c.rdb.HSet(
		ctx,
		bs.BytesToString(deviceKey),
		bs.BytesToString(bDeviceID),
		"2",
	).Err()
	require.NoError(t, err)

	err = c.rdb.SAdd(
		ctx,
		bs.BytesToString(hubDeviceKey2),
		bDeviceID,
	).Err()
	require.NoError(t, err)

	err = c.reapHub(ctx, 1)
	require.NoError(t, err)

	owner, err := c.rdb.HGet(
		ctx,
		bs.BytesToString(deviceKey),
		bs.BytesToString(bDeviceID),
	).Result()
	require.NoError(t, err)
	require.Equal(t, "2", owner)

	stillInOld, err := c.rdb.SIsMember(
		ctx,
		bs.BytesToString(hubDeviceKey),
		bDeviceID,
	).Result()
	require.NoError(t, err)
	require.False(t, stillInOld)

	stillInNew, err := c.rdb.SIsMember(
		ctx,
		bs.BytesToString(hubDeviceKey2),
		bDeviceID,
	).Result()
	require.NoError(t, err)
	require.True(t, stillInNew)
}

func TestReapHub_SecondCallerYieldsWhenLockHeld(t *testing.T) {
	c, mr := newTestCache(t, 1)
	ctx := ctxT(t)
	err := c.addHub(ctx)
	require.NoError(t, err)

	// Simulate another instance already holding the reap lock for hub 1.
	other := sharedCache(t, mr, 99)
	lockKey := bs.BytesToString(bs.Concat(hubReapLockPrefix, uint64(1)))
	err = other.rdb.SetNX(ctx, lockKey, 1, time.Minute).Err()
	require.NoError(t, err)

	err = c.reapHub(ctx, 1)
	require.NoError(t, err)

	// Hub must still be registered — this instance backed off instead of
	// racing the reap.
	isMember, err := c.rdb.SIsMember(ctx, hubsKey, "1").Result()
	require.NoError(t, err)
	require.True(t, isMember)
}

func TestReapHub_NoDevices_StillRemovesHubAndHeartbeat(t *testing.T) {
	c, _ := newTestCache(t, 1)
	ctx := ctxT(t)

	err := c.addHub(ctx)
	require.NoError(t, err)

	hbKey := bs.BytesToString(bs.Concat(hubHeartbeatPrefix, uint64(1)))
	err = c.rdb.Set(ctx, hbKey, 1, time.Minute).Err()
	require.NoError(t, err)

	err = c.reapHub(ctx, 1)
	require.NoError(t, err)

	isMember, err := c.rdb.SIsMember(ctx, hubsKey, "1").Result()
	require.NoError(t, err)
	require.False(t, isMember)

	exists, err := c.rdb.Exists(ctx, hbKey).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), exists)
}

func TestReapHub_DrainsDevicesAcrossMultipleBatches(t *testing.T) {
	c, _ := newTestCache(t, 1)
	ctx := ctxT(t)

	c.reapBatchSize = 3

	var devices []device.ID
	for i := range uint64(10) {
		devices = append(devices, device.ID(i))
	}

	_, err := c.BatchAddDevices(ctx, devices)
	require.NoError(t, err)
	require.NoError(t, c.addHub(ctx))

	require.NoError(t, c.reapHub(ctx, 1))

	// Hub must be removed.
	isMember, err := c.rdb.SIsMember(ctx, hubsKey, "1").Result()
	require.NoError(t, err)
	require.False(t, isMember)

	// Hub device set must be completely drained.
	hubDeviceKey := bs.Concat(hubDevicesPrefix, uint64(1))
	defer slicepool.Put(hubDeviceKey)

	count, err := c.rdb.SCard(
		ctx,
		bs.BytesToString(hubDeviceKey),
	).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), count)

	// No device may still point to the reaped hub.
	for _, dev := range devices {
		bucket := c.bucketFor(dev.Uint64())
		deviceKey := bs.Concat(devicesPrefix, uint64(bucket))
		bDeviceID := bs.U64ToBytes(dev.Uint64())

		exists, err := c.rdb.HExists(
			ctx,
			bs.BytesToString(deviceKey),
			bs.BytesToString(bDeviceID),
		).Result()

		slicepool.Put(deviceKey)
		slicepool.Put(bDeviceID)

		require.NoError(t, err)
		require.False(t, exists)
	}
}

// ----------------------------------------------------------------------
// pub/sub event handlers
//
// These construct the subscriber directly (bypassing Start) and run the
// handler goroutine manually, so they exercise the exact wire format
// addHubScript/deleteHubScript/delDeviceScript publish and that
// onAddHubEvent/onDelHubEvent/onDelDeviceEvent parse — this is the
// regression coverage for the earlier decimal-string-vs-binary payload
// mismatch and the stale fixed-length payload checks.
// ----------------------------------------------------------------------

func TestOnDelHubEvent_ReceivesOtherHubsDel(t *testing.T) {
	c, mr := newTestCache(t, 1)
	ctx := ctxT(t)

	other := sharedCache(t, mr, 555)
	err := other.addHub(ctx)
	require.NoError(t, err)

	attachHandlerSubs(t, c, ctx)

	got := make(chan uint64, 1)
	c.callbacks.OnHubDeleted = func(hubID uint64) {
		got <- hubID
	}

	go c.handleEvents(ctx)

	require.NoError(t, other.reapHub(ctx, 555))

	select {
	case hubID := <-got:
		require.Equal(t, uint64(555), hubID)
	case <-time.After(2 * time.Second):
		require.Fail(t, "timed out waiting for del-hub event")
	}

}

func TestOnDelDeviceEvent_ReceivesOtherHubsDelete_FiltersSelf(t *testing.T) {
	c, mr := newTestCache(t, 1)
	ctx := ctxT(t)

	other := sharedCache(t, mr, 2)
	dev := device.ID(42)
	_, err := other.BatchAddDevices(ctx, []device.ID{dev})
	require.NoError(t, err)

	attachHandlerSubs(t, c, ctx)

	type call struct {
		hubID uint64
		dev   device.ID
	}

	got := make(chan call, 1)
	c.callbacks.OnDeviceDeleted = func(hubID uint64, deviceID device.ID) {
		got <- call{hubID, deviceID}
	}

	go c.handleEvents(ctx)

	err = other.BatchDelDevices(ctx, []device.ID{dev})
	require.NoError(t, err)

	select {
	case cl := <-got:
		require.Equal(t, uint64(2), cl.hubID)
		require.Equal(t, dev, cl.dev)
	case <-time.After(2 * time.Second):
		require.Fail(t, "timed out waiting for del-device event")
	}

	// After receiving an event from another hub, Hub 1 deletes its own device.
	_, err = c.BatchAddDevices(ctx, []device.ID{999})
	require.NoError(t, err)
	err = c.BatchDelDevices(ctx, []device.ID{999})
	require.NoError(t, err)

	select {
	case cl := <-got:
		require.Fail(t, "cbDelDevice fired for the hub's own delete", "got %+v", cl)
	case <-time.After(200 * time.Millisecond):
		// as expected: no callback for its own event
	}
}

// ------------
// Start / Stop
// ------------

func TestStart_PingFailure_ReturnsErrorAndClosesRedis(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redisv9.NewClient(&redisv9.Options{
		Addr: mr.Addr(),
	})

	logger, closeLogger, loggerErr := log.NewLogger(nil)
	require.NoError(t, loggerErr)
	defer func() { _ = closeLogger() }()

	c := &Cache{
		rdb:               rdb,
		logger:            logger,
		done:              make(chan struct{}),
		hubID:             1,
		deviceBucketCount: testBucketCount,
		reapBatchSize:     500,
		heartbeatInterval: time.Hour,
		heartbeatTTL:      time.Hour,
	}

	// Make Redis unavailable before Start().
	mr.Close()

	err := c.Start(ctxT(t))
	require.Error(t, err)

	// Start() closes the Redis client when Ping fails.
	require.Error(t, c.rdb.Ping(ctxT(t)).Err())
}

func TestStart_Success_RegistersHubAndSubscribes(t *testing.T) {
	c, _ := newTestCache(t, 1)
	c.callbacks.OnHubDeleted = func(hubID uint64) {}
	c.callbacks.OnDeviceDeleted = func(hubID uint64, deviceID device.ID) {}
	ctx := ctxT(t)

	require.NoError(t, c.Start(ctx))
	defer func() { _ = c.Close() }()

	isMember, err := c.rdb.SIsMember(ctx, hubsKey, "1").Result()
	require.NoError(t, err)
	require.True(t, isMember)

	hubHeartbeatKey := bs.Concat(hubHeartbeatPrefix, uint64(1))
	ttl, err := c.rdb.PTTL(
		ctx,
		bs.BytesToString(hubHeartbeatKey),
	).Result()
	slicepool.Put(hubHeartbeatKey)
	require.NoError(t, err)
	require.Greater(t, ttl, time.Duration(0))
}

// End-to-end: two independent cache instances against the same Redis,
// wired through Start(), actually see each other's hub add/remove events
// over real pub/sub — the full addHubScript/deleteHubScript -> onAddHubEvent
// /onDelHubEvent pipeline, payload format included.
func TestStart_CrossInstance_DelEventPropagates(t *testing.T) {
	mr := miniredis.RunT(t)
	ctx := ctxT(t)

	newStarted := func(hubID uint64) (*Cache, chan uint64) {
		rdb := redisv9.NewClient(&redisv9.Options{Addr: mr.Addr()})
		logger, closeLogger, loggerErr := log.NewLogger(nil)
		require.NoError(t, loggerErr)
		t.Cleanup(func() {
			_ = rdb.Close()
			_ = closeLogger()
		})
		c := &Cache{
			rdb:               rdb,
			logger:            logger,
			done:              make(chan struct{}),
			deviceBucketCount: testBucketCount,
			hubID:             hubID,
			reapBatchSize:     500,
			heartbeatInterval: time.Hour,
			heartbeatTTL:      time.Hour,
		}
		delCh := make(chan uint64, 4)
		c.callbacks.OnHubDeleted = func(hubID uint64) { delCh <- hubID }
		c.callbacks.OnDeviceDeleted = func(uint64, device.ID) {}

		require.NoError(t, c.Start(ctx))
		t.Cleanup(func() { _ = c.Close() })
		return c, delCh
	}

	_, delA := newStarted(1)
	hubB, _ := newStarted(2)

	require.NoError(t, hubB.ReapHub(ctx))

	select {
	case id := <-delA:
		require.Equal(t, uint64(2), id)
	case <-time.After(2 * time.Second):
		require.Fail(t, "hubA never saw hub 2's del event")
	}
}

func TestStop_AfterStart_StopsBackgroundHandlerAndClosesRedis(t *testing.T) {
	c, _ := newTestCache(t, 1)
	ctx := ctxT(t)

	c.callbacks.OnHubDeleted = func(uint64) {}
	c.callbacks.OnDeviceDeleted = func(uint64, device.ID) {}

	require.NoError(t, c.Start(ctx))

	require.NoError(t, c.Close())

	// handleEvents must have exited and closed done before Stop returns.
	select {
	case <-c.done:
	default:
		require.Fail(t, "expected cache background handler to stop")
	}

	// Redis client must be closed by Stop().
	err := c.rdb.Ping(ctxT(t)).Err()
	require.Error(t, err)
}

func TestDeviceBatchesEnforceLimitBeforeRedisCommands(t *testing.T) {
	cache, mr := newTestCache(t, 7)
	ctx := ctxT(t)
	ids := make([]device.ID, presence.MaxBatchSize)
	for i := range ids {
		ids[i] = device.ID(i + 1)
	}
	pending, err := cache.BatchAddDevices(ctx, ids)
	require.NoError(t, err)
	require.Empty(t, pending, "devices have no pending offline message dates")

	oversized := append(append([]device.ID(nil), ids...), device.ID(presence.MaxBatchSize+1))
	before := mr.CommandCount()
	pending, err = cache.BatchAddDevices(ctx, oversized)
	require.ErrorIs(t, err, ErrBatchTooLarge)
	require.Nil(t, pending)
	require.False(
		t,
		cache.IsInfraError(err),
		"oversized input must not trigger infrastructure retries",
	)
	require.ErrorIs(
		t,
		cache.BatchDelDevices(ctx, oversized),
		ErrBatchTooLarge,
	)
	require.Equal(
		t,
		before,
		mr.CommandCount(),
		"oversized requests must not send Redis commands",
	)

	owners, err := cache.ListHubsByDevices(ctx, ids)
	require.NoError(t, err)

	for _, owner := range owners {
		require.Equal(t, uint64(7), owner, "rejected deletion must leave ownership unchanged")
	}
	require.NoError(t, cache.BatchDelDevices(ctx, ids))

	owners, err = cache.ListHubsByDevices(ctx, ids)
	require.NoError(t, err)

	for _, owner := range owners {
		require.Zero(t, owner)
	}
}

func newLifecycleCache(t *testing.T) *Cache {
	t.Helper()
	mr := miniredis.RunT(t)
	logger, closeLogger, loggerErr := log.NewLogger(nil)
	require.NoError(t, loggerErr)
	t.Cleanup(func() { require.NoError(t, closeLogger()) })
	c := NewCache(7, Config{Addr: mr.Addr(), HeartbeatInterval: time.Hour, HeartbeatTTL: 2 * time.Hour}, logger, nil)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	return c
}

func TestCacheSetCallbacksBeforeStart(t *testing.T) {
	c := newLifecycleCache(t)
	var got uint64
	require.NoError(t, c.SetCallbacks(Callbacks{OnHubDeleted: func(id uint64) { got = id }}))
	require.ErrorIs(t, c.SetCallbacks(Callbacks{}), ErrCallbacksAlreadySet)
	c.callbacks.OnHubDeleted(42)
	require.Equal(t, uint64(42), got, "duplicate setup must preserve the original callback")
	require.NotPanics(t, func() { c.callbacks.OnDeviceDeleted(7, 1); c.callbacks.OnSelfReaped() })
	require.NoError(t, c.Start(ctxT(t)))
	require.ErrorIs(t, c.SetCallbacks(Callbacks{}), ErrCallbacksAfterStart)
	require.ErrorIs(t, c.Start(ctxT(t)), ErrCacheStarted)
	require.NoError(t, c.Close())
	require.NoError(t, c.Close())
	require.ErrorIs(t, c.SetCallbacks(Callbacks{}), ErrCacheClosed)
	require.ErrorIs(t, c.Start(ctxT(t)), ErrCacheClosed)
}

func TestCacheCloseBeforeStart(t *testing.T) {
	c := newLifecycleCache(t)
	require.NoError(t, c.Close())
	require.NoError(t, c.Close())
	require.ErrorIs(t, c.Start(ctxT(t)), ErrCacheClosed)
}

func TestCacheConcurrentSetCallbacks(t *testing.T) {
	c := newLifecycleCache(t)
	results := make(chan error, 16)
	var wg sync.WaitGroup
	for range cap(results) {
		wg.Go(func() { results <- c.SetCallbacks(Callbacks{}) })
	}
	wg.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
			continue
		}
		require.ErrorIs(t, err, ErrCallbacksAlreadySet)
	}
	require.Equal(t, 1, successes)
}

func TestCacheConcurrentStartAndSetCallbacks(t *testing.T) {
	c := newLifecycleCache(t)
	startErr := make(chan error, 1)
	callbackErr := make(chan error, 1)
	ready := make(chan struct{})
	ctx := ctxT(t)
	go func() { <-ready; startErr <- c.Start(ctx) }()
	go func() { <-ready; callbackErr <- c.SetCallbacks(Callbacks{}) }()
	close(ready)
	require.NoError(t, <-startErr)
	err := <-callbackErr
	require.True(t, err == nil || errors.Is(err, ErrCallbacksAfterStart))
	require.NotNil(t, c.callbacks.OnHubDeleted)
}

func TestCacheConcurrentStartAndClose(t *testing.T) {
	c := newLifecycleCache(t)
	ctx := ctxT(t)
	startErr := make(chan error, 1)
	closeErr := make(chan error, 1)
	ready := make(chan struct{})
	go func() { <-ready; startErr <- c.Start(ctx) }()
	go func() { <-ready; closeErr <- c.Close() }()
	close(ready)
	err := <-startErr
	require.True(t, err == nil || errors.Is(err, ErrCacheClosed))
	require.NoError(t, <-closeErr)
	require.ErrorIs(t, c.rdb.Ping(ctx).Err(), redisv9.ErrClosed)
	require.ErrorIs(t, c.Start(ctx), ErrCacheClosed)
	if c.running.Load() {
		select {
		case <-c.done:
		default:
			require.Fail(t, "Close must wait for the event handler to stop")
		}
	}
}
