/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package redis

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"xxx/internal/hub/core/presence"
	"xxx/internal/hub/domain/message"
	"xxx/internal/shared/device"

	"xxx/pkg/bit"
	"xxx/pkg/bs"
	"xxx/pkg/log"
	slicepool "xxx/pkg/pool/slice"
	workerpool "xxx/pkg/pool/worker"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	eventDelHub    = "del_hub_event"
	eventDelDevice = "del_device_event"

	hubsKey            = "hubs"
	hubReapLockPrefix  = "hub:reaping:"
	hubHeartbeatPrefix = "hub:heartbeat:"
	hubDevicesPrefix   = "hub:devices:"

	// devicesPrefix plus a bucket index is the key of one shard of the
	// device registry, for example "devices:0", "devices:1", up to
	// "devices:{N-1}". The code splits the one global "devices" hash into
	// many bucket hashes. This keeps each hash under the Redis
	// hash-max-listpack-entries limit. Redis then keeps the compact
	// listpack encoding, which has far less overhead per field. Without
	// the split, a hash above that limit uses the heavier hashtable
	// encoding instead. See DeviceBucketCount in Config for the size of N.
	devicesPrefix = "devices:"

	pendingBucketsPrefix = "pending_buckets:"

	reapLockTTL = 30 * time.Second
)

type Callbacks struct {
	OnHubDeleted    func(hubID uint64)
	OnDeviceDeleted func(hubID uint64, deviceID device.ID)

	// OnSelfReaped runs when this hub detects that another instance may
	// have reaped it. This can happen during a network partition, when
	// this hub loses contact with the cache for longer than
	// HeartbeatTTL, while its own TCP clients stay connected. The cache
	// layer only detects and reports this event. The application must
	// decide the response: disconnect all local TCP clients so they
	// reconnect to a live hub, stop the process, or register the hub
	// again from scratch.
	OnSelfReaped func()
}

type subscribers struct {
	delHub    *redis.PubSub
	delDevice *redis.PubSub
	expired   *redis.PubSub
}

// Cache owns the Redis commands, the subscriptions, and the hub heartbeat
// lifecycle. Set the callbacks before Start. The callbacks do not change
// after that, while the cache handles events.
type Cache struct {
	rdb                        *redis.Client
	logger                     *log.Logger
	pool                       *workerpool.Pool
	subscribers                subscribers
	callbacks                  Callbacks
	done                       chan struct{}
	heartbeatInterval          time.Duration
	heartbeatTTL               time.Duration
	hubID                      uint64
	dbIndex                    uint16
	reapBatchSize              uint16
	deviceBucketCount          uint16
	selfReapChecking           atomic.Bool
	starting                   atomic.Bool
	running                    atomic.Bool
	closeOnce                  sync.Once
	closeErr                   error
	lifecycleMu                sync.Mutex
	callbacksSet               bool
	closed                     bool
	verifyExpiredNotifications bool
}

// NewCache creates a Redis-backed cache. The cache does not start yet. Set
// any callbacks before Start. Close is safe before Start, after a failed
// Start, and on repeated calls.
func NewCache(
	hubID uint64,
	cfg Config,
	logger *log.Logger,
	pool *workerpool.Pool,
) *Cache {
	cfg.applyDefault()

	rdb := redis.NewClient(&redis.Options{
		Dialer:           newDialer(&cfg),
		Addr:             cfg.Addr,
		Password:         cfg.Password,
		ReadTimeout:      cfg.ReadTimeout,
		WriteTimeout:     cfg.WriteTimeout,
		DB:               int(cfg.DB),
		PoolSize:         int(cfg.PoolSize),
		MaxIdleConns:     int(cfg.MaxIdleConns),
		MaxActiveConns:   int(cfg.MaxActiveConns),
		PipelinePoolSize: cfg.PipelinePoolSize,
	})

	cc := &Cache{
		rdb:                        rdb,
		logger:                     logger,
		pool:                       pool,
		done:                       make(chan struct{}),
		heartbeatInterval:          cfg.HeartbeatInterval,
		heartbeatTTL:               cfg.HeartbeatTTL,
		hubID:                      hubID,
		dbIndex:                    cfg.DB,
		reapBatchSize:              cfg.ReapBatchSize,
		deviceBucketCount:          cfg.DeviceBucketCount,
		verifyExpiredNotifications: cfg.VerifyExpiredNotifications,
		callbacks: Callbacks{
			OnHubDeleted:    func(uint64) {},
			OnDeviceDeleted: func(uint64, device.ID) {},
			OnSelfReaped:    func() {},
		},
	}
	return cc
}

// ReapHub removes the hub and reaps the devices still linked to it. The
// distributed reap lock ensures that concurrent cache instances do not
// reap the same hub at the same time.
func (c *Cache) ReapHub(ctx context.Context) error {
	return c.reapHub(ctx, c.hubID)
}

// ListHubsByDevices returns the hub ID that owns each device ID now. The
// result keeps the same order as deviceIDs. A zero value means that the
// device has no registered hub.
func (c *Cache) ListHubsByDevices(
	ctx context.Context,
	deviceIDs []device.ID,
) ([]uint64, error) {
	if len(deviceIDs) == 0 {
		return []uint64{}, nil
	}

	// byBucket maps a bucket to the indices into deviceIDs and result
	// for that bucket.
	byBucket := make(map[uint32][]int)
	for i, deviceID := range deviceIDs {
		bucket := c.bucketFor(deviceID.Uint64())
		byBucket[bucket] = append(byBucket[bucket], i)
	}

	var (
		pipe      = c.rdb.Pipeline()
		cmds      = make(map[uint32]*redis.Cmd, len(byBucket))
		arrKeys   = make([][]byte, 0, len(byBucket))
		arrFields = make([][]byte, 0, len(deviceIDs))
	)

	for bucket, idxs := range byBucket {
		bucketKey := bs.Concat(devicesPrefix, uint64(bucket))
		arrKeys = append(arrKeys, bucketKey)

		// HMGET takes its fields as separate arguments. The code
		// flattens the pooled field buffers into args. It does not pass
		// them as one slice.
		args := make([]any, 0, 2+len(idxs))
		args = append(args, "hmget", bucketKey)
		for _, idx := range idxs {
			field := bs.U64ToBytes(deviceIDs[idx].Uint64())
			arrFields = append(arrFields, field)
			args = append(args, field)
		}
		cmds[bucket] = pipe.Do(ctx, args...)
	}

	_, err := pipe.Exec(ctx)
	defer func() {
		// Release the pooled buffers.
		for _, key := range arrKeys {
			slicepool.Put(key)
		}
		for _, field := range arrFields {
			slicepool.Put(field)
		}
	}()
	if err != nil {
		return nil, err
	}

	result := make([]uint64, len(deviceIDs))
	for bucket, idxs := range byBucket {
		var values []any
		values, err = cmds[bucket].Slice()
		if err != nil {
			return nil, err
		}

		parsed := arrAnyToU64s(values)
		for j, idx := range idxs {
			result[idx] = parsed[j]
		}
	}
	return result, nil
}

// IsInfraError reports whether err shows a problem with the Redis
// connection, the node, or the cluster itself. RedisBreaker must trip on
// this kind of failure.
//
// A command problem is different: bad arguments, wrong type, no
// permission, a missing key, and so on. This kind of error keeps
// happening even when Redis is healthy. It must not cause a fail-fast for
// every other caller.
//
// The function uses only the exported API of
// github.com/redis/go-redis/v9: the typed Is*Error helpers and the
// sentinel variables. This way, the function keeps working across
// go-redis versions. It does not reach into the internal packages of
// go-redis.
func (c *Cache) IsInfraError(err error) bool {
	if err == nil {
		return false
	}

	switch {
	// The caller canceled the request. This says nothing about the
	// health of Redis.
	case errors.Is(err, context.Canceled):
		return false

	// The deadline passed before Redis answered the call. This logic
	// assumes that the ctx passed into the guarded call (see
	// cb.circuitBreaker.Execute) applies only to that Redis call. A
	// recovered panic skips this function entirely (see cb.New's
	// IsExcluded). A recovered panic always counts as a failure.
	case errors.Is(err, context.DeadlineExceeded):
		return true

	// This is the client lifecycle case: the client itself shut down.
	// This is caller-side state, not a true signal of Redis health, in
	// the same way as the pool errors below. But unlike pool pressure,
	// no call can succeed until the process builds a new client. For
	// this reason, the function classifies this as an infra error for
	// now.
	case errors.Is(err, redis.ErrClosed):
		return true

	// These are server or cluster errors. They report that the server
	// itself is unavailable, overloaded, or mid-failover. They do not
	// report a malformed command.
	//   - ClusterDown / MasterDown / Loading / NoReplicas: the node or
	//     the cluster cannot serve requests right now.
	//   - TryAgain: the cluster asks the client to retry. This usually
	//     happens mid-reshard.
	//   - MaxClients / OOM: the server has no more capacity.
	//   - ReadOnly: a failover likely just happened, and the client's
	//     routing table is stale. The function keeps this case here.
	//     The same "this is only cluster protocol" reasoning that
	//     excludes Moved and Ask below applies partially here too. You
	//     decide whether to change this for full consistency.
	//   - Auth: a client that cannot authenticate cannot do anything
	//     else either. For this reason, the function classifies it as
	//     an infra error.
	//
	// The function does NOT classify Moved or Ask as infra errors. These
	// are normal Redis Cluster routing protocol, not signs of bad
	// health. A ClusterClient absorbs them during normal operation, such
	// as slot migration or resharding: it updates its slot table and
	// retries. If one of these errors reaches the caller, the likely
	// cause is a MaxRedirects budget hit during a routine reshard.
	// Another likely cause is a plain, non-cluster-aware Client pointed
	// at a cluster node. Both causes are configuration mismatches. They
	// do not mean that the cluster is down, the way ClusterDown and
	// MasterDown explicitly do.
	//
	// The function does NOT classify redis.ErrPoolExhausted or
	// redis.ErrPoolTimeout as infra errors. These errors reflect this
	// client's configured capacity (MaxActiveConns, PoolSize,
	// PoolTimeout) against the current demand on this process. They do
	// not reflect the health of Redis. ErrPoolExhausted fires only when
	// the client configures an explicit MaxActiveConns ceiling and hits
	// it. This is a pure client-side capacity limit (see
	// internal/pool/pool.go: newConn). ErrPoolTimeout is more ambiguous.
	// It can also happen because Redis is slow. When Redis is slow,
	// connections stay checked out longer than usual. But tripping the
	// breaker does not fix either cause. Genuine Redis-side slowness
	// still triggers context.DeadlineExceeded or a network error below,
	// once a connection reaches Redis. If you size PoolSize generously
	// against real concurrency, you can add ErrPoolTimeout back to the
	// infra bucket. That choice depends on your own pool tuning. This
	// function cannot know that choice on its own.
	case redis.IsClusterDownError(err),
		redis.IsTryAgainError(err),
		redis.IsMasterDownError(err),
		redis.IsLoadingError(err),
		redis.IsMaxClientsError(err),
		redis.IsOOMError(err),
		redis.IsNoReplicasError(err),
		redis.IsReadOnlyError(err),
		redis.IsAuthError(err):
		return true
	}

	// These are transport-level failures: refused or reset connections,
	// DNS failures, dial timeouts, or a connection that dies mid-read or
	// mid-write. net.Error covers *net.OpError and *net.DNSError. This
	// is how go-redis reports almost all dial, read, and write failures.
	if _, ok := errors.AsType[net.Error](err); ok {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	// Everything else is a per-command or business signal, not an infra
	// error: redis.Nil, redis.TxFailedErr, IsPermissionError/NOPERM,
	// IsExecAbortError, redis.ErrCrossSlot, redis.ErrNoScript,
	// WRONGTYPE, syntax errors, Moved/Ask (see above), and anything
	// unrecognized. A recovered panic skips this function entirely (see
	// cb.New's IsExcluded). A recovered panic always counts as a
	// failure.
	return false
}

// BatchAddDevices registers multiple devices as owned by this hub. The
// function groups devices by bucket and updates them through the
// matching Lua script. This keeps the device registry and this hub's
// device set consistent. The function rejects inputs above
// presence.MaxBatchSize before it sends any Redis command.
func (c *Cache) BatchAddDevices(
	ctx context.Context,
	deviceIDs []device.ID,
) (map[device.ID][]message.Date, error) {
	if len(deviceIDs) == 0 {
		return nil, nil
	}

	if len(deviceIDs) > presence.MaxBatchSize {
		return nil, ErrBatchTooLarge
	}

	var (
		hubDeviceKey = bs.Concat(hubDevicesPrefix, c.hubID)
		bHubID       = bs.U64ToBytes(c.hubID)
		byBucket     = make(map[uint32][]int) // byBucket maps a bucket to the indices into deviceIDs.
		order        = make([]uint32, 0)
		flattened    = make([]device.ID, 0, len(deviceIDs)) // The order of devices in flattened matches the bucket pattern in ARGV.
	)

	for i, deviceID := range deviceIDs {
		b := c.bucketFor(deviceID.Uint64())
		if _, ok := byBucket[b]; !ok {
			order = append(order, b)
		}
		byBucket[b] = append(byBucket[b], i)
	}

	var (
		i       = 1
		keyBufs = make([][]byte, 1+len(order)+len(deviceIDs)) // +len(deviceIDs) for pending_buckets keys
		keys    = make([]string, 1+len(order)+len(deviceIDs)) // +len(deviceIDs) for pending_buckets keys
		args    = make([]any, 2+len(order)+len(deviceIDs))
	)

	keys[0] = bs.BytesToString(hubDeviceKey)
	keyBufs[0] = hubDeviceKey
	for _, b := range order {
		keyBufs[i] = bs.Concat(devicesPrefix, uint64(b))
		keys[i] = bs.BytesToString(keyBufs[i])
		i += 1
	}

	args[0] = bHubID
	args[1] = len(order)
	i = 2
	pendingKeyIdx := 1 + len(order) // pendingKeyIdx starts right after the devices:{bucket} keys.

	for _, b := range order {
		idxs := byBucket[b]
		args[i] = len(idxs)
		i += 1
		for _, idx := range idxs {
			deviceID := deviceIDs[idx]

			args[i] = bs.U64ToBytes(deviceID.Uint64())
			i += 1

			keyBufs[pendingKeyIdx] = bs.Concat(pendingBucketsPrefix, deviceID.Uint64())
			keys[pendingKeyIdx] = bs.BytesToString(keyBufs[pendingKeyIdx])
			pendingKeyIdx += 1

			flattened = append(flattened, deviceID)
		}
	}

	resp, err := batchAddDevicesScript.Run(ctx, c.rdb, keys, args...).Slice()

	// Release the pooled buffers.
	slicepool.Put(bHubID)
	for _, key := range keyBufs {
		slicepool.Put(key)
	}
	// Every string in keys views storage that now belongs to the pool.
	keys = nil
	for i = 2; i < len(args); i += 1 {
		switch arg := args[i].(type) {
		case []byte:
			slicepool.Put(arg)
		}
	}

	if err != nil {
		return nil, err
	}

	pendingRaw, ok := resp[1].([]any)
	if !ok {
		return nil, fmt.Errorf("batchAddDevices: unexpected pending format: %T", resp[1])
	}
	if len(pendingRaw) != len(flattened) {
		return nil, fmt.Errorf("batchAddDevices: pending count mismatch: got %d, want %d", len(pendingRaw), len(flattened))
	}

	pending := make(map[device.ID][]message.Date, len(pendingRaw))
	for idx, p := range pendingRaw {
		// The code uses flattened[idx] instead of the deviceID the
		// script returns. Lua returns items in the same order as the Go
		// flatten order, so the two match.
		deviceID := flattened[idx]
		datesRaw, ok := p.([]any)
		if !ok {
			return nil, fmt.Errorf("batchAddDevices: unexpected pending days format: %T", p)
		}

		dates := make([]message.Date, 0, len(datesRaw))
		for _, date := range datesRaw {
			str, ok := date.(string)
			if !ok {
				c.logger.Error("batchAddDevices: unexpected date type", zap.Any("date", str))
				continue
			}

			v, parseErr := strconv.ParseUint(str, 10, 32)
			if parseErr != nil {
				c.logger.Error("batchAddDevices: invalid date", zap.String("date", str), zap.Error(err))
				continue
			}
			dates = append(dates, message.Date(v))
		}

		if len(dates) > 0 {
			pending[deviceID] = dates
		}
	}

	return pending, nil
}

// BatchDelDevices deletes deviceIDs from the registry, but only the
// devices still owned by this hub. A different hub may already have
// re-claimed a device through batchAddDevices in the meantime. In that
// case, the function leaves the device alone and reports no error. This
// matches the old "not owned, so no-op" behavior of the single-device
// DelDevice. The return value does not distinguish "all devices deleted"
// from "some devices were already owned elsewhere", in the same way that
// batchAddDevices also discards its Lua script's changed count.
//
// One Lua call handles every bucket present in deviceIDs. The name does
// not split the call by bucket. See batchDelDeviceScript for the reason
// the PUBLISH payload is precomputed here, instead of built in Lua. The
// function rejects inputs above presence.MaxBatchSize before it sends
// any Redis command.
func (c *Cache) BatchDelDevices(
	ctx context.Context,
	deviceIDs []device.ID,
) error {
	if len(deviceIDs) == 0 {
		return nil
	}
	if len(deviceIDs) > presence.MaxBatchSize {
		return ErrBatchTooLarge
	}

	var (
		hubDeviceKey = bs.Concat(hubDevicesPrefix, c.hubID)
		bHubID       = bs.U64ToBytes(c.hubID)
		byBucket     = make(map[uint32][]int) // byBucket maps a bucket to the indices into deviceIDs.
		order        = make([]uint32, 0)
	)

	for i, deviceID := range deviceIDs {
		b := c.bucketFor(deviceID.Uint64())
		if _, ok := byBucket[b]; !ok {
			order = append(order, b)
		}
		byBucket[b] = append(byBucket[b], i)
	}

	var (
		i       = 2
		keyBufs = make([][]byte, 2+len(order))
		keys    = make([]string, 2+len(order))
		args    = make([]any, 2+len(order)+2*len(deviceIDs))
	)

	// keyBufs[0] stays nil, because keys[0] is a string constant, not
	// pool storage. Put ignores a nil slice. So the cleanup loop can run
	// over the whole slice. It needs no start index that you must keep
	// in sync by hand.
	keys[0] = eventDelDevice
	keys[1] = bs.BytesToString(hubDeviceKey)
	keyBufs[1] = hubDeviceKey
	for _, b := range order {
		keyBufs[i] = bs.Concat(devicesPrefix, uint64(b))
		keys[i] = bs.BytesToString(keyBufs[i])
		i += 1
	}

	// The code precomputes every device's PUBLISH payload before the
	// Lua call. See the comment on batchDelDeviceScript for the reason:
	// it is not safe to build the payload in Lua. deviceID can exceed
	// 2^53, where Lua's float-backed numbers start to lose integer
	// precision.
	payloads := make([][]byte, len(deviceIDs))
	for idx, id := range deviceIDs {
		buf := slicepool.Get(16)
		binary.LittleEndian.PutUint64(buf[:8], id.Uint64())
		binary.LittleEndian.PutUint64(buf[8:], c.hubID)
		payloads[idx] = buf
	}

	args[0] = bHubID
	args[1] = len(order)
	i = 2
	for _, b := range order {
		idxs := byBucket[b]
		args[i] = len(idxs)
		i += 1
		for _, idx := range idxs {
			args[i] = bs.U64ToBytes(deviceIDs[idx].Uint64())
			args[i+1] = payloads[idx]
			i += 2
		}
	}

	err := batchDelDeviceScript.Run(ctx, c.rdb, keys, args...).Err()

	// Release the pooled buffers.
	slicepool.Put(bHubID)
	for _, key := range keyBufs {
		slicepool.Put(key)
	}
	// Every string in keys views storage that the pool now owns.
	keys = nil
	// Every deviceID-bytes slot and every payload slot in args is a
	// []byte from slicepool. Every count slot is a plain int. A type
	// switch over every index frees both kinds correctly, with no
	// double free and no leak. The code does not assume a fixed
	// step-by-2 position, because the count slot for bucket 2 and later
	// does not sit at a fixed parity relative to the deviceID/payload
	// pairs. Each bucket's own count slot shifts everything after it by
	// one position whenever that bucket holds an odd number of devices.
	// This mirrors the cleanup in BatchAddDevices above, for the same
	// reason.
	for idx := 2; idx < len(args); idx += 1 {
		switch arg := args[idx].(type) {
		case []byte:
			slicepool.Put(arg)
		}
	}

	return err
}

// Start subscribes to events, registers the hub lease, and starts event
// handling. Only one start attempt can run at a time. A successful Start
// call cannot run again.
func (c *Cache) Start(rootCtx context.Context) error {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	if c.closed {
		return ErrCacheClosed
	}

	if !c.starting.CompareAndSwap(false, true) {
		return ErrCacheStarted
	}

	// The code tracks every PubSub connection that it opens below. This
	// lets it close each connection on any failure path. Without this
	// list, a later step that fails would leak the open connections.
	var opened []*redis.PubSub
	started := false
	defer func() {
		if !started {
			for _, sub := range opened {
				_ = sub.Close()
			}
			// This allows a retry after a failed start attempt.
			c.starting.Store(false)
		}
	}()

	if err := c.rdb.Ping(rootCtx).Err(); err != nil {
		closeErr := c.rdb.Close()
		if closeErr != nil {
			err = errors.Join(err, closeErr)
		}
		return err
	}

	delHubSubscriber, err := c.subscribe(rootCtx, eventDelHub)
	if err == nil {
		opened = append(opened, delHubSubscriber)
	} else {
		return err
	}

	delDeviceSubscriber, err := c.subscribe(rootCtx, eventDelDevice)
	if err == nil {
		opened = append(opened, delDeviceSubscriber)
	} else {
		return err
	}

	if c.verifyExpiredNotifications {
		// If you host Redis yourself, set `notify-keyspace-events Ex`
		// in the config file. If not, you must configure it through
		// the management interface or the API of the Redis provider.
		if err = c.verifyTurnOnExpiredNotifications(rootCtx); err != nil {
			return err
		}
	}

	expiredSubscriber, err := c.subscribe(rootCtx, fmt.Sprintf("__keyevent@%d__:expired", c.dbIndex))
	if err == nil {
		opened = append(opened, expiredSubscriber)
	} else {
		return err
	}

	// The code publishes the heartbeat before it registers the hub in
	// "hubs", not after. If addHub below fails, the result is an
	// orphaned heartbeat key with no entry in "hubs". This is harmless,
	// because the key expires on its own through its TTL. The reverse
	// order carries a risk: if Set fails after addHub, a "hubs" entry is
	// left with no lease behind it. This state never heals itself,
	// because a key that the code never created can never emit an
	// expired event for onExpiredKeyEvent to react to.
	hubHeartbeatKey := bs.Concat(hubHeartbeatPrefix, c.hubID)
	defer slicepool.Put(hubHeartbeatKey)

	err = c.rdb.Set(rootCtx, bs.BytesToString(hubHeartbeatKey), 1, c.heartbeatTTL).Err()
	if err != nil {
		return err
	}

	if err = c.addHub(rootCtx); err != nil {
		return err
	}

	c.subscribers = subscribers{
		delHub:    delHubSubscriber,
		delDevice: delDeviceSubscriber,
		expired:   expiredSubscriber,
	}

	go c.handleEvents(rootCtx)
	c.running.Store(true)
	started = true

	return err
}

// Close stops the subscriptions and the event handling, then closes the
// Redis client. Close waits for an in-flight Start call. Close is safe to
// call again and again, and safe to call before Start.
func (c *Cache) Close() error {
	c.lifecycleMu.Lock()
	c.closed = true
	c.lifecycleMu.Unlock()

	c.closeOnce.Do(func() {
		if c.subscribers.delHub != nil {
			if err := c.subscribers.delHub.Close(); err != nil {
				c.logger.Error("failed to close redis delete hub subscriber", zap.Error(err))
			}
		}
		if c.subscribers.delDevice != nil {
			if err := c.subscribers.delDevice.Close(); err != nil {
				c.logger.Error("failed to close redis delete device subscriber", zap.Error(err))
			}
		}
		if c.subscribers.expired != nil {
			if err := c.subscribers.expired.Close(); err != nil {
				c.logger.Error("failed to close redis expired subscriber", zap.Error(err))
			}
		}

		if c.running.Load() {
			<-c.done
		}

		c.closeErr = c.rdb.Close()
	})
	return c.closeErr
}

// SetCallbacks installs optional event callbacks one time, before Start.
// A nil callback acts as a no-op. SetCallbacks returns an error if the
// cache already has callbacks, already started, or already closed.
func (c *Cache) SetCallbacks(cb Callbacks) error {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	if c.closed {
		return ErrCacheClosed
	}
	if c.starting.Load() {
		return ErrCallbacksAfterStart
	}
	if c.callbacksSet {
		return ErrCallbacksAlreadySet
	}
	if cb.OnHubDeleted == nil {
		cb.OnHubDeleted = func(uint64) {}
	}
	if cb.OnDeviceDeleted == nil {
		cb.OnDeviceDeleted = func(uint64, device.ID) {}
	}
	if cb.OnSelfReaped == nil {
		cb.OnSelfReaped = func() {}
	}
	c.callbacks = cb
	c.callbacksSet = true
	return nil
}

// newDialer returns a function. The function serves as the default
// dialer when Options.Dialer specifies none.
func newDialer(cfg *Config) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		netDialer := &net.Dialer{
			Timeout: cfg.DialTimeout,
			KeepAliveConfig: net.KeepAliveConfig{
				Idle:     cfg.DialIdle,
				Interval: cfg.DialInterval,
				Count:    cfg.DialKeepCount,
			},
		}
		if cfg.TLSConfig == nil {
			return netDialer.DialContext(ctx, network, addr)
		}
		return tls.DialWithDialer(netDialer, network, addr, cfg.TLSConfig)
	}
}

// addHub registers this hub in the global hub registry, with no
// expiration. A separate heartbeat key tracks the liveness of the hub.
func (c *Cache) addHub(ctx context.Context) error {
	bHubID := bs.U64ToBytes(c.hubID)
	defer slicepool.Put(bHubID)
	return c.rdb.SAdd(ctx, hubsKey, bHubID).Err()
}

// reapHub removes a hub from the "hubs" set, announces its departure, and
// drains its device set in bounded batches. This way, a hub that owns
// many devices does not block Redis for too long in one call. For each
// batch, the code runs SRandMemberN on "hub:devices:<hubID>" in Go,
// groups the result by bucket, then deletes each group from the matching
// "devices:{bucket}" hash through one Lua call per bucket group. reapHub
// backs both a graceful shutdown and crash detection, through
// handleEvents. reapHub is safe to call at the same time from multiple
// hub instances: only one instance wins the reap lock and does the work.
// The other instances return right away.
func (c *Cache) reapHub(ctx context.Context, hubID uint64) error {
	var (
		bHubID         = bs.U64ToBytes(hubID)
		reapHubLockKey = bs.Concat(hubReapLockPrefix, hubID)
	)

	acquired, err := c.rdb.SetNX(
		ctx,
		bs.BytesToString(reapHubLockKey),
		1,
		reapLockTTL,
	).Result()
	if err != nil {
		slicepool.Put(bHubID)
		slicepool.Put(reapHubLockKey)
		return err
	}
	if !acquired {
		slicepool.Put(bHubID)
		slicepool.Put(reapHubLockKey)
		// Another hub instance is already reaping this hub.
		return nil
	}

	defer func() {
		delErr := c.rdb.Del(
			ctx,
			bs.BytesToString(reapHubLockKey),
		).Err()
		if delErr != nil {
			c.logger.Error(
				"failed to release reap lock",
				zap.Uint64("hubID", hubID),
				zap.Error(delErr),
			)
		}
		slicepool.Put(bHubID)
		slicepool.Put(reapHubLockKey)
	}()

	if err = deleteHubScript.Run(
		ctx,
		c.rdb,
		[]string{hubsKey, eventDelHub},
		bHubID,
		bHubID,
	).Err(); err != nil {
		return err
	}

	hubDeviceKey := bs.Concat(hubDevicesPrefix, hubID)
	for {
		// SRANDMEMBER reads a member without deleting it. Deletion from
		// both the hub set and the devices hash happens only after the
		// matching HDEL finishes in Lua. If a crash or an error happens
		// halfway through, the member stays intact in
		// hub:devices:<hubID>, so the next reap picks it up again.
		popped, popErr := c.rdb.SRandMemberN(
			ctx,
			bs.BytesToString(hubDeviceKey),
			int64(c.reapBatchSize),
		).Result()
		if popErr != nil {
			slicepool.Put(hubDeviceKey)
			return popErr
		}
		if len(popped) == 0 {
			break
		}

		byBucket := make(map[uint32][]string)
		for _, deviceIDStr := range popped {
			id, parseErr := strconv.ParseUint(deviceIDStr, 10, 64)
			if parseErr != nil {
				// The code does not expect this case. It skips a
				// malformed member instead of failing the whole reap
				// over it.
				continue
			}
			bucket := uint32(c.bucketFor(id))
			byBucket[bucket] = append(byBucket[bucket], deviceIDStr)
		}

		for bucket, ids := range byBucket {
			deviceBucketKey := bs.Concat(devicesPrefix, uint64(bucket))

			args := make([]any, 1+len(ids))
			args[0] = bHubID
			for idx, strID := range ids {
				args[idx+1] = strID
			}

			runErr := reapDevicesInBucketScript.Run(
				ctx,
				c.rdb,
				[]string{
					bs.BytesToString(deviceBucketKey),
					bs.BytesToString(hubDeviceKey),
				},
				args...,
			).Err()

			slicepool.Put(deviceBucketKey)
			if runErr != nil {
				slicepool.Put(hubDeviceKey)
				return runErr
			}
		}

		if uint16(len(popped)) < c.reapBatchSize {
			// The batch holds fewer members than requested. The set is
			// empty or almost empty.
			break
		}
	}

	hubHeartbeatKey := bs.Concat(hubHeartbeatPrefix, hubID)
	err = c.rdb.Del(
		ctx,
		bs.BytesToString(hubHeartbeatKey),
	).Err()

	slicepool.Put(hubDeviceKey)
	slicepool.Put(hubHeartbeatKey)

	return err
}

// checkSelfReaped checks that this hub is still present in "hubs". It
// runs this check after a gap long enough for another instance to have
// reaped this hub as crashed. If the hub entry is gone, checkSelfReaped
// re-registers it. If re-registration fails, checkSelfReaped logs the
// error and continues; it does not return early, because the application
// still needs the OnSelfReaped callback below. checkSelfReaped always
// calls OnSelfReaped, whether or not re-registration succeeded. The
// application still needs to know about the gap: the application may
// believe that it owns TCP connections or devices that were reassigned
// to a live hub while this hub was partitioned. The application must
// reconcile that state itself, for example by force-disconnecting local
// clients so they reconnect and get reassigned correctly.
// checkSelfReaped does not perform that reconciliation.
func (c *Cache) checkSelfReaped(ctx context.Context) {
	if !c.selfReapChecking.CompareAndSwap(false, true) {
		return // Another check is already running. This call skips itself.
	}
	defer c.selfReapChecking.Store(false)

	bHubID := bs.U64ToBytes(c.hubID)
	exists, err := c.rdb.SIsMember(
		ctx,
		hubsKey,
		bHubID,
	).Result()

	slicepool.Put(bHubID)

	if err != nil {
		c.logger.Warn("failed to check self reaped", zap.Error(err))
		return
	}
	if exists {
		return
	}

	if err = c.addHub(ctx); err != nil {
		// This is a best-effort call. The code logs the error and
		// continues. It does not return early, because the application
		// still needs the OnSelfReaped callback below.
		c.logger.Warn("failed to re-register hub after a possible self-reap", zap.Error(err))
	}

	c.callbacks.OnSelfReaped()
}

// verifyTurnOnExpiredNotifications checks that Redis emits keyevent
// notifications for expired keys. handleEvents relies on these
// notifications to detect a crashed hub's expired heartbeat lease.
func (c *Cache) verifyTurnOnExpiredNotifications(ctx context.Context) error {
	verify, err := c.rdb.ConfigGet(ctx, "notify-keyspace-events").Result()
	if err != nil {
		return err
	}

	got := verify["notify-keyspace-events"]
	if !strings.Contains(got, "E") ||
		(!strings.Contains(got, "A") &&
			!strings.Contains(got, "x")) {
		return fmt.Errorf("notify-keyspace-events did not take effect: got %q", got)
	}
	return nil
}

// handleEvents runs the background loop of the cache. It refreshes the
// hub heartbeat. It also processes hub deletion events, device deletion
// events, and heartbeat-expiration events. It runs until the context
// cancels or a subscription closes.
func (c *Cache) handleEvents(ctx context.Context) {
	var (
		ticker          = time.NewTicker(c.heartbeatInterval)
		hubHeartbeatKey = bs.Concat(hubHeartbeatPrefix, c.hubID)

		// go-redis gives no explicit "reconnected" event. The pool
		// retries and reconnects on its own, without exposing that to
		// the caller. The code only observes a pass or fail result from
		// each Set call. For this reason, the code infers a partition
		// by tracking the time since the last confirmed heartbeat
		// write.
		lastSuccess = time.Now()
	)

	defer ticker.Stop()
	defer slicepool.Put(hubHeartbeatKey)
	defer close(c.done)

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			err := c.rdb.Set(
				ctx,
				bs.BytesToString(hubHeartbeatKey),
				1,
				c.heartbeatTTL,
			).Err()

			if err != nil {
				// You can connect c.logger here for visibility into
				// repeated failures. The code does not touch
				// lastSuccess here on purpose. The gap keeps growing
				// across ticks until a write succeeds.
				continue
			}

			// The write just moved from "not confirmed" to "confirmed".
			// If the gap since the last confirmed write is at least the
			// TTL, Redis may have expired this hub's lease in the
			// meantime. Another hub's crash detector relies on the
			// same condition to reap this hub. The code verifies this
			// before it assumes the worst.
			if time.Since(lastSuccess) >= c.heartbeatTTL {
				_ = c.pool.Submit(func(_ *workerpool.Context) {
					c.checkSelfReaped(ctx)
				})
			}
			lastSuccess = time.Now()

		case delDeviceMsg := <-c.subscribers.delDevice.Channel():
			if delDeviceMsg == nil {
				return
			}

			if len(delDeviceMsg.Payload) != 16 {
				c.logger.Warn("delDeviceMsg.Payload is not 16 bytes", zap.String("payload", delDeviceMsg.Payload))
				continue
			}

			buf := bs.StringToBytes(delDeviceMsg.Payload)
			hubID := binary.LittleEndian.Uint64(buf[8:16])
			if hubID != c.hubID {
				c.callbacks.OnDeviceDeleted(
					hubID,
					device.ID(binary.LittleEndian.Uint64(buf[:8])),
				)
			}

		case delHubMsg := <-c.subscribers.delHub.Channel():
			if delHubMsg == nil {
				return
			}

			hubID, err := strconv.ParseUint(delHubMsg.Payload, 10, 64)
			if err != nil {
				c.logger.Warn("failed to parse hub ID", zap.String("payload", delHubMsg.Payload), zap.Error(err))
			} else if hubID != c.hubID {
				c.callbacks.OnHubDeleted(hubID)
			}

		case expiredMsg := <-c.subscribers.expired.Channel():
			if expiredMsg == nil {
				return
			}

			hubID, ok := parseHeartbeatKey(expiredMsg.Payload)
			if !ok || hubID == c.hubID {
				continue
			}

			_ = c.pool.Submit(func(_ *workerpool.Context) {
				err := c.reapHub(ctx, hubID)
				if err != nil {
					c.logger.Warn("failed to reap hub", zap.Uint64("hubID", hubID), zap.Error(err))
				}
			})
		}
	}
}

// bucketFor returns the registry bucket for the given device ID.
func (c *Cache) bucketFor(id uint64) uint32 {
	return uint32(bit.Mix64(id) % uint64(c.deviceBucketCount))
}

// subscribe creates a Redis Pub/Sub subscription and waits until Redis
// acknowledges the subscription before returning it.
func (c *Cache) subscribe(ctx context.Context, channel string) (*redis.PubSub, error) {
	sub := c.rdb.Subscribe(ctx, channel)
	if _, err := sub.Receive(ctx); err != nil {
		_ = sub.Close()
		return nil, err
	}
	return sub, nil
}
