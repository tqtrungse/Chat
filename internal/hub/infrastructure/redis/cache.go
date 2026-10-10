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

package redis

import (
	"context"
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

	"xxx/internal/hub/connection"
	"xxx/internal/hub/domain/message"
	shareddevice "xxx/internal/shared/device"

	"xxx/pkg"
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

	// devicesPrefix + bucket index is the key of one shard of the device
	// registry, e.g. "devices:0", "devices:1", ... "devices:{N-1}". Splitting
	// the single global "devices" hash into many bucket hashes keeps each
	// one comfortably under Redis's hash-max-listpack-entries threshold, so
	// Redis keeps using its compact listpack encoding (far less per-field
	// overhead) instead of falling back to the much heavier hashtable
	// encoding once a hash grows past that threshold. See DeviceBucketCount
	// in Config for how to size N.
	devicesPrefix = "devices:"

	pendingBucketsPrefix = "pending_buckets:"

	defaultDeviceBucketCount = uint16(4096)
	defaultReapBatchSize     = uint16(500)
	defaultHeartbeatInterval = 3 * time.Second
	defaultHeartbeatTTL      = 10 * time.Second
	reapLockTTL              = 30 * time.Second
)

type Callbacks struct {
	OnHubDeleted    func(hubID uint64)
	OnDeviceDeleted func(hubID uint64, deviceID shareddevice.ID)

	// OnSelfReaped is called if this hub detects it may have been reaped by
	// another instance while disconnected from cache service for longer than
	// HeartbeatTTL (e.g. during a network partition, while its own TCP
	// clients were still alive). The cache layer only detects and reports
	// this; deciding what to do — force-disconnect all local TCP clients so
	// they reconnect to a live hub, exit the process, or re-register from
	// scratch — is an application-level decision.
	OnSelfReaped func()
}

type subscribers struct {
	delHub    *redis.PubSub
	delDevice *redis.PubSub
	expired   *redis.PubSub
}

type cache struct {
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
	setCbOnce                  atomic.Bool
	verifyExpiredNotifications bool
}

// NewCache creates a Redis-backed cache for the specified hub and applies
// default values for optional timing, reaping, and device-bucket settings.
// The returned cache is not started until startFn is called.
//
// startFn initializes the Redis connection, subscribes to required Pub/Sub
// channels, registers the hub, and starts the background event handler.
//
// startFn is safe to call concurrently and more than once: only one call can
// be in flight or have succeeded at a time. If a call fails, the guard is
// released so a later retry can still succeed; once a call succeeds,
// further calls return an error instead of leaking subscribers or starting
// a second background event handler.
//
// closeFn closes Redis Pub/Sub subscriptions, waits for the background event
// handler to exit, and then closes the Redis client.
//
// Safe to call even if start failed or was never called — it only waits on
// c.done if the background event handler was actually launched, so it
// never hangs waiting for a signal that would never come. Safe to call more
// than once, concurrently or not; only the first call does anything.
func NewCache(
	hubID uint64,
	cfg Config,
	logger *log.Logger,
	pool *workerpool.Pool,
) (
	c connection.DistributedCache,
	startFn func(rootCtx context.Context) error,
	closeFn func() error,
	setCbFn func(callbacks Callbacks) bool,
) {
	rdb := redis.NewClient(&redis.Options{
		Addr:           cfg.Addr,
		Password:       cfg.Password,
		DB:             int(cfg.DB),
		PoolSize:       int(cfg.PoolSize),
		MaxIdleConns:   int(cfg.MaxIdleConns),
		MaxActiveConns: int(cfg.MaxActiveConns),
	})

	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = defaultHeartbeatInterval
	}

	if cfg.HeartbeatTTL <= 0 {
		cfg.HeartbeatTTL = defaultHeartbeatTTL
	}

	if cfg.ReapBatchSize <= 0 {
		cfg.ReapBatchSize = defaultReapBatchSize
	}

	if cfg.DeviceBucketCount == 0 {
		cfg.DeviceBucketCount = defaultDeviceBucketCount
	}

	cc := &cache{
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
	}
	startFn = cc.start
	closeFn = cc.close
	setCbFn = cc.setCallback
	return cc, startFn, closeFn, setCbFn
}

// ReapHub removes the specified hub and reaps the devices still associated
// with it. The distributed reap lock ensures that concurrent cache instances
// do not reap the same hub at the same time.
func (c *cache) ReapHub(ctx context.Context) error {
	return c.reapHub(ctx, c.hubID)
}

// ListHubsByDevices returns the hub ID currently owning each device ID.
// The returned slice preserves the same order as deviceIDs; a zero value
// indicates that a device has no registered hub.
func (c *cache) ListHubsByDevices(
	ctx context.Context,
	deviceIDs []shareddevice.ID,
) ([]uint64, error) {
	if len(deviceIDs) == 0 {
		return []uint64{}, nil
	}

	// bucket -> indices into deviceIDs/result that fall in that bucket.
	byBucket := make(map[uint32][]int)
	for i, deviceID := range deviceIDs {
		bucket := c.bucketFor(deviceID.Uint64())
		byBucket[bucket] = append(byBucket[bucket], i)
	}

	var (
		pipe      = c.rdb.Pipeline()
		cmds      = make(map[uint32]*redis.SliceCmd, len(byBucket))
		arrKeys   = make([][]byte, 0, len(byBucket))
		arrFields = make([][]string, 0, len(byBucket))
	)

	for bucket, idxs := range byBucket {
		fields := make([]string, len(idxs))
		for j, idx := range idxs {
			fields[j] = pkg.BytesToString(pkg.U64ToBytes(deviceIDs[idx].Uint64()))
		}
		bucketKey := pkg.Concat(devicesPrefix, uint64(bucket))
		arrKeys = append(arrKeys, bucketKey)
		arrFields = append(arrFields, fields)
		cmds[bucket] = pipe.HMGet(ctx, pkg.BytesToString(bucketKey), fields...)
	}

	_, err := pipe.Exec(ctx)

	// cleanup.
	for _, key := range arrKeys {
		slicepool.Put(key)
	}
	for _, fields := range arrFields {
		for _, field := range fields {
			slicepool.Put(pkg.StringToBytes(field))
		}
	}
	arrKeys = nil
	arrFields = nil

	if err != nil {
		return nil, err
	}

	result := make([]uint64, len(deviceIDs))
	for bucket, idxs := range byBucket {
		var values []any
		values, err = cmds[bucket].Result()
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

// IsInfraError reports whether err indicates a problem with
// the Redis connection, node, or cluster itself — the kind of failure
// RedisBreaker should trip on — as opposed to a problem with a particular
// command (bad arguments, wrong type, no permission, key not found, and
// so on), which will keep failing regardless of whether Redis is healthy
// and shouldn't cost every other caller a fail-fast.
//
// Only exported github.com/redis/go-redis/v9 API is used here — the typed
// Is*Error helpers and sentinel vars — so this keeps working across
// go-redis versions without reaching into its internal packages.
func (c *cache) IsInfraError(err error) bool {
	if err == nil {
		return false
	}

	switch {
	// The caller gave up; that says nothing about Redis's health.
	case errors.Is(err, context.Canceled):
		return false

	// The deadline reached all the way down to the Redis call without an
	// answer. This assumes the ctx passed into the guarded call (see
	// RedisBreaker.Execute in redis_breaker.go) is scoped specifically to
	// that Redis call — if so, this means Redis or the network to it was
	// too slow, not that some unrelated caller-side budget ran out.
	case errors.Is(err, context.DeadlineExceeded):
		return true

	// Client lifecycle: the client itself was shut down. Arguably this is
	// caller-side state rather than a Redis health signal (same tension
	// as the pool errors below), but unlike pool pressure it means
	// literally nothing can succeed until the process reconstructs a
	// client, so it stays here for now.
	case errors.Is(err, redis.ErrClosed):
		return true

	// Server/cluster-reported errors describing the server itself as
	// unavailable, overloaded, or mid-failover — not this command being
	// malformed:
	//   - ClusterDown / MasterDown / Loading / NoReplicas: the
	//     node/cluster isn't in a state to serve requests right now.
	//   - TryAgain: the cluster asked us to retry (typically mid-reshard).
	//   - MaxClients / OOM: the server is out of capacity.
	//   - ReadOnly: usually means a failover just happened and the
	//     client's routing table is stale. Kept here, though the same
	//     "it's just cluster protocol" reasoning that excludes Moved/Ask
	//     below applies partially to this too — worth your own call if
	//     you want full consistency.
	//   - Auth: a client that can't authenticate can't do anything else
	//     either — but see IsRedisAuthError / WithOnAuthError in
	//     redis_breaker.go for why this deserves its own alert too, not
	//     just silent inclusion here.
	//
	// Deliberately NOT here: Moved / Ask. These are normal Redis Cluster
	// routing protocol, not unhealthiness — a ClusterClient absorbs them
	// transparently under ordinary operation (slot migration, resharding)
	// by updating its slot table and retrying. If one leaks out to the
	// caller, it's more likely a MaxRedirects budget hit during a routine
	// reshard, or a plain (non-cluster-aware) Client pointed at a cluster
	// node — a configuration mismatch, not "the cluster is down" the way
	// ClusterDown/MasterDown explicitly are.
	//
	// Deliberately NOT infra: redis.ErrPoolExhausted / redis.ErrPoolTimeout
	// reflect this client's configured capacity (MaxActiveConns / PoolSize
	// / PoolTimeout) versus current concurrent demand on THIS process, not
	// Redis's health. ErrPoolExhausted specifically only fires when an
	// explicit MaxActiveConns ceiling is configured and hit — a pure
	// client-side capacity limit (see internal/pool/pool.go: newConn).
	// ErrPoolTimeout is genuinely more ambiguous — it can also happen
	// because Redis is slow enough that connections are held checked-out
	// longer than usual — but tripping the breaker doesn't fix either
	// cause, and genuine Redis-side slowness is still caught below via
	// context.DeadlineExceeded and network errors once a connection
	// actually reaches Redis. If your PoolSize is sized generously
	// relative to real concurrency, you may want ErrPoolTimeout back in
	// the infra bucket — that's a call about your own pool tuning, not
	// something this function can know on its own.
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

	// Transport-level failures — refused/reset connections, DNS
	// failures, dial timeouts, or the connection dying mid-read/write.
	// net.Error covers *net.OpError and *net.DNSError, which is how
	// go-redis surfaces essentially all dial/read/write failures.
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	// Everything else — redis.Nil, redis.TxFailedErr,
	// IsPermissionError/NOPERM, IsExecAbortError, redis.ErrCrossSlot,
	// redis.ErrNoScript, WRONGTYPE, syntax errors, Moved/Ask (see above),
	// and anything unrecognized — is a per-command or business signal,
	// not infrastructure, and shouldn't trip a breaker meant to detect
	// "Redis is unreachable". A recovered panic bypasses this function
	// entirely (see NewRedisBreaker in redis_breaker.go) and always
	// counts as a failure regardless of what it would return here.
	return false
}

// BatchAddDevices registers multiple devices as owned by this hub.
// Devices are grouped by bucket and updated through the corresponding Lua
// script so the device registry and this hub's device set remain consistent.
func (c *cache) BatchAddDevices(
	ctx context.Context,
	deviceIDs []shareddevice.ID,
) (map[shareddevice.ID][]message.Date, error) {
	if len(deviceIDs) == 0 {
		return nil, nil
	}

	var (
		hubDeviceKey = pkg.Concat(hubDevicesPrefix, c.hubID)
		bHubID       = pkg.U64ToBytes(c.hubID)
		byBucket     = make(map[uint32][]int) // bucket -> indices to deviceIDs
		order        = make([]uint32, 0)
		flattened    = make([]shareddevice.ID, 0, len(deviceIDs)) // The device order after flattening follows the bucket pattern, matching the ARGV.
	)

	for i, deviceID := range deviceIDs {
		b := c.bucketFor(deviceID.Uint64())
		if _, ok := byBucket[b]; !ok {
			order = append(order, b)
		}
		byBucket[b] = append(byBucket[b], i)
	}

	var (
		i    = 1
		keys = make([]string, 1+len(order)+len(deviceIDs)) // +len(deviceIDs) for pending_buckets keys
		args = make([]any, 2+len(order)+len(deviceIDs))
	)

	keys[0] = pkg.BytesToString(hubDeviceKey)
	for _, b := range order {
		keys[i] = pkg.BytesToString(pkg.Concat(devicesPrefix, uint64(b)))
		i += 1
	}

	args[0] = bHubID
	args[1] = len(order)
	i = 2
	pendingKeyIdx := 1 + len(order) // right after block devices:{bucket} keys

	for _, b := range order {
		idxs := byBucket[b]
		args[i] = len(idxs)
		i += 1
		for _, idx := range idxs {
			deviceID := deviceIDs[idx]

			args[i] = pkg.U64ToBytes(deviceID.Uint64())
			i += 1

			keys[pendingKeyIdx] = pkg.BytesToString(pkg.Concat(pendingBucketsPrefix, deviceID.Uint64()))
			pendingKeyIdx += 1

			flattened = append(flattened, deviceID)
		}
	}

	resp, err := batchAddDevicesScript.Run(ctx, c.rdb, keys, args...).Slice()

	// cleanup.
	slicepool.Put(bHubID)
	for _, key := range keys {
		slicepool.Put(pkg.StringToBytes(key))
	}
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

	pending := make(map[shareddevice.ID][]message.Date, len(pendingRaw))
	for idx, p := range pendingRaw {
		// Use flattened[idx] instead of decoding the deviceID the script returns —
		// The order Lua returns ensures a match with the Go flatten order.
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

// BatchDelDevices deletes deviceIDs from the registry, but only those still
// owned by this hub — a device may already have been re-claimed by a
// different hub via batchAddDevices in the meantime, in which case it's
// silently left alone (same "not owned -> no-op" semantics the old
// single-device DelDevice had; the return value doesn't distinguish "all
// deleted" from "some were already owned elsewhere", matching how
// batchAddDevices also discards its Lua script's changed count).
//
// One Lua call per distinct bucket present in deviceIDs, same shape as
// batchAddDevices — see batchDelDeviceScript for why the PUBLISH payload is
// precomputed here rather than built in Lua.
func (c *cache) BatchDelDevices(
	ctx context.Context,
	deviceIDs []shareddevice.ID,
) error {
	if len(deviceIDs) == 0 {
		return nil
	}

	var (
		hubDeviceKey = pkg.Concat(hubDevicesPrefix, c.hubID)
		bHubID       = pkg.U64ToBytes(c.hubID)
		byBucket     = make(map[uint32][]int) // bucket -> indices vào deviceIDs
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
		i    = 2
		keys = make([]string, 2+len(order))
		args = make([]any, 2+len(order)+2*len(deviceIDs))
	)

	keys[0] = eventDelDevice
	keys[1] = pkg.BytesToString(hubDeviceKey)
	for _, b := range order {
		keys[i] = pkg.BytesToString(pkg.Concat(devicesPrefix, uint64(b)))
		i += 1
	}

	// Precompute every device's PUBLISH payload up front — see
	// batchDelDeviceScript's comment for why this can't safely be built in
	// Lua (deviceID can exceed 2^53, where Lua's float-backed numbers start
	// losing integer precision).
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
			args[i] = pkg.U64ToBytes(deviceIDs[idx].Uint64())
			args[i+1] = payloads[idx]
			i += 2
		}
	}

	err := batchDelDeviceScript.Run(ctx, c.rdb, keys, args...).Err()

	// cleanup.
	slicepool.Put(bHubID)
	slicepool.Put(hubDeviceKey)
	for idx := 2; idx < len(keys); idx += 1 {
		slicepool.Put(pkg.StringToBytes(keys[idx]))
	}
	// Every deviceID-bytes AND payload slot in args is a []byte from
	// slicepool; every count slot is a plain int. A type switch over every
	// index — not a step-by-2 position assumption — is what correctly
	// frees both without double-freeing or leaking: the count slot for
	// bucket 2+ does NOT sit at a fixed parity relative to the deviceID/
	// payload pairs, since each bucket's own count slot shifts everything
	// after it by one position whenever that bucket holds an odd number
	// of devices. Mirrors BatchAddDevices's cleanup above for the same
	// reason.
	for idx := 2; idx < len(args); idx += 1 {
		switch arg := args[idx].(type) {
		case []byte:
			slicepool.Put(arg)
		}
	}

	return err
}

func (c *cache) start(rootCtx context.Context) error {
	if !c.starting.CompareAndSwap(false, true) {
		return errors.New("cache: start already called")
	}

	// Track every PubSub connection opened below so we can close them on
	// any failure path instead of leaking them if a later step errors out.
	var opened []*redis.PubSub
	started := false
	defer func() {
		if !started {
			for _, sub := range opened {
				_ = sub.Close()
			}
			// Allow a retry after a failed start attempt.
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
		// If we host Redis, just set `notify-keyspace-events Ex` in config file.
		// Otherwise, we must configure it through the management interface or the Redis provider's API.
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

	// Publish the heartbeat before registering in "hubs", not after: if
	// addHub below fails, an orphaned heartbeat key with no "hubs" entry is
	// harmless and self-expires via TTL. The reverse order can leave a
	// "hubs" entry with no lease behind it at all if the Set fails after —
	// which never self-heals, since a key that was never created can never
	// emit an expired event for onExpiredKeyEvent to react to.
	hubHeartbeatKey := pkg.Concat(hubHeartbeatPrefix, c.hubID)
	defer slicepool.Put(hubHeartbeatKey)

	err = c.rdb.Set(rootCtx, pkg.BytesToString(hubHeartbeatKey), 1, c.heartbeatTTL).Err()
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

func (c *cache) close() error {
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

func (c *cache) setCallback(cb Callbacks) bool {
	if c.setCbOnce.Swap(true) {
		return true
	}
	c.callbacks = cb
	return false
}

// addHub registers this hub in the global hub registry with no expiration.
// The hub's liveness is tracked separately by its heartbeat key.
func (c *cache) addHub(ctx context.Context) error {
	bHubID := pkg.U64ToBytes(c.hubID)
	defer slicepool.Put(bHubID)
	return c.rdb.SAdd(ctx, hubsKey, bHubID).Err()
}

// reapHub removes a hub from the "hubs" hash, announces its departure, and
// drains its device set in bounded batches so a hub owning many devices
// doesn't block Redis for too long in one shot: each batch is SRandMemberN'd from
// "hub:devices:<hubID>" in Go, grouped by bucket, then deleted from the
// matching "devices:{bucket}" hash via one Lua call per bucket group. It
// backs both graceful shutdown and crash detection (via
// handleEvents), and is safe to call concurrently from multiple hub
// instances: only one of them wins the reap lock and actually does the work,
// the rest return immediately.
func (c *cache) reapHub(ctx context.Context, hubID uint64) error {
	var (
		bHubID         = pkg.U64ToBytes(hubID)
		reapHubLockKey = pkg.Concat(hubReapLockPrefix, hubID)
	)

	acquired, err := c.rdb.SetNX(
		ctx,
		pkg.BytesToString(reapHubLockKey),
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
			pkg.BytesToString(reapHubLockKey),
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

	hubDeviceKey := pkg.Concat(hubDevicesPrefix, hubID)
	for {
		// SRANDMEMBER: read without deleting. Deletion (both from hub set and
		// devices hash) only occurs after the corresponding HDEL has finished running
		// in Lua — if there is a crash/error halfway through, the member is still intact
		// in hub:devices:<hubID> so that the next reap can pick it up automatically.
		popped, popErr := c.rdb.SRandMemberN(
			ctx,
			pkg.BytesToString(hubDeviceKey),
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
				// Shouldn't happen; skip a malformed member defensively
				// rather than failing the whole reap over it.
				continue
			}
			bucket := uint32(c.bucketFor(id))
			byBucket[bucket] = append(byBucket[bucket], deviceIDStr)
		}

		for bucket, ids := range byBucket {
			deviceBucketKey := pkg.Concat(devicesPrefix, uint64(bucket))

			args := make([]any, 1+len(ids))
			args[0] = bHubID
			for idx, strID := range ids {
				args[idx+1] = strID
			}

			runErr := reapDevicesInBucketScript.Run(
				ctx,
				c.rdb,
				[]string{
					pkg.BytesToString(deviceBucketKey),
					pkg.BytesToString(hubDeviceKey),
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
			// Fewer than requested — the set is (about to be) drained.
			break
		}
	}

	hubHeartbeatKey := pkg.Concat(hubHeartbeatPrefix, hubID)
	err = c.rdb.Del(
		ctx,
		pkg.BytesToString(hubHeartbeatKey),
	).Err()

	slicepool.Put(hubDeviceKey)
	slicepool.Put(hubHeartbeatKey)

	return err
}

// checkSelfReaped verifies this hub is still present in "hubs" after a gap
// long enough that another instance could have reaped it as crashed. If it's
// gone, this re-registers the hub entry itself (best-effort — a live hub
// should be visible in "hubs" again so other hubs route new devices to it),
// and always notifies the app via OnSelfReaped regardless of whether that
// re-registration succeeded. The app still needs to know: it may believe it
// owns TCP connections / devices that were reassigned to a live hub while
// this hub was partitioned, and needs to reconcile that itself (e.g.
// force-disconnect local clients so they reconnect fresh and get correctly
// re-assigned) — this function does not attempt that reconciliation.
func (c *cache) checkSelfReaped(ctx context.Context) {
	if !c.selfReapChecking.CompareAndSwap(false, true) {
		return // there is anyone is already running; skipping this one.
	}
	defer c.selfReapChecking.Store(false)

	bHubID := pkg.U64ToBytes(c.hubID)
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
		// Best-effort; hook up c.logger here if you want visibility. Don't
		// return early on this error — the app still needs the callback below.
		c.logger.Warn("failed to re-register hub after a possible self-reap")
	}

	c.callbacks.OnSelfReaped()
}

// verifyTurnOnExpiredNotifications check Redis turn on emit keyEvent
// notifications for expired keys, which handleEvents relies on to
// detect a crashed hub's expired heartbeat lease.
func (c *cache) verifyTurnOnExpiredNotifications(ctx context.Context) error {
	verify, err := c.rdb.ConfigGet(ctx, "notify-keyspace-events").Result()
	if err != nil {
		return err
	}

	got := verify["notify-keyspace-events"]
	if !strings.Contains(got, "E") ||
		!(strings.Contains(got, "A") ||
			strings.Contains(got, "x")) {
		return fmt.Errorf("notify-keyspace-events did not take effect: got %q", got)
	}
	return nil
}

// handleEvents runs the cache background loop. It refreshes the hub
// heartbeat and processes hub/device deletion and heartbeat-expiration events
// until the context is canceled or a subscription is closed.
func (c *cache) handleEvents(ctx context.Context) {
	var (
		ticker          = time.NewTicker(c.heartbeatInterval)
		hubHeartbeatKey = pkg.Concat(hubHeartbeatPrefix, c.hubID)

		// go-redis gives no explicit "reconnected" event — the pool retries and
		// reconnects transparently under the hood. We only ever observe pass/fail
		// results from each Set call, so we infer a partition by tracking how
		// long it's been since our last *confirmed* heartbeat write.
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
				pkg.BytesToString(hubHeartbeatKey),
				1,
				c.heartbeatTTL,
			).Err()

			if err != nil {
				// Hook up c.logger here if you want visibility into
				// repeated failures. Deliberately don't touch lastSuccess:
				// the gap keeps growing across ticks until a writing succeeds.
				continue
			}

			// We just went from "couldn't confirm the lease" to "confirmed
			// it". If the gap since our last confirmed write is at least
			// the TTL, Redis could have expired our lease in the meantime —
			// the same condition another hub's crash detector relies on to
			// reap us. Verify before assuming the worst.
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

			buf := pkg.StringToBytes(delDeviceMsg.Payload)
			hubID := binary.LittleEndian.Uint64(buf[8:16])
			if hubID != c.hubID {
				c.callbacks.OnDeviceDeleted(
					hubID,
					shareddevice.ID(binary.LittleEndian.Uint64(buf[:8])),
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

// deviceBucket returns the registry bucket for deviceID.
func (c *cache) bucketFor(id uint64) uint32 {
	return uint32(pkg.Mix64(id) % uint64(c.deviceBucketCount))
}

// subscribe creates a Redis Pub/Sub subscription and waits until Redis
// acknowledges the subscription before returning it.
func (c *cache) subscribe(ctx context.Context, channel string) (*redis.PubSub, error) {
	sub := c.rdb.Subscribe(ctx, channel)
	if _, err := sub.Receive(ctx); err != nil {
		_ = sub.Close()
		return nil, err
	}
	return sub, nil
}
