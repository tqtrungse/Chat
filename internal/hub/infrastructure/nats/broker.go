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

package nats

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	pbpriv "xxx/api/hub/v1/proto/gen/priv"
	"xxx/internal/hub/connection"

	"xxx/pkg"
	"xxx/pkg/log"
	slicepool "xxx/pkg/pool/slice"
	workerpool "xxx/pkg/pool/worker"

	"github.com/nats-io/nats.go"
	natscore "github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

var (
	errMaxHopsExceeded = errors.New("broker: max hops exceeded")

	defaultDeliveryBackoff = []time.Duration{
		100 * time.Millisecond, // First attempt: Extremely fast retry for transient errors (CPU spike, Goroutine pause)
		500 * time.Millisecond, // Second attempt: Fast retry if the Hub has just reconnected the socket
		1 * time.Second,        // Third attempt: Hub starts showing signs of overload
		2 * time.Second,        // Fourth attempt: Wait for network stability
		5 * time.Second,        // Fifth and subsequent attempts: Cap - Repeat every 5 seconds to avoid exhausting the CPU/NATS
	}
)

const (
	limitMaxHops = uint8(120)

	defaultMaxHops        = uint8(5)
	defaultPublishTimeout = 30 * time.Second
	defaultInboxWorkers   = 128
)

type broker struct {
	hubSubjectPrefix string
	hubID            uint64
	inboxWorkers     uint32
	maxHops          uint8
	publishTimeout   time.Duration
	deliveryBackOff  []time.Duration
	nc               *nats.Conn
	logger           *log.Logger
	pool             *workerpool.Pool
	onMessage        func(*pbpriv.Message) (bool, uint64, error)
	pushToOffline    func(context.Context, uint64, []byte) error
	pbMarshal        proto.MarshalOptions
}

func NewBroker(
	cfg Config,
	hubID uint64,
	nc *nats.Conn,
	logger *log.Logger,
	pool *workerpool.Pool,
	onMessage func(*pbpriv.Message) (bool, uint64, error),
	//persists a message for a device that can't be delivered live right now
	pushToOffline func(context.Context, uint64, []byte) error,
) (connection.Broker, error) {
	// In `handleInboxMessage`, we mutate the struct by incrementing the hops.
	// If hops is over 127, `MarshalOptions.UseCachedSize` will not apply and
	// `proto.MarshalAppend` will either panic or write corrupted data.
	// So we must set `maxHops` less than 127 to be able to use `MarshalOptions.UseCachedSize`.
	if cfg.MaxHops > limitMaxHops {
		return nil, errMaxHopsExceeded
	}

	b := &broker{
		hubSubjectPrefix: cfg.HubSubjectPrefix,
		hubID:            hubID,
		nc:               nc,
		logger:           logger,
		pool:             pool,
		onMessage:        onMessage,
		pushToOffline:    pushToOffline,
		pbMarshal:        proto.MarshalOptions{UseCachedSize: true},
	}

	b.maxHops = cfg.MaxHops
	if cfg.MaxHops == 0 {
		b.maxHops = defaultMaxHops
	}

	b.publishTimeout = cfg.PublishTimeout
	if cfg.PublishTimeout <= 0 {
		// Core NATS publish is a local buffered write, not a round trip, so
		// this now only bounds the Flush confirmation in Route — it no
		// longer waits on a JetStream persistence/ack handshake.
		b.publishTimeout = defaultPublishTimeout
	}

	b.deliveryBackOff = cfg.DeliveryBackOff
	if len(cfg.DeliveryBackOff) == 0 {
		b.deliveryBackOff = defaultDeliveryBackoff
	}

	b.inboxWorkers = uint32(cfg.InboxWorkers)
	if cfg.InboxWorkers <= 0 {
		// Core NATS has no consumer-side batching/prefetch knob to reuse
		// (ConsumerFetchBatch is gone along with the pull consumer), so
		// this is the direct replacement for it. Picked a reasonable
		// starting point, not a measured one — tune against real load.
		b.inboxWorkers = defaultInboxWorkers
	}
	return b, nil
}

// Route delivers msg to dstHubID's inbox subject over core NATS pub/sub:
// fire-and-forget, at-most-once, no persistence, no redelivery. A nil
// return means the payload was handed to the local NATS client and a round
// trip to the server (via Flush) confirmed the connection is alive — not
// that dstHubID received or processed it.
func (b *broker) Route(
	ctx context.Context,
	dstHubID uint64,
	msg *pbpriv.Message,
) error {
	if msg.Hops >= uint32(b.maxHops) {
		return errMaxHopsExceeded
	}

	buf := slicepool.Get(b.pbMarshal.Size(msg))
	buf, err := b.pbMarshal.MarshalAppend(buf[:0], msg)
	if err != nil {
		slicepool.Put(buf)
		return err
	}

	subject := pkg.Concat(b.hubSubjectPrefix, dstHubID)
	err = b.nc.Publish(pkg.BytesToString(subject), buf)
	slicepool.Put(buf)
	slicepool.Put(subject)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, b.publishTimeout)
	defer cancel()
	return b.nc.FlushWithContext(ctx)
}

// RunInboxConsumer subscribes to this hub's own inbox subject and dispatches
// each message to handleInboxMessage, bounded to InboxWorkers concurrent handlers.
func (b *broker) RunInboxConsumer(ctx context.Context) error {
	var (
		wg      = sync.WaitGroup{}
		sem     = make(chan struct{}, b.inboxWorkers)
		subject = fmt.Sprintf("%s%d", b.hubSubjectPrefix, b.hubID)
	)

	sub, err := b.nc.Subscribe(subject, func(msg *natscore.Msg) {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			// Shutting down and every worker slot is busy — drop rather
			// than block the NATS client's internal dispatch goroutine
			// indefinitely. Consistent with the at-most-once semantics
			// this whole package already accepted for Route.
			return
		}
		wg.Add(1)

		err := b.pool.Submit(func(_ *workerpool.Context) {
			b.handleInboxMessage(ctx, msg)
			<-sem
			wg.Done()
		})
		if err != nil {
			<-sem
			wg.Done()

			b.logger.Error("failed to submit inbox message to worker pool", zap.Error(err))
		}
	})
	if err != nil {
		return fmt.Errorf("subscribe to %s: %w", subject, err)
	}

	<-ctx.Done()
	// Drain stops the server from delivering any new message to this
	// subscription; whatever's already buffered may still reach the
	// callback above briefly in the background.
	statusCh := sub.StatusChanged(natscore.SubscriptionClosed)
	if dErr := sub.Drain(); dErr != nil {
		b.logger.Error("failed to drain subscription", zap.Error(dErr))
		_ = sub.Unsubscribe()
	}

	<-statusCh

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All submitted handlers completed.
	case <-time.After(2 * b.publishTimeout):
		b.logger.Warn("timed out waiting for inbox handlers to finish")
	}

	return ctx.Err()
}

func (b *broker) handleInboxMessage(
	ctx context.Context,
	natMsg *natscore.Msg,
) {
	var msg pbpriv.Message
	if err := proto.Unmarshal(natMsg.Data, &msg); err != nil {
		b.logger.Error("message is corrupted, ignore", zap.Error(err))
		// Core NATS: no Term() to call — there's no redelivery to suppress
		// in the first place. The message is simply gone either way.
		return
	}

	// Separate the shutdown-cancel signal from the I/O processing logic,
	// but retain all TraceID/Logger data currently in ctx.
	var (
		execCtx          = context.WithoutCancel(ctx)
		sent, hubID, err = b.onMessage(&msg)
		resErr           = b.resolve(execCtx, &msg, sent, hubID, err)
	)

	if resErr == nil {
		return
	}
	b.logger.Warn(
		"first delivery attempt failed, entering local retry",
		zap.Uint64("recv_device_id", msg.RecvDeviceId),
		zap.Error(resErr),
	)
	b.retryLocalDelivery(execCtx, &msg)
}

// resolve applies one OnMessage outcome: local delivery, re-route to
// wherever the ledger now says the device is, or offline fallback. A nil
// return means the message reached a terminal, successful outcome. Any
// non-nil return — including a failed re-route or a failed offline write,
// not just a failed OnMessage call.
func (b *broker) resolve(
	ctx context.Context,
	msg *pbpriv.Message,
	sent bool,
	hubID uint64,
	err error,
) error {
	if err != nil {
		return err
	}
	if sent {
		return nil
	}
	if hubID == 0 {
		// Device is fully offline.
		return b.pushOffline(ctx, msg)
	}

	// No longer at this hub (switched hubs while the message was in
	// transit, or this hub just restarted and the device reconnected
	// elsewhere) -> forward instead of drop.
	msg.Hops++
	rErr := b.Route(ctx, hubID, msg)
	if rErr == nil {
		return nil
	}
	if !errors.Is(rErr, errMaxHopsExceeded) {
		return rErr
	}
	// Bounced too many times — fall back to offline instead of retrying
	// the route forever.
	b.logger.Warn(
		"message bouncing between hubs, max hops reached, forcing offline",
		zap.Uint64("recv_device_id", msg.RecvDeviceId),
		zap.Uint32("hops", msg.Hops),
	)
	return b.pushOffline(ctx, msg)
}

// retryLocalDelivery retries delivery of message using the configured backoff
// schedule after the initial delivery attempt fails.
//
// Each retry waits for its corresponding backoff duration, then invokes
// onMessage and resolves the result through resolve. Retrying stops as soon
// as resolve succeeds or ctx is canceled.
//
// If all retry attempts are exhausted, retryLocalDelivery makes one final
// best-effort attempt to persist the message to offline storage.
func (b *broker) retryLocalDelivery(
	ctx context.Context,
	msg *pbpriv.Message,
) {
	for attempt, delay := range b.deliveryBackOff {
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return
		}

		sent, hubID, err := b.onMessage(msg)
		if resErr := b.resolve(ctx, msg, sent, hubID, err); resErr == nil {
			return
		} else {
			b.logger.Warn(
				"local delivery retry failed",
				zap.Int("attempt", attempt+1),
				zap.Uint64("recv_device_id", msg.RecvDeviceId),
				zap.Error(resErr),
			)
		}
	}

	// Exhausted the retry budget. Last resort: try one direct offline
	// write — OnMessage itself might be what's persistently broken, not
	// routing or offline storage.
	if pErr := b.pushOffline(ctx, msg); pErr != nil {
		b.logger.Error(
			"failed to persist offline message after exhausting local retries",
			zap.Uint64("recv_device_id", msg.RecvDeviceId),
			zap.Error(pErr),
		)
	}
}

// pushOffline isolates the logic to persist a message via OfflineStore.
func (b *broker) pushOffline(
	ctx context.Context,
	msg *pbpriv.Message,
) error {
	buf := slicepool.Get(b.pbMarshal.Size(msg))
	buf, err := b.pbMarshal.MarshalAppend(buf[:0], msg)
	if err != nil {
		slicepool.Put(buf)
		return fmt.Errorf("marshal for offline: %w", err)
	}

	err = b.pushToOffline(ctx, msg.RecvDeviceId, buf)
	slicepool.Put(buf)
	return err
}
