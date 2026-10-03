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
	"sync/atomic"
	"testing"
	"time"

	pbpriv "xxx/api/hub/v1/proto/gen/priv"

	"xxx/pkg/log"
	workerpool "xxx/pkg/pool/worker"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func startTestNATS(t *testing.T) *nats.Conn {
	t.Helper()

	opts := &natsserver.Options{
		Host:   "127.0.0.1",
		Port:   -1, // random free port
		NoLog:  true,
		NoSigs: true,
	}
	srv, err := natsserver.NewServer(opts)
	require.NoError(t, err)

	go srv.Start()
	require.True(t, srv.ReadyForConnections(5*time.Second))
	t.Cleanup(srv.Shutdown)

	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)

	return nc
}

func newTestLogger(t *testing.T) *log.Logger {
	t.Helper()
	logger, closeLogger := log.NewLogger(nil)
	t.Cleanup(func() { _ = closeLogger() })
	return logger
}

func newTestPool(t *testing.T, workers int) *workerpool.Pool {
	t.Helper()
	p, closePool := workerpool.New(
		workerpool.Config{NumWorkers: workers},
		func(workerID int, recovered any, stack []byte) {},
	)
	t.Cleanup(func() { closePool() })
	return p
}

func validConfig() Config {
	return Config{
		HubSubjectPrefix: "hub.inbox.",
		MaxHops:          5,
		PublishTimeout:   2 * time.Second,
		InboxWorkers:     8,
	}
}

func mustNewBroker(
	t *testing.T,
	cfg Config,
	hubID uint64,
	nc *nats.Conn,
	onMsg func(*pbpriv.Message) (bool, uint64, error),
	pushOff func(context.Context, uint64, []byte) error,
) *broker {
	t.Helper()

	svc, err := NewBroker(
		cfg,
		hubID,
		nc,
		newTestLogger(t),
		newTestPool(t, 4),
		onMsg,
		pushOff,
	)
	require.NoError(t, err)

	br, ok := svc.(*broker)
	require.True(t, ok)
	return br
}

// newBareBroker builds a *broker directly, bypassing NewBroker's validation
// and defaulting, for white-box tests of resolve/retryLocalDelivery/
// pushOffline that want tight control over individual fields (e.g. a short
// deliveryBackOff so the test runs fast).
func newBareBroker(
	t *testing.T,
	nc *nats.Conn,
	maxHops uint8,
	onMsg func(*pbpriv.Message) (bool, uint64, error),
	pushOff func(context.Context, uint64, []byte) error,
) *broker {
	t.Helper()

	return &broker{
		hubSubjectPrefix: "hub.inbox.",
		hubID:            1,
		maxHops:          maxHops,
		publishTimeout:   2 * time.Second,
		nc:               nc,
		logger:           newTestLogger(t),
		onMessage:        onMsg,
		pushToOffline:    pushOff,
		pbMarshal:        proto.MarshalOptions{UseCachedSize: true},
	}
}

func noopOnMessage(sent bool, hubID uint64, err error) func(*pbpriv.Message) (bool, uint64, error) {
	return func(*pbpriv.Message) (bool, uint64, error) { return sent, hubID, err }
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	require.Failf(t, "", "condition not met within %s", timeout)
}

// waitForSubscription blocks until nc's active subscription count exceeds
// baseline -- i.e. until a subscription registered after baseline was taken
// has actually been established with the server.
//
// Comparing against a fixed baseline (rather than asserting count > 0,
// which an earlier version of this file did) matters whenever the test
// already holds other subscriptions of its own on the same connection: with
// a bare "> 0" check, a subscription the test itself made earlier (e.g. to
// observe a forwarded message) satisfies the condition immediately, so the
// wait returns before the broker's own Subscribe call has necessarily run
// on its separate goroutine -- a race that shows up as an intermittent
// NextMsg timeout, since the publishing that follows can beat the broker's
// subscription to the server.
func waitForSubscription(t *testing.T, nc *nats.Conn, baseline int) {
	t.Helper()
	waitFor(t, time.Second, func() bool { return nc.NumSubscriptions() > baseline })
}

// startConsumer runs RunInboxConsumer in the background and hands back a
// cancel func plus a wait func that blocks for its return value.
func startConsumer(t *testing.T, br *broker) (cancel context.CancelFunc, wait func() error) {
	t.Helper()

	ctx, cancelFn := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- br.RunInboxConsumer(ctx) }()
	return cancelFn, func() error {
		select {
		case err := <-errCh:
			return err
		case <-time.After(5 * time.Second):
			t.Fatal("RunInboxConsumer did not return in time")
			return nil
		}
	}
}

// recorder is a concurrency-safe collector for callback invocations, since
// onMessage/pushToOffline get called from worker-pool goroutines. Uses
// proto.Clone rather than a naive struct copy -- pbpriv.Message embeds
// protoimpl.MessageState, and copying that directly trips go vets
// copy locks check.
type recorder struct {
	mu    sync.Mutex
	calls []*pbpriv.Message
}

func (r *recorder) add(msg *pbpriv.Message) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, proto.Clone(msg).(*pbpriv.Message))
}

func (r *recorder) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func TestBroker_New_MaxHopsTooLarge(t *testing.T) {
	nc := startTestNATS(t)
	cfg := validConfig()
	cfg.MaxHops = limitMaxHops + 1

	_, err := NewBroker(
		cfg,
		1,
		nc,
		newTestLogger(t),
		newTestPool(t, 4),
		noopOnMessage(true, 0, nil),
		func(context.Context, uint64, []byte) error { return nil },
	)
	require.Error(t, err)
}

func TestBroker_New_HappyPath(t *testing.T) {
	nc := startTestNATS(t)
	br := mustNewBroker(
		t,
		validConfig(),
		1,
		nc,
		noopOnMessage(true, 0, nil),
		func(context.Context, uint64, []byte) error { return nil },
	)
	require.NotNil(t, br)
}

// TestBroker_New_DefaultsWhenZeroValue documents the intended (and, for the
// all-zero-value path, actually working) behavior.
func TestBroker_New_DefaultsWhenZeroValue(t *testing.T) {
	nc := startTestNATS(t)
	cfg := Config{HubSubjectPrefix: "hub.inbox."} // everything else zero-value

	br := mustNewBroker(
		t,
		cfg,
		1,
		nc,
		noopOnMessage(true, 0, nil),
		func(context.Context, uint64, []byte) error { return nil },
	)

	require.Equal(t, uint8(5), br.maxHops)
	require.Equal(t, 30*time.Second, br.publishTimeout)
	require.Len(t, br.deliveryBackOff, 5)
	require.Equal(t, uint32(128), br.inboxWorkers)
}

// TestBroker_New_PreservesCustomValues encodes the INTENDED behavior for a
// caller who explicitly sets these fields away from zero. See the "READ ME
// FIRST" comment at the top of this file -- this currently fails.
func TestBroker_New_PreservesCustomValues(t *testing.T) {
	nc := startTestNATS(t)
	cfg := Config{
		HubSubjectPrefix: "hub.inbox.",
		MaxHops:          10,
		PublishTimeout:   7 * time.Second,
		DeliveryBackOff:  []time.Duration{50 * time.Millisecond},
		InboxWorkers:     64,
	}

	br := mustNewBroker(
		t,
		cfg,
		1,
		nc,
		noopOnMessage(true, 0, nil),
		func(context.Context, uint64, []byte) error { return nil },
	)

	require.Equal(t, cfg.MaxHops, br.maxHops)
	require.Equal(t, cfg.PublishTimeout, br.publishTimeout)
	require.Equal(t, cfg.DeliveryBackOff, br.deliveryBackOff)
	require.Equal(t, uint32(cfg.InboxWorkers), br.inboxWorkers)
}

func TestBroker_Route_PublishesToTargetHubInbox(t *testing.T) {
	nc := startTestNATS(t)
	cfg := validConfig()
	br := mustNewBroker(
		t,
		cfg,
		1,
		nc,
		noopOnMessage(true, 0, nil),
		func(context.Context, uint64, []byte) error { return nil },
	)

	const toHubID = uint64(42)
	wantSubject := fmt.Sprintf("%s%d", cfg.HubSubjectPrefix, toHubID)

	sub, err := nc.SubscribeSync(wantSubject)
	require.NoError(t, err)
	defer func() { _ = sub.Unsubscribe() }()

	msg := &pbpriv.Message{
		RecvDeviceId: 7,
		Hops:         0,
	}
	err = br.Route(
		context.Background(),
		toHubID,
		msg,
	)
	require.NoError(t, err)

	natMsg, err := sub.NextMsg(2 * time.Second)
	require.NoError(t, err)
	require.Equal(t, wantSubject, natMsg.Subject)

	var got pbpriv.Message
	err = proto.Unmarshal(natMsg.Data, &got)
	require.NoError(t, err)
	require.Equal(t, msg.RecvDeviceId, got.RecvDeviceId)
}

func TestBroker_Route_MaxHopsExceeded(t *testing.T) {
	nc := startTestNATS(t)
	cfg := validConfig() // MaxHops: 5
	br := mustNewBroker(
		t,
		cfg,
		1,
		nc,
		noopOnMessage(true, 0, nil),
		func(context.Context, uint64, []byte) error { return nil },
	)

	const toHubID = uint64(42)
	subject := fmt.Sprintf("%s%d", cfg.HubSubjectPrefix, toHubID)
	sub, err := nc.SubscribeSync(subject)
	require.NoError(t, err)
	defer func() { _ = sub.Unsubscribe() }()

	msg := &pbpriv.Message{
		RecvDeviceId: 7,
		Hops:         uint32(cfg.MaxHops),
	}
	err = br.Route(
		context.Background(),
		toHubID,
		msg,
	)
	require.Equal(t, errMaxHopsExceeded, err)

	_, err = sub.NextMsg(200 * time.Millisecond)
	require.Error(t, err)
}

func TestBroker_Route_RespectsCallerContextCancellation(t *testing.T) {
	nc := startTestNATS(t)
	br := mustNewBroker(
		t,
		validConfig(),
		1,
		nc,
		noopOnMessage(true, 0, nil),
		func(context.Context, uint64, []byte) error { return nil },
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already canceled

	err := br.Route(
		ctx,
		42,
		&pbpriv.Message{RecvDeviceId: 7},
	)
	require.Equal(t, context.Canceled, err)
}

func TestBroker_Resolve_PropagatesOnMessageError(t *testing.T) {
	nc := startTestNATS(t)
	wantErr := errors.New("boom")
	br := newBareBroker(
		t,
		nc,
		5,
		nil,
		nil,
	)

	err := br.resolve(
		context.Background(),
		&pbpriv.Message{},
		false,
		0,
		wantErr,
	)
	require.Equal(t, wantErr, err)
}

func TestResolve_SentReturnsNil(t *testing.T) {
	nc := startTestNATS(t)
	br := newBareBroker(
		t,
		nc,
		5,
		nil,
		nil,
	)

	err := br.resolve(
		context.Background(),
		&pbpriv.Message{},
		true,
		0,
		nil,
	)
	require.NoError(t, err)
}

func TestBroker_Resolve_HubIDZeroPushesOffline(t *testing.T) {
	nc := startTestNATS(t)

	var rec recorder
	pushOff := func(_ context.Context, _ uint64, buf []byte) error {
		var m pbpriv.Message
		err := proto.Unmarshal(buf, &m)
		require.NoError(t, err)
		rec.add(&m)
		return nil
	}
	br := newBareBroker(
		t,
		nc,
		5,
		nil,
		pushOff,
	)

	msg := &pbpriv.Message{RecvDeviceId: 99}
	err := br.resolve(
		context.Background(),
		msg,
		false,
		0,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, 1, rec.len())
}

func TestBroker_Resolve_ForwardsToNewHubAndIncrementsHops(t *testing.T) {
	nc := startTestNATS(t)
	br := newBareBroker(
		t,
		nc,
		5,
		nil,
		nil,
	)

	const otherHubID = uint64(2)
	subject := fmt.Sprintf("%s%d", br.hubSubjectPrefix, otherHubID)
	sub, err := nc.SubscribeSync(subject)
	require.NoError(t, err)
	defer func() { _ = sub.Unsubscribe() }()

	msg := &pbpriv.Message{RecvDeviceId: 3, Hops: 1}
	err = br.resolve(
		context.Background(),
		msg,
		false,
		otherHubID,
		nil,
	)
	require.NoError(t, err)

	natMsg, err := sub.NextMsg(2 * time.Second)
	require.NoError(t, err)

	var got pbpriv.Message
	err = proto.Unmarshal(natMsg.Data, &got)
	require.NoError(t, err)
	require.NotEqual(t, 2, got.Hops)
}

func TestBroker_Resolve_FallsBackToOfflineWhenForwardExceedsMaxHops(t *testing.T) {
	nc := startTestNATS(t)

	var rec recorder
	pushOff := func(_ context.Context, _ uint64, buf []byte) error {
		var m pbpriv.Message
		err := proto.Unmarshal(buf, &m)
		require.NoError(t, err)
		rec.add(&m)
		return nil
	}
	br := newBareBroker(
		t,
		nc,
		5,
		nil,
		pushOff,
	)

	// One below the limit; resolve increments it to exactly maxHops
	// before calling Route, which must then refuse to send.
	msg := &pbpriv.Message{
		RecvDeviceId: 4,
		Hops:         uint32(br.maxHops) - 1,
	}
	err := br.resolve(
		context.Background(),
		msg,
		false,
		2,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, 1, rec.len())
}

func TestBroker_PushOffline_MarshalsAndCallsCallback(t *testing.T) {
	nc := startTestNATS(t)

	var got []byte
	var gotDeviceID uint64
	pushOff := func(_ context.Context, deviceID uint64, buf []byte) error {
		gotDeviceID = deviceID
		got = append([]byte(nil), buf...) // buf is pool-owned once we return
		return nil
	}

	br := newBareBroker(
		t,
		nc,
		5,
		nil,
		pushOff,
	)

	msg := &pbpriv.Message{
		RecvDeviceId: 55,
		Hops:         3,
	}
	err := br.pushOffline(context.Background(), msg)
	require.NoError(t, err)
	require.Equal(t, uint64(55), gotDeviceID)

	var decoded pbpriv.Message
	err = proto.Unmarshal(got, &decoded)
	require.NoError(t, err)
	require.Equal(t, uint32(3), decoded.Hops)
}

func TestBroker_PushOffline_PropagatesCallbackError(t *testing.T) {
	nc := startTestNATS(t)
	wantErr := errors.New("disk full")
	pushOff := func(context.Context, uint64, []byte) error { return wantErr }
	br := newBareBroker(
		t,
		nc,
		5,
		nil,
		pushOff,
	)

	err := br.pushOffline(context.Background(), &pbpriv.Message{RecvDeviceId: 1})
	require.Equal(t, wantErr, err)
}

func TestBroker_RetryLocalDelivery_SucceedsBeforeExhaustingBackoff(t *testing.T) {
	nc := startTestNATS(t)

	var attempts int32
	onMsg := func(*pbpriv.Message) (bool, uint64, error) {
		n := atomic.AddInt32(&attempts, 1)
		if n < 3 {
			return false, 0, errors.New("transient")
		}
		return true, 0, nil // delivered on the 3rd attempt
	}

	var offline recorder
	pushOff := func(_ context.Context, _ uint64, buf []byte) error {
		var m pbpriv.Message
		_ = proto.Unmarshal(buf, &m)
		offline.add(&m)
		return nil
	}

	br := newBareBroker(
		t,
		nc,
		5,
		onMsg,
		pushOff,
	)
	br.deliveryBackOff = []time.Duration{
		5 * time.Millisecond,
		5 * time.Millisecond,
		5 * time.Millisecond,
		5 * time.Millisecond,
		5 * time.Millisecond,
	}

	br.retryLocalDelivery(context.Background(), &pbpriv.Message{RecvDeviceId: 1})

	require.Equal(t, int32(3), atomic.LoadInt32(&attempts))
	require.Zero(t, offline.len())
}

func TestBroker_RetryLocalDelivery_ExhaustsBudgetThenPushesOffline(t *testing.T) {
	nc := startTestNATS(t)

	var attempts int32
	onMsg := func(*pbpriv.Message) (bool, uint64, error) {
		atomic.AddInt32(&attempts, 1)
		return false, 0, errors.New("permanently down")
	}

	var offline recorder
	pushOff := func(_ context.Context, _ uint64, buf []byte) error {
		var m pbpriv.Message
		_ = proto.Unmarshal(buf, &m)
		offline.add(&m)
		return nil
	}

	backoff := []time.Duration{
		2 * time.Millisecond,
		2 * time.Millisecond,
		2 * time.Millisecond,
	}
	br := newBareBroker(
		t,
		nc,
		5,
		onMsg,
		pushOff,
	)
	br.deliveryBackOff = backoff

	br.retryLocalDelivery(context.Background(), &pbpriv.Message{RecvDeviceId: 1})

	require.Len(t, backoff, int(atomic.LoadInt32(&attempts)))
	require.Equal(t, 1, offline.len())
}

// TestBroker_RetryLocalDelivery_IgnoresCancellationOfAWithoutCancelContext is a
// characterization test -- see "READ ME FIRST" at the top of this file.
// It currently passes: retryLocalDelivery runs the full backoff schedule to
// completion even though the parent context was canceled before the call.
func TestBroker_RetryLocalDelivery_IgnoresCancellationOfAWithoutCancelContext(t *testing.T) {
	nc := startTestNATS(t)

	onMsg := func(*pbpriv.Message) (bool, uint64, error) {
		return false, 0, errors.New("still down")
	}

	var offline recorder
	pushOff := func(context.Context, uint64, []byte) error {
		offline.add(&pbpriv.Message{})
		return nil
	}

	backoff := []time.Duration{
		15 * time.Millisecond,
		15 * time.Millisecond,
	}
	br := newBareBroker(
		t,
		nc,
		5,
		onMsg,
		pushOff,
	)
	br.deliveryBackOff = backoff

	parent, cancel := context.WithCancel(context.Background())
	cancel() // canceled BEFORE retryLocalDelivery is even called
	execCtx := context.WithoutCancel(parent)

	start := time.Now()
	br.retryLocalDelivery(execCtx, &pbpriv.Message{RecvDeviceId: 1})
	elapsed := time.Since(start)

	wantMin := time.Duration(len(backoff)) * 15 * time.Millisecond
	require.GreaterOrEqual(t, elapsed, wantMin)
	require.Equal(t, 1, offline.len())
}

func TestBroker_RunInboxConsumer_DeliversAndShutsDownCleanly(t *testing.T) {
	nc := startTestNATS(t)
	cfg := validConfig()

	var delivered recorder
	onMsg := func(m *pbpriv.Message) (bool, uint64, error) {
		delivered.add(m)
		return true, 0, nil
	}
	br := mustNewBroker(t, cfg, 1, nc, onMsg, func(context.Context, uint64, []byte) error { return nil })

	baseline := nc.NumSubscriptions()
	cancel, wait := startConsumer(t, br)
	waitForSubscription(t, nc, baseline)

	payload, err := proto.Marshal(&pbpriv.Message{RecvDeviceId: 9})
	require.NoError(t, err)

	inbox := fmt.Sprintf("%s%d", cfg.HubSubjectPrefix, br.hubID)
	err = nc.Publish(inbox, payload)
	require.NoError(t, err)

	waitFor(t, 2*time.Second, func() bool { return delivered.len() == 1 })

	cancel()
	err = wait()
	require.Equal(t, context.Canceled, err)
}

func TestBroker_RunInboxConsumer_ForwardsToNewHub(t *testing.T) {
	nc := startTestNATS(t)
	cfg := validConfig()
	const forwardTo = uint64(2)

	onMsg := func(*pbpriv.Message) (bool, uint64, error) { return false, forwardTo, nil }
	br := mustNewBroker(
		t,
		cfg,
		1,
		nc,
		onMsg,
		func(context.Context, uint64, []byte) error { return nil },
	)

	forwardSub, err := nc.SubscribeSync(fmt.Sprintf("%s%d", cfg.HubSubjectPrefix, forwardTo))
	require.NoError(t, err)
	defer func() { _ = forwardSub.Unsubscribe() }()

	// Baseline is taken AFTER forwardSub already exists -- this is the
	// call site that was flaky: with a bare NumSubscriptions() > 0 check,
	// forwardSub alone satisfies it, so the wait didn't actually wait for
	// the broker's own inbox subscription to be registered.
	baseline := nc.NumSubscriptions()
	cancel, wait := startConsumer(t, br)
	defer func() {
		cancel()
		_ = wait()
	}()
	waitForSubscription(t, nc, baseline)

	payload, _ := proto.Marshal(&pbpriv.Message{
		RecvDeviceId: 1,
		Hops:         0},
	)
	inbox := fmt.Sprintf("%s%d", cfg.HubSubjectPrefix, br.hubID)
	err = nc.Publish(inbox, payload)
	require.NoError(t, err)

	natMsg, err := forwardSub.NextMsg(2 * time.Second)
	require.NoError(t, err)

	var got pbpriv.Message
	err = proto.Unmarshal(natMsg.Data, &got)
	require.NoError(t, err)
	require.Equal(t, uint32(1), got.Hops)
}

func TestBroker_RunInboxConsumer_PushesOfflineWhenDeviceUnreachable(t *testing.T) {
	nc := startTestNATS(t)
	cfg := validConfig()

	onMsg := func(*pbpriv.Message) (bool, uint64, error) { return false, 0, nil }
	var offline recorder
	pushOff := func(_ context.Context, _ uint64, buf []byte) error {
		var m pbpriv.Message
		_ = proto.Unmarshal(buf, &m)
		offline.add(&m)
		return nil
	}
	br := mustNewBroker(t, cfg, 1, nc, onMsg, pushOff)

	baseline := nc.NumSubscriptions()
	cancel, wait := startConsumer(t, br)
	defer func() {
		cancel()
		_ = wait()
	}()
	waitForSubscription(t, nc, baseline)

	payload, _ := proto.Marshal(&pbpriv.Message{RecvDeviceId: 77})
	inbox := fmt.Sprintf("%s%d", cfg.HubSubjectPrefix, br.hubID)
	err := nc.Publish(inbox, payload)
	require.NoError(t, err)

	waitFor(t, 2*time.Second, func() bool { return offline.len() == 1 })
}

func TestBroker_RunInboxConsumer_DropsCorruptedMessage(t *testing.T) {
	nc := startTestNATS(t)
	cfg := validConfig()

	var onMsgCalls int32
	onMsg := func(*pbpriv.Message) (bool, uint64, error) {
		atomic.AddInt32(&onMsgCalls, 1)
		return true, 0, nil
	}
	br := mustNewBroker(
		t,
		cfg,
		1,
		nc,
		onMsg,
		func(context.Context, uint64, []byte) error { return nil },
	)

	baseline := nc.NumSubscriptions()
	cancel, wait := startConsumer(t, br)
	defer func() {
		cancel()
		_ = wait()
	}()
	waitForSubscription(t, nc, baseline)

	inbox := fmt.Sprintf("%s%d", cfg.HubSubjectPrefix, br.hubID)
	err := nc.Publish(inbox, []byte("not a valid protobuf message"))
	require.NoError(t, err)
	_ = nc.Flush()

	// Negative assertion: give it a generous window, then confirm
	// onMessage was never reached for the corrupted payload.
	time.Sleep(300 * time.Millisecond)
	require.Equal(t, int32(0), atomic.LoadInt32(&onMsgCalls))
}

func TestBroker_RunInboxConsumer_ShutdownWaitsForInFlightHandler(t *testing.T) {
	nc := startTestNATS(t)
	cfg := validConfig()

	release := make(chan struct{})
	started := make(chan struct{}, 1)
	onMsg := func(*pbpriv.Message) (bool, uint64, error) {
		started <- struct{}{}
		<-release // held open until the test says otherwise
		return true, 0, nil
	}
	br := mustNewBroker(
		t,
		cfg,
		1,
		nc,
		onMsg,
		func(context.Context, uint64, []byte) error { return nil },
	)

	baseline := nc.NumSubscriptions()
	cancel, wait := startConsumer(t, br)
	waitForSubscription(t, nc, baseline)

	payload, _ := proto.Marshal(&pbpriv.Message{RecvDeviceId: 1})
	inbox := fmt.Sprintf("%s%d", cfg.HubSubjectPrefix, br.hubID)
	err := nc.Publish(inbox, payload)
	require.NoError(t, err)

	<-started // handler is now blocked inside onMessage
	cancel()  // ask the consumer to shut down while the handler is in flight

	doneEarly := make(chan struct{})
	go func() {
		_ = wait()
		close(doneEarly)
	}()

	select {
	case <-doneEarly:
		require.Fail(t, "RunInboxConsumer returned before the in-flight handler finished")
	case <-time.After(200 * time.Millisecond):
		// Expected: still blocked on wg.Wait().
	}

	close(release) // let the handler finish

	select {
	case <-doneEarly:
		// Good: it returned only after the handler drained.
	case <-time.After(5 * time.Second):
		require.Fail(t, "RunInboxConsumer did not return after the handler finished")
	}
}
