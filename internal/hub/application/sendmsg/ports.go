/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package sendmsg

import (
	"context"

	pbpriv "xxx/api/hub/v2/proto/gen/priv"
)

type Broker interface {
	Route(ctx context.Context, toHubID uint64, message *pbpriv.Message) error

	RunInboxConsumer(ctx context.Context) error
}

type GenID interface {
	NextID() (uint64, error)
}
