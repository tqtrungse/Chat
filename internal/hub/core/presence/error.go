/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package presence

import "errors"

// errSuperseded is returned to a batched request that lost to a newer
// operation on the same device (see dedupeDeviceOps).
var errSuperseded = errors.New("device operation superseded")
