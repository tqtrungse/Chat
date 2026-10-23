/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package message

type ID uint64

func (id ID) Uint64() uint64 {
	return uint64(id)
}
