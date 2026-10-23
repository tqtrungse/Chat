//go:build (darwin || dragonfly || freebsd || netbsd || openbsd) && (amd64 || arm64)

/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package netpoll

type keventIdent = uint64
