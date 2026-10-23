//go:build (darwin || dragonfly || freebsd || netbsd || openbsd) && (amd64 || arm64)

/*
 * Copyright (c) 2023 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package netpoll

type keventIdent = uint64
