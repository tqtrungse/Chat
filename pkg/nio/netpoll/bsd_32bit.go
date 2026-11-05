//go:build (darwin || dragonfly || freebsd || netbsd || openbsd) && (386 || arm)

/*
 * Copyright (c) 2023 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package netpoll

type keventIdent = uint32
