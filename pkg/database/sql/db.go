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

package sql

import (
	"context"
	"errors"
)

var (
	ErrExtractDB       = errors.New("failed to extract DB")
	ErrReopenDB        = errors.New("database already open")
	ErrInvalidTxSource = errors.New("invalid transaction source")
)

type TxSource uint8

const (
	TxSourcePrimary  TxSource = 1
	TxSourceReplicas TxSource = 2
)

type CommandFunc func(context.Context) error

type DB interface {
	Open(ctx context.Context, cfg Config) error

	Close() error

	Transaction(
		ctx context.Context,
		source TxSource,
		fn func(ctx context.Context) error,
	) error
}
