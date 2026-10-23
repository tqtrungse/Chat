/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package sql

import "errors"

var (
	ErrDBNotOpen       = errors.New("db not open")
	ErrDBClosed        = errors.New("db closed")
	ErrExtractDB       = errors.New("failed to extract DB")
	ErrInvalidTxSource = errors.New("invalid transaction source")
)
