/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package zapcore

// addFields is a copy of the function with the same name in
// go.uber.org/zap/zapcore/field.go. The function is not exported there, and
// the forked encoder and the forked core both use it.
//
// Field is an alias of the Field of upstream. Therefore, AddTo accepts the
// encoder of this fork.
func addFields(enc ObjectEncoder, fields []Field) {
	for i := range fields {
		fields[i].AddTo(enc)
	}
}
