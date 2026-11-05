/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package sql

import "strings"

// likeEscaper escapes special LIKE characters with '!'. Use ESCAPE '!' in the SQL query.
// Do not use '\' because its behavior depends on the NO_BACKSLASH_ESCAPES SQL mode.
var likeEscaper = strings.NewReplacer("!", "!!", "%", "!%", "_", "!_")

// EscapeLike escapes '%' and '_' in the input so that LIKE matches them as literal characters, not wildcards.
func EscapeLike(s string) string { return likeEscaper.Replace(s) }
