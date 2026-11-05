/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package sql

import (
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEscapeLike(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "empty",
			in:   "",
			want: "",
		},
		{
			name: "no special characters",
			in:   "abc 123",
			want: "abc 123",
		},
		{
			name: "percent",
			in:   "100%",
			want: "100!%"},
		{
			name: "underscore",
			in:   "a_b",
			want: "a!_b",
		},
		{
			name: "escape character",
			in:   "!",
			want: "!!",
		},
		{
			name: "all three",
			in:   "%_!",
			want: "!%!_!!",
		},
		{
			name: "repeated percent",
			in:   "%%",
			want: "!%!%",
		},
		{
			name: "repeated underscore",
			in:   "__",
			want: "!_!_",
		},
		{
			name: "escape character before percent",
			in:   "!%",
			want: "!!!%",
		},
		{
			name: "mixed text",
			in:   "50% off_sale!",
			want: "50!% off!_sale!!",
		},
		{
			name: "unicode",
			in:   "héllo_wörld%",
			want: "héllo!_wörld!%",
		},
		{
			name: "unicode only",
			in:   "日本語",
			want: "日本語",
		},
		// Backslash is not special. Its behavior depends on the
		// NO_BACKSLASH_ESCAPES SQL mode, so EscapeLike must not change it.
		{
			name: "backslash unchanged",
			in:   `a\b`,
			want: `a\b`,
		},
		{
			name: "backslash before percent",
			in:   `\%`,
			want: `\!%`,
		},
		// EscapeLike escapes LIKE characters only. It does not quote SQL.
		{
			name: "single quote unchanged",
			in:   "it's",
			want: "it's",
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.name,
			func(t *testing.T) {
				got := EscapeLike(tt.in)
				assert.Equal(t, tt.want, got)
			},
		)
	}
}

// likeMatch is a small model of the SQL LIKE operator with ESCAPE '!'.
// '%' matches zero or more characters, '_' matches exactly one character,
// and '!' makes the next character literal.
func likeMatch(pattern, s []rune) bool {
	for len(pattern) > 0 {
		switch p := pattern[0]; p {
		case '%':
			rest := pattern[1:]
			for i := 0; i <= len(s); i++ {
				if likeMatch(rest, s[i:]) {
					return true
				}
			}
			return false

		case '_':
			if len(s) == 0 {
				return false
			}
			pattern, s = pattern[1:], s[1:]

		case '!':
			if len(pattern) < 2 || len(s) == 0 || s[0] != pattern[1] {
				return false
			}
			pattern, s = pattern[2:], s[1:]

		default:
			if len(s) == 0 || s[0] != p {
				return false
			}
			pattern, s = pattern[1:], s[1:]
		}
	}
	return len(s) == 0
}

// TestEscapeLikeMatchesLiterally checks the meaning of the result. The escaped
// pattern must match the original string, and it must not match a string that
// only a wildcard would accept.
func TestEscapeLikeMatchesLiterally(t *testing.T) {
	tests := []struct {
		name     string
		in       string
		match    []string
		mismatch []string
	}{
		{
			name:     "percent is not a wildcard",
			in:       "100%",
			match:    []string{"100%"},
			mismatch: []string{"100", "1000", "100 percent"},
		},
		{
			name:     "underscore is not a single-character wildcard",
			in:       "a_b",
			match:    []string{"a_b"},
			mismatch: []string{"axb", "a-b", "ab"},
		},
		{
			name:     "escape character is literal",
			in:       "a!b",
			match:    []string{"a!b"},
			mismatch: []string{"ab", "a!!b"},
		},
		{
			name:     "only wildcards",
			in:       "%_",
			match:    []string{"%_"},
			mismatch: []string{"", "ab", "%", "_"},
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.name,
			func(t *testing.T) {
				pattern := []rune(EscapeLike(tt.in))
				for _, s := range tt.match {
					assert.Truef(
						t,
						likeMatch(pattern, []rune(s)),
						"pattern %q should match %q", string(pattern), s,
					)
				}

				for _, s := range tt.mismatch {
					assert.Falsef(
						t,
						likeMatch(pattern, []rune(s)), "pattern %q must not match %q",
						string(pattern), s,
					)
				}
			},
		)
	}
}

func FuzzEscapeLike(f *testing.F) {
	seeds := [][2]string{
		{"", ""},
		{"abc", "abc"},
		{"100%", "100x"},
		{"a_b", "axb"},
		{"!%_", "!!%"},
		{`a\b`, `a\b`},
		{"héllo_", "héllox"},
	}
	for _, s := range seeds {
		f.Add(s[0], s[1])
	}

	f.Fuzz(func(t *testing.T, s, other string) {
		if !utf8.ValidString(s) || !utf8.ValidString(other) {
			t.Skip("input is not valid UTF-8")
		}

		pattern := []rune(EscapeLike(s))

		// The escaped pattern must match the original string.
		if !likeMatch(pattern, []rune(s)) {
			require.Failf(t, "", "EscapeLike(%q) = %q does not match the input", s, string(pattern))
		}
		// The escaped pattern must not match any different string.
		if other != s && likeMatch(pattern, []rune(other)) {
			require.Failf(t, "", "EscapeLike(%q) = %q wrongly matches %q", s, string(pattern), other)
		}
	})
}
