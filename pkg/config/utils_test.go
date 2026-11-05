/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ------------
// Test helpers
// ------------

// writeFile creates the parent folders of path and writes content to path.
func writeFile(t *testing.T, path, content string) {
	t.Helper()

	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
}

// assertSamePath compares two paths after symlink resolution.
// On macOS, the temp folder is a symlink from /var to /private/var.
func assertSamePath(t *testing.T, want, got string) {
	t.Helper()

	wantReal, err := filepath.EvalSymlinks(want)
	require.NoError(t, err)

	gotReal, err := filepath.EvalSymlinks(got)
	require.NoError(t, err)
	assert.Equal(t, wantReal, gotReal)
}

// isolate moves the test into an empty work folder and clears the config env var.
// The env var of the developer machine must not change the test result.
func isolate(t *testing.T, envName string) string {
	t.Helper()

	dir := t.TempDir()
	t.Chdir(dir)
	t.Setenv(envName, "")
	return dir
}

// ---------
// SplitList
// ---------

func TestSplitList(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{
			name: "empty",
			in:   "",
			want: nil},
		{
			name: "only separators and spaces",
			in:   " , ,, ",
			want: nil},
		{
			name: "single",
			in:   "a", want: []string{"a"},
		},
		{
			name: "multiple",
			in:   "a,b,c",
			want: []string{"a", "b", "c"},
		},
		{
			name: "trims spaces",
			in:   "  a , b\t,\nc  ",
			want: []string{"a", "b", "c"},
		},
		{
			name: "drops empty items",
			in:   "a,,b,",
			want: []string{"a", "b"},
		},
		{
			name: "keeps inner spaces",
			in:   "a b, c d",
			want: []string{"a b", "c d"},
		},
		{
			name: "keeps duplicates and order",
			in:   "b,a,b",
			want: []string{"b", "a", "b"},
		},
	}
	for _, tt := range tests {
		t.Run(
			tt.name,
			func(t *testing.T) {
				assert.Equal(t, tt.want, SplitList(tt.in))
			},
		)
	}
}

// --------
// findPath
// --------

func TestFindPath_EnvAbsolutePath(t *testing.T) {
	isolate(t, "SVC_CONFIG_FILE")
	path := filepath.Join(t.TempDir(), "app.yaml")
	writeFile(t, path, "a: 1\n")
	t.Setenv("SVC_CONFIG_FILE", path)

	got, err := findPath("svc")
	require.NoError(t, err)
	assert.Equal(t, path, got)
}

func TestFindPath_EnvRelativePath(t *testing.T) {
	dir := isolate(t, "SVC_CONFIG_FILE")
	writeFile(t, filepath.Join(dir, "conf", "app.yaml"), "a: 1\n")
	t.Setenv("SVC_CONFIG_FILE", filepath.Join("conf", "app.yaml"))

	got, err := findPath("svc")
	require.NoError(t, err)
	assert.True(t, filepath.IsAbs(got))
	assertSamePath(t, filepath.Join(dir, "conf", "app.yaml"), got)
}

// TestFindPath_EnvUsesUpperCaseName checks that the env var name is in upper case,
// for a service name in mixed case.
func TestFindPath_EnvUsesUpperCaseName(t *testing.T) {
	isolate(t, "MYSVC_CONFIG_FILE")
	path := filepath.Join(t.TempDir(), "app.yaml")
	writeFile(t, path, "a: 1\n")
	t.Setenv("MYSVC_CONFIG_FILE", path)

	got, err := findPath("MySvc")
	require.NoError(t, err)
	assert.Equal(t, path, got)
}

// TestFindPath_EnvMissingFile checks that a bad env var value is an error.
// The search must not continue to the default candidates in this case.
func TestFindPath_EnvMissingFile(t *testing.T) {
	dir := isolate(t, "SVC_CONFIG_FILE")
	writeFile(t, filepath.Join(dir, ".prod.env"), "A=1\n")
	t.Setenv("SVC_CONFIG_FILE", filepath.Join(dir, "missing.yaml"))

	got, err := findPath("svc")
	require.Error(t, err)
	assert.Empty(t, got)
	assert.ErrorIs(t, err, os.ErrNotExist)
	assert.Contains(t, err.Error(), "SVC_CONFIG_FILE")
	assert.Contains(t, err.Error(), "missing.yaml")
}

func TestFindPath_CmdServiceFile(t *testing.T) {
	dir := isolate(t, "SVC_CONFIG_FILE")
	writeFile(t, filepath.Join(dir, "cmd", "svc", ".prod.env"), "A=1\n")

	got, err := findPath("svc")
	require.NoError(t, err)
	assertSamePath(t, filepath.Join(dir, "cmd", "svc", ".prod.env"), got)
}

// TestFindPath_CmdServiceFileLowerCase checks that the cmd folder name is in lower case,
// for a service name in mixed case.
func TestFindPath_CmdServiceFileLowerCase(t *testing.T) {
	dir := isolate(t, "SVC_CONFIG_FILE")
	writeFile(t, filepath.Join(dir, "cmd", "svc", ".prod.env"), "A=1\n")

	got, err := findPath("SVC")
	require.NoError(t, err)
	assertSamePath(t, filepath.Join(dir, "cmd", "svc", ".prod.env"), got)
}

func TestFindPath_WorkDirFile(t *testing.T) {
	dir := isolate(t, "SVC_CONFIG_FILE")
	writeFile(t, filepath.Join(dir, ".prod.env"), "A=1\n")

	got, err := findPath("svc")
	require.NoError(t, err)
	assertSamePath(t, filepath.Join(dir, ".prod.env"), got)
}

func TestFindPath_CmdServiceFileHasPriority(t *testing.T) {
	dir := isolate(t, "SVC_CONFIG_FILE")
	writeFile(t, filepath.Join(dir, "cmd", "svc", ".prod.env"), "A=cmd\n")
	writeFile(t, filepath.Join(dir, ".prod.env"), "A=root\n")

	got, err := findPath("svc")
	require.NoError(t, err)
	assertSamePath(t, filepath.Join(dir, "cmd", "svc", ".prod.env"), got)
}

func TestFindPath_EnvHasPriority(t *testing.T) {
	dir := isolate(t, "SVC_CONFIG_FILE")
	writeFile(t, filepath.Join(dir, "cmd", "svc", ".prod.env"), "A=cmd\n")
	path := filepath.Join(t.TempDir(), "app.yaml")
	writeFile(t, path, "a: env\n")
	t.Setenv("SVC_CONFIG_FILE", path)

	got, err := findPath("svc")
	require.NoError(t, err)
	assert.Equal(t, path, got)
}

// TestFindPath_OtherServiceFolderIgnored checks that the search uses only the folder of the given service.
func TestFindPath_OtherServiceFolderIgnored(t *testing.T) {
	dir := isolate(t, "SVC_CONFIG_FILE")
	writeFile(t, filepath.Join(dir, "cmd", "other", ".prod.env"), "A=1\n")

	_, err := findPath("svc")
	require.Error(t, err)
}

func TestFindPath_NotFound(t *testing.T) {
	isolate(t, "SVC_CONFIG_FILE")

	got, err := findPath("Svc")
	require.Error(t, err)
	assert.Empty(t, got)
	assert.Equal(t, "svc config not found; set SVC_CONFIG_FILE or provide cmd/svc/.prod.env", err.Error())
}

// TestFindPath_CandidateIsFolder checks that a folder with the candidate name is a match.
// findPath uses os.Stat only, so it does not check the file type.
func TestFindPath_CandidateIsFolder(t *testing.T) {
	dir := isolate(t, "SVC_CONFIG_FILE")
	require.NoError(t, os.MkdirAll(filepath.Join(dir, ".prod.env"), 0o755))

	got, err := findPath("svc")
	require.NoError(t, err)
	assertSamePath(t, filepath.Join(dir, ".prod.env"), got)
}

// TestFindPath_StatError checks that a stat error other than ErrNotExist stops the search.
func TestFindPath_StatError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root ignores folder permissions")
	}
	dir := isolate(t, "SVC_CONFIG_FILE")
	cmdDir := filepath.Join(dir, "cmd", "svc")
	writeFile(t, filepath.Join(cmdDir, ".prod.env"), "A=1\n")
	writeFile(t, filepath.Join(dir, ".prod.env"), "A=1\n")
	// A folder without the execute bit makes os.Stat of its children fail with EACCES.
	require.NoError(t, os.Chmod(cmdDir, 0o600))
	t.Cleanup(func() { _ = os.Chmod(cmdDir, 0o755) })

	got, err := findPath("svc")
	require.Error(t, err)
	assert.Empty(t, got)
	assert.ErrorIs(t, err, os.ErrPermission)
	assert.Contains(t, err.Error(), "inspect svc config")
}

// -------------
// defaultLoader
// -------------

// TestDefaultLoader_EnvOnly checks that an empty path gives a loader that reads only env vars.
func TestDefaultLoader_EnvOnly(t *testing.T) {
	t.Setenv("CONFIG_TEST_ENV_ONLY", "from-env")

	l, err := defaultLoader("")
	require.NoError(t, err)
	require.NotNil(t, l)
	assert.Equal(t, "from-env", l.GetString("config_test_env_only"))
	assert.True(t, l.IsSet("config_test_env_only"))
	assert.False(t, l.IsSet("config_test_absent_key"))
}

func TestDefaultLoader_FileNotFound(t *testing.T) {
	l, err := defaultLoader(filepath.Join(t.TempDir(), "missing.yaml"))
	require.Error(t, err)
	assert.Nil(t, l)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestDefaultLoader_InvalidContent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.yaml")
	writeFile(t, path, "a: [1, 2\n")

	l, err := defaultLoader(path)
	require.Error(t, err)
	assert.Nil(t, l)
}

// TestDefaultLoader_ExactFile checks that the loader reads the file at path.
// It must not read a different file with the same base name in the same folder.
func TestDefaultLoader_ExactFile(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "app.json"), `{"k":"json"}`)
	writeFile(t, filepath.Join(dir, "app.toml"), "k = \"toml\"\n")
	writeFile(t, filepath.Join(dir, "app.yaml"), "k: yaml\n")

	for _, ext := range []string{"json", "toml", "yaml"} {
		t.Run(
			ext,
			func(t *testing.T) {
				l, err := defaultLoader(filepath.Join(dir, "app."+ext))
				require.NoError(t, err)
				assert.Equal(t, ext, l.GetString("k"))
			},
		)
	}
}

// ----
// Load
// ----

func TestLoad_DotEnvFile(t *testing.T) {
	dir := isolate(t, "SVC_CONFIG_FILE")
	writeFile(t, filepath.Join(dir, "cmd", "svc", ".prod.env"), "APP_NAME=demo\nAPP_PORT=8080\n")

	l, err := Load("svc")
	require.NoError(t, err)
	assert.Equal(t, "demo", l.GetString("app_name"))
	assert.Equal(t, 8080, l.GetInt("app_port"))
}

func TestLoad_EnvOverridesFile(t *testing.T) {
	dir := isolate(t, "SVC_CONFIG_FILE")
	writeFile(t, filepath.Join(dir, ".prod.env"), "CONFIG_TEST_NAME=from-file\nCONFIG_TEST_KEEP=kept\n")
	t.Setenv("CONFIG_TEST_NAME", "from-env")

	l, err := Load("svc")
	require.NoError(t, err)
	assert.Equal(t, "from-env", l.GetString("config_test_name"))
	assert.Equal(t, "kept", l.GetString("config_test_keep"))
}

// TestLoad_KeysAreCaseInsensitive checks that a key in upper case in the file is found with a key in any case.
func TestLoad_KeysAreCaseInsensitive(t *testing.T) {
	dir := isolate(t, "SVC_CONFIG_FILE")
	writeFile(t, filepath.Join(dir, ".prod.env"), "APP_NAME=demo\n")

	l, err := Load("svc")
	require.NoError(t, err)
	assert.Equal(t, "demo", l.GetString("app_name"))
	assert.Equal(t, "demo", l.GetString("APP_NAME"))
	assert.Equal(t, "demo", l.GetString("App_Name"))
}

func TestLoad_TypedGetters(t *testing.T) {
	isolate(t, "SVC_CONFIG_FILE")
	path := filepath.Join(t.TempDir(), "app.yaml")
	// YAML does not accept tabs for indentation.
	// A slice of lines keeps the content correct when a formatter indents the code.
	writeFile(
		t,
		path,
		strings.Join([]string{
			"str: hello",
			"int: -42",
			"u8: 200",
			"u16: 60000",
			"u32: 4000000000",
			"u64: 18000000000000000000",
			"f64: 3.5",
			"flag: true",
			"at: 2026-01-02T03:04:05Z",
			"timeout: 1500ms",
			"zero: 0",
			`list: " a, b ,,c "`,
		},
			"\n")+"\n",
	)
	t.Setenv("SVC_CONFIG_FILE", path)

	l, err := Load("svc")
	require.NoError(t, err)

	assert.Equal(t, "hello", l.GetString("str"))
	assert.Equal(t, -42, l.GetInt("int"))
	assert.Equal(t, uint8(200), l.GetUint8("u8"))
	assert.Equal(t, uint16(60000), l.GetUint16("u16"))
	assert.Equal(t, uint32(4000000000), l.GetUint32("u32"))
	assert.Equal(t, uint64(18000000000000000000), l.GetUint64("u64"))
	assert.InDelta(t, 3.5, l.GetFloat64("f64"), 1e-9)
	assert.True(t, l.GetBool("flag"))
	assert.True(t, time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC).Equal(l.GetTime("at")))
	assert.Equal(t, 1500*time.Millisecond, l.GetDuration("timeout"))
	assert.Equal(t, []string{"a", "b", "c"}, SplitList(l.GetString("list")))
}

// TestLoad_IsSetZeroValue checks that IsSet tells an explicit zero value from an absent key.
func TestLoad_IsSetZeroValue(t *testing.T) {
	isolate(t, "SVC_CONFIG_FILE")
	path := filepath.Join(t.TempDir(), "app.yaml")
	writeFile(t, path, "zero: 0\nempty: \"\"\noff: false\n")
	t.Setenv("SVC_CONFIG_FILE", path)

	l, err := Load("svc")
	require.NoError(t, err)

	for _, key := range []string{"zero", "empty", "off"} {
		assert.True(t, l.IsSet(key), key)
	}
	assert.False(t, l.IsSet("config_test_absent_key"))

	assert.Zero(t, l.GetInt("zero"))
	assert.Zero(t, l.GetInt("config_test_absent_key"))
	assert.Empty(t, l.GetString("config_test_absent_key"))
	assert.False(t, l.GetBool("config_test_absent_key"))
	assert.Zero(t, l.GetDuration("config_test_absent_key"))
	assert.True(t, l.GetTime("config_test_absent_key").IsZero())
}

func TestLoad_NotFound(t *testing.T) {
	isolate(t, "SVC_CONFIG_FILE")

	l, err := Load("svc")
	require.Error(t, err)
	assert.Nil(t, l)
	assert.Contains(t, err.Error(), "svc config not found")
}

func TestLoad_InvalidContent(t *testing.T) {
	isolate(t, "SVC_CONFIG_FILE")
	path := filepath.Join(t.TempDir(), "app.json")
	writeFile(t, path, "{not json")
	t.Setenv("SVC_CONFIG_FILE", path)

	l, err := Load("svc")
	require.Error(t, err)
	assert.Nil(t, l)
}

// TestLoad_EnvFileExtension checks that Load reads the file in SVC_CONFIG_FILE.
// It must not read a file with the same base name and a different extension.
func TestLoad_EnvFileExtension(t *testing.T) {
	isolate(t, "SVC_CONFIG_FILE")
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "app.json"), `{"k":"json"}`)
	writeFile(t, filepath.Join(dir, "app.yaml"), "k: yaml\n")
	t.Setenv("SVC_CONFIG_FILE", filepath.Join(dir, "app.yaml"))

	l, err := Load("svc")
	require.NoError(t, err)
	assert.Equal(t, "yaml", l.GetString("k"))
}

// TestLoad_UnsupportedExtension checks that a file with an extension that viper does not know is an error.
func TestLoad_UnsupportedExtension(t *testing.T) {
	for _, name := range []string{"app.txt", "app"} {
		t.Run(
			name,
			func(t *testing.T) {
				isolate(t, "SVC_CONFIG_FILE")
				path := filepath.Join(t.TempDir(), name)
				writeFile(t, path, "a=1\n")
				t.Setenv("SVC_CONFIG_FILE", path)

				l, err := Load("svc")
				require.Error(t, err)
				assert.Nil(t, l)

				var unsupported viper.UnsupportedConfigError
				assert.True(t, errors.As(err, &unsupported))
			},
		)
	}
}
