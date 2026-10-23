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

// Provision a service key once, outside pod startup. See
// docs/protocol/noise-v2.md for deployment and rotation instructions.
package main

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"

	hubnoise "xxx/internal/hub/infrastructure/noise"
	"xxx/internal/shared/hubendpoint"

	"github.com/flynn/noise"
)

func main() {
	err := run(os.Args[1:], os.Stdout, os.Stderr)
	if err != nil && !errors.Is(err, flag.ErrHelp) {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, output, errorOutput io.Writer) error {
	flags := flag.NewFlagSet("gen-noise-key", flag.ContinueOnError)
	flags.SetOutput(errorOutput)

	address := flags.String("address", "", "client-reachable host:port of the Hub service")
	audience := flags.String("audience", "", "Hub service audience")
	id := flags.Uint64("key-id", 1, "nonzero uint32 static key ID")
	privatePath := flags.String("private-out", "", "new private keyring JSON file (0600)")
	publicPath := flags.String("public-out", "", "new public descriptor JSON file")

	if err := flags.Parse(args); err != nil {
		return err
	}

	if flags.NArg() != 0 || *id == 0 || *id > math.MaxUint32 || *privatePath == "" || *publicPath == "" {
		return fmt.Errorf("provide -address, -audience, -private-out, -public-out and a key ID in 1..4294967295")
	}

	privateAbs, err := filepath.Abs(*privatePath)
	if err != nil {
		return err
	}

	publicAbs, err := filepath.Abs(*publicPath)
	if err != nil {
		return err
	}

	if privateAbs == publicAbs {
		return fmt.Errorf("private and public output paths must be different")
	}

	pair, err := noise.DH25519.GenerateKeypair(nil)
	if err != nil {
		return fmt.Errorf("generate Noise static key: %w", err)
	}
	defer clear(pair.Private)

	descriptor := hubendpoint.Descriptor{
		Address:   *address,
		Audience:  *audience,
		KeyID:     uint32(*id),
		PublicKey: pair.Public,
		Protocol:  hubendpoint.Protocol,
		Version:   hubendpoint.WireVersion,
	}
	if err = descriptor.Validate(); err != nil {
		return fmt.Errorf("validate descriptor: %w", err)
	}

	privateData, err := json.Marshal(
		hubnoise.KeyFile{
			Keys: []hubnoise.KeyEntry{
				{
					KeyID:      uint32(*id),
					PrivateKey: pair.Private,
					PublicKey:  pair.Public,
				},
			},
		},
		jsontext.WithIndent("  "),
	)
	if err != nil {
		return fmt.Errorf("encode private keyring")
	}
	defer clear(privateData)

	publicData, err := json.Marshal(descriptor, jsontext.WithIndent("  "))
	if err != nil {
		return fmt.Errorf("encode public descriptor")
	}

	if err = os.MkdirAll(filepath.Dir(privateAbs), 0o700); err != nil {
		return fmt.Errorf("create private output directory: %w", err)
	}

	if err = os.MkdirAll(filepath.Dir(publicAbs), 0o755); err != nil {
		return fmt.Errorf("create public output directory: %w", err)
	}

	if err = createExclusive(privateAbs, privateData, 0o600); err != nil {
		return fmt.Errorf("create private keyring: %w", err)
	}

	complete := false
	defer func() {
		if !complete {
			_ = os.Remove(privateAbs) // only the file exclusively published above
		}
	}()
	if err = createExclusive(publicAbs, publicData, 0o644); err != nil {
		return fmt.Errorf("create public descriptor: %w", err)
	}
	complete = true
	_, err = fmt.Fprintf(
		output,
		"Created Hub key %d. Private keyring: %s\nPublic descriptor (IAM): %s\n",
		*id, privateAbs, publicAbs,
	)
	return err
}

// createExclusive publishes a fully written file without replacing an existing
// destination. It requires hard-link support in the destination filesystem.
func createExclusive(path string, data []byte, mode os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".noise-key-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if err = writeFile(tmp, data, mode); err != nil {
		return err
	}
	return os.Link(tmp.Name(), path)
}

// Close before publishing or removing the temporary file, including on failure.
func writeFile(file *os.File, data []byte, mode os.FileMode) (err error) {
	defer func() {
		if closeErr := file.Close(); err == nil {
			err = closeErr
		}
	}()
	if err = file.Chmod(mode); err != nil {
		return err
	}
	if _, err = file.Write(data); err != nil {
		return err
	}
	if _, err = file.WriteString("\n"); err != nil {
		return err
	}
	return file.Sync()
}
