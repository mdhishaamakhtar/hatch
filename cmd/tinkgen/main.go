// tinkgen prints a base64-encoded Tink AES256-GCM keyset JSON on stdout.
// Used to seed the PROVIDER_CRED_KEY env var (Phase 1 admin/provider
// credentials encryption).
//
//	go run ./cmd/tinkgen
package main

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"os"

	"github.com/tink-crypto/tink-go/v2/aead"
	"github.com/tink-crypto/tink-go/v2/insecurecleartextkeyset"
	"github.com/tink-crypto/tink-go/v2/keyset"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "tinkgen:", err)
		os.Exit(1)
	}
}

func run() error {
	kh, err := keyset.NewHandle(aead.AES256GCMKeyTemplate())
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	if err := insecurecleartextkeyset.Write(kh, keyset.NewJSONWriter(&buf)); err != nil {
		return err
	}
	fmt.Println(base64.StdEncoding.EncodeToString(buf.Bytes()))
	return nil
}
