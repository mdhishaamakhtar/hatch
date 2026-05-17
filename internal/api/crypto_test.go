package api

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/tink-crypto/tink-go/v2/aead"
	"github.com/tink-crypto/tink-go/v2/insecurecleartextkeyset"
	"github.com/tink-crypto/tink-go/v2/keyset"
)

func freshKeysetB64(t *testing.T) string {
	t.Helper()
	kh, err := keyset.NewHandle(aead.AES256GCMKeyTemplate())
	if err != nil {
		t.Fatalf("keyset handle: %v", err)
	}
	var buf bytes.Buffer
	if err := insecurecleartextkeyset.Write(kh, keyset.NewJSONWriter(&buf)); err != nil {
		t.Fatalf("write keyset: %v", err)
	}
	return base64.StdEncoding.EncodeToString(buf.Bytes())
}

func TestEncryptCredentialsRoundtrip(t *testing.T) {
	cipher, err := LoadCipher(freshKeysetB64(t))
	if err != nil {
		t.Fatalf("load cipher: %v", err)
	}
	plaintext := []byte(`{"api_key":"re_secret_123"}`)
	out, err := cipher.EncryptCredentials(plaintext)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	// Envelope shape.
	var env encEnvelope
	if err := json.Unmarshal(out, &env); err != nil {
		t.Fatalf("envelope decode: %v", err)
	}
	if env.V != 1 {
		t.Fatalf("version: want 1, got %d", env.V)
	}
	if env.Ct == "" {
		t.Fatal("ciphertext empty")
	}
	if strings.Contains(string(out), "re_secret_123") {
		t.Fatalf("ciphertext leaked plaintext: %s", string(out))
	}

	// Same plaintext encrypts to a different ciphertext (nonce randomness).
	out2, err := cipher.EncryptCredentials(plaintext)
	if err != nil {
		t.Fatalf("encrypt2: %v", err)
	}
	if bytes.Equal(out, out2) {
		t.Fatal("encryption should produce a fresh nonce each call")
	}
}

func TestLoadCipherErrors(t *testing.T) {
	if _, err := LoadCipher(""); err == nil {
		t.Fatal("empty key should error")
	}
	if _, err := LoadCipher("!!!notbase64!!!"); err == nil {
		t.Fatal("non-base64 should error")
	}
	if _, err := LoadCipher(base64.StdEncoding.EncodeToString([]byte("not-a-keyset"))); err == nil {
		t.Fatal("invalid keyset JSON should error")
	}
}
