package crypto

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

	// Decrypt recovers the exact plaintext for both ciphertexts.
	for _, ct := range [][]byte{out, out2} {
		got, err := cipher.DecryptCredentials(ct)
		if err != nil {
			t.Fatalf("decrypt: %v", err)
		}
		if !bytes.Equal(got, plaintext) {
			t.Fatalf("decrypt roundtrip mismatch: got %s want %s", got, plaintext)
		}
	}
}

func TestDecryptCredentialsErrors(t *testing.T) {
	cipher, err := LoadCipher(freshKeysetB64(t))
	if err != nil {
		t.Fatalf("load cipher: %v", err)
	}
	if _, err := cipher.DecryptCredentials([]byte("not-json")); err == nil {
		t.Fatal("malformed envelope should error")
	}
	if _, err := cipher.DecryptCredentials([]byte(`{"v":2,"ct":"AAAA"}`)); err == nil {
		t.Fatal("unsupported version should error")
	}
	if _, err := cipher.DecryptCredentials([]byte(`{"v":1,"ct":"!!notb64"}`)); err == nil {
		t.Fatal("non-base64 ciphertext should error")
	}
	// Ciphertext from a different keyset must fail authentication.
	other, _ := LoadCipher(freshKeysetB64(t))
	enc, _ := other.EncryptCredentials([]byte(`{"api_key":"x"}`))
	if _, err := cipher.DecryptCredentials(enc); err == nil {
		t.Fatal("decrypt with wrong key should error")
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
