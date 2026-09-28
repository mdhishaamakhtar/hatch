package crypto

import (
	"bytes"
	"encoding/base64"
	"testing"

	"github.com/tink-crypto/tink-go/v2/aead"
	"github.com/tink-crypto/tink-go/v2/insecurecleartextkeyset"
	"github.com/tink-crypto/tink-go/v2/keyset"
)

func newCipher(t *testing.T) *Cipher {
	t.Helper()
	handle, err := keyset.NewHandle(aead.AES256GCMKeyTemplate())
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := insecurecleartextkeyset.Write(handle, keyset.NewJSONWriter(&buf)); err != nil {
		t.Fatal(err)
	}
	c, err := New(base64.StdEncoding.EncodeToString(buf.Bytes()))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

var (
	clientA = bytes.Repeat([]byte{0xa}, 16)
	clientB = bytes.Repeat([]byte{0xb}, 16)
	creds   = []byte(`{"api_key":"re_secret_123"}`)
)

func TestRoundTrip(t *testing.T) {
	c := newCipher(t)
	sealed, err := c.Encrypt(creds, clientA, "resend")
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if bytes.Contains(sealed, []byte("re_secret_123")) {
		t.Fatal("ciphertext contains the plaintext")
	}
	got, err := c.Decrypt(sealed, clientA, "resend")
	if err != nil || !bytes.Equal(got, creds) {
		t.Fatalf("Decrypt = %q, %v; want %q", got, err, creds)
	}
}

// Credentials are bound to the client and vendor they were stored for, so a
// row whose ciphertext was copied from elsewhere cannot be decrypted.
func TestCiphertextOnlyOpensForItsClientAndVendor(t *testing.T) {
	c := newCipher(t)
	sealed, err := c.Encrypt(creds, clientA, "resend")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Decrypt(sealed, clientB, "resend"); err == nil {
		t.Error("decrypted with another client's id")
	}
	if _, err := c.Decrypt(sealed, clientA, "mock"); err == nil {
		t.Error("decrypted under another vendor")
	}
	if _, err := newCipher(t).Decrypt(sealed, clientA, "resend"); err == nil {
		t.Error("decrypted with a different keyset")
	}
}

func TestNewRejectsBadKeysets(t *testing.T) {
	for _, bad := range []string{"", "!!!not-base64", base64.StdEncoding.EncodeToString([]byte("not a keyset"))} {
		if _, err := New(bad); err == nil {
			t.Errorf("New(%q) succeeded", bad)
		}
	}
}
