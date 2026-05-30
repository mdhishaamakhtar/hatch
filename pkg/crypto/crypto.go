// Package crypto wraps a Tink AEAD primitive for provider-credential
// encryption. The Scheduler API encrypts credentials before persisting them to
// client_providers.credentials; the Delivery Worker decrypts them at send time.
// Both sides share this one implementation so the envelope format can never
// drift between writer and reader.
package crypto

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/tink-crypto/tink-go/v2/aead"
	"github.com/tink-crypto/tink-go/v2/insecurecleartextkeyset"
	"github.com/tink-crypto/tink-go/v2/keyset"
	"github.com/tink-crypto/tink-go/v2/tink"
)

// Cipher wraps a Tink AEAD primitive for provider credentials.
//
// Stored payload shape (JSONB): { "v": 1, "ct": "<base64 ciphertext>" }.
type Cipher struct {
	aead tink.AEAD
}

// LoadCipher parses a base64-encoded Tink keyset JSON (as produced by
// cmd/tinkgen) and returns a primitive ready to Encrypt and Decrypt.
func LoadCipher(b64 string) (*Cipher, error) {
	if b64 == "" {
		return nil, fmt.Errorf("provider cred key empty")
	}
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, fmt.Errorf("decode keyset b64: %w", err)
	}
	ks, err := insecurecleartextkeyset.Read(keyset.NewJSONReader(bytes.NewReader(raw)))
	if err != nil {
		return nil, fmt.Errorf("parse keyset: %w", err)
	}
	a, err := aead.New(ks)
	if err != nil {
		return nil, fmt.Errorf("aead primitive: %w", err)
	}
	return &Cipher{aead: a}, nil
}

type encEnvelope struct {
	V  int    `json:"v"`
	Ct string `json:"ct"`
}

// EncryptCredentials encrypts the given plaintext (already JSON-encoded
// credentials object) and returns the JSONB-ready envelope bytes.
func (c *Cipher) EncryptCredentials(plaintext []byte) ([]byte, error) {
	ct, err := c.aead.Encrypt(plaintext, nil)
	if err != nil {
		return nil, fmt.Errorf("aead encrypt: %w", err)
	}
	env := encEnvelope{V: 1, Ct: base64.StdEncoding.EncodeToString(ct)}
	return json.Marshal(env)
}

// DecryptCredentials reverses EncryptCredentials: it parses the JSONB envelope
// and returns the original plaintext credentials JSON.
func (c *Cipher) DecryptCredentials(envelope []byte) ([]byte, error) {
	var env encEnvelope
	if err := json.Unmarshal(envelope, &env); err != nil {
		return nil, fmt.Errorf("decode envelope: %w", err)
	}
	if env.V != 1 {
		return nil, fmt.Errorf("unsupported credential envelope version %d", env.V)
	}
	ct, err := base64.StdEncoding.DecodeString(env.Ct)
	if err != nil {
		return nil, fmt.Errorf("decode ciphertext b64: %w", err)
	}
	pt, err := c.aead.Decrypt(ct, nil)
	if err != nil {
		return nil, fmt.Errorf("aead decrypt: %w", err)
	}
	return pt, nil
}
