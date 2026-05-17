package api

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

// CredentialCipher wraps a Tink AEAD primitive for provider credentials.
//
// Stored payload shape (JSONB): { "v": 1, "ct": "<base64 ciphertext>" }.
// Decryption lives in the delivery worker (Phase 3); the API only encrypts.
type CredentialCipher struct {
	aead tink.AEAD
}

// LoadCipher parses a base64-encoded Tink keyset JSON (as produced by
// cmd/tinkgen) and returns a primitive ready to Encrypt.
func LoadCipher(b64 string) (*CredentialCipher, error) {
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
	return &CredentialCipher{aead: a}, nil
}

type encEnvelope struct {
	V  int    `json:"v"`
	Ct string `json:"ct"`
}

// EncryptCredentials encrypts the given plaintext (already JSON-encoded
// credentials object) and returns the JSONB-ready envelope bytes.
func (c *CredentialCipher) EncryptCredentials(plaintext []byte) ([]byte, error) {
	ct, err := c.aead.Encrypt(plaintext, nil)
	if err != nil {
		return nil, fmt.Errorf("aead encrypt: %w", err)
	}
	env := encEnvelope{V: 1, Ct: base64.StdEncoding.EncodeToString(ct)}
	return json.Marshal(env)
}
