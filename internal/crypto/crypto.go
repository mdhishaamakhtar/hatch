// Package crypto encrypts provider credentials. The API encrypts them before
// storing them in client_providers, and the delivery worker decrypts them just
// before a send; both go through this one Cipher.
package crypto

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/tink-crypto/tink-go/v2/aead"
	"github.com/tink-crypto/tink-go/v2/insecurecleartextkeyset"
	"github.com/tink-crypto/tink-go/v2/keyset"
	"github.com/tink-crypto/tink-go/v2/tink"
)

// Cipher seals and opens provider credentials with a Tink AEAD keyset.
type Cipher struct {
	aead tink.AEAD
}

// New loads a base64-encoded Tink keyset in JSON form, as printed by
// cmd/tinkgen.
func New(keysetB64 string) (*Cipher, error) {
	if keysetB64 == "" {
		return nil, errors.New("empty keyset")
	}
	raw, err := base64.StdEncoding.DecodeString(keysetB64)
	if err != nil {
		return nil, fmt.Errorf("decode keyset: %w", err)
	}
	handle, err := insecurecleartextkeyset.Read(keyset.NewJSONReader(bytes.NewReader(raw)))
	if err != nil {
		return nil, fmt.Errorf("read keyset: %w", err)
	}
	a, err := aead.New(handle)
	if err != nil {
		return nil, err
	}
	return &Cipher{aead: a}, nil
}

// Encrypt seals one client's credentials for one vendor. The client and vendor
// are bound to the ciphertext as associated data, so it only opens for that same
// pair: credentials copied onto another client's row cannot be used by it.
func (c *Cipher) Encrypt(creds, clientID []byte, vendor string) ([]byte, error) {
	return c.aead.Encrypt(creds, associatedData(clientID, vendor))
}

// Decrypt opens credentials sealed by Encrypt for the same client and vendor.
func (c *Cipher) Decrypt(sealed, clientID []byte, vendor string) ([]byte, error) {
	return c.aead.Decrypt(sealed, associatedData(clientID, vendor))
}

// associatedData is unambiguous because client ids are always 16 bytes.
func associatedData(clientID []byte, vendor string) []byte {
	return append(bytes.Clone(clientID), vendor...)
}
