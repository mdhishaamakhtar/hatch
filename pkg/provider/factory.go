package provider

// Factory builds a Provider for a single client from that client's decrypted
// credentials JSON (the plaintext stored encrypted in client_providers.credentials).
// One Factory is registered per vendor at Delivery Worker startup; the Provider
// Router calls it lazily the first time it routes a send for a (client, vendor)
// pair and caches the result.
type Factory func(creds []byte) (Provider, error)

// MockFactory returns a Factory that ignores credentials and yields a
// MockProvider tuned by cfg. Used by the Delivery Worker for vendor "mock".
func MockFactory(cfg MockConfig) Factory {
	return func(_ []byte) (Provider, error) {
		return NewMockProvider(cfg), nil
	}
}
