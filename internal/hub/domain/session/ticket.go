package session

// Ticket is the state that used to live in a pending session. It is sealed
// (AES-GCM) with a cluster-wide key, so any hub can open it.
type Ticket struct {
	DeviceID    uint64
	ExpiresAt   int64 // unix seconds
	Keys        Keys
	IdentityPub [32]byte // ed25519 public key
}

type TicketSealer interface {
	Seal(t *Ticket) ([]byte, error)
	// Open authenticates the ticket against deviceID (used as AAD) and checks expiry.
	Open(deviceID uint64, ticket []byte) (*Ticket, error)
}
