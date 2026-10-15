package sep54

// Tip is the v1 wire format of the lake's GET /v1/tip. All fields
// are null while the lake is empty. The protocol only ever gains fields,
// because lake and ingest may run different binary versions.
type Tip struct {
	// Tip is the newest committed ledger. It never moves backwards.
	Tip *uint32 `json:"tip"`
	// Oldest is the oldest ledger the lake is expected to hold. Ledgers
	// between Oldest and Tip may still be missing (holes).
	Oldest *uint32 `json:"oldest"`
	// TipCommittedAt is when Tip was committed, in Unix nanoseconds. For a
	// tip loaded from disk at lake startup it is the object's mtime.
	TipCommittedAt *int64 `json:"tip_committed_at"`
}
