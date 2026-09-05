package service

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

// G4 (#57) CAS/fencing: dispatched_at is the fencing epoch / idempotency key
// on autonomous claim-path mutations. These unit tests pin the epoch
// comparison semantics without needing a database.
func TestSameFencingEpoch(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	epoch := pgtype.Timestamptz{Time: now, Valid: true}
	same := pgtype.Timestamptz{Time: now, Valid: true}
	later := pgtype.Timestamptz{Time: now.Add(time.Second), Valid: true}
	invalid := pgtype.Timestamptz{}

	if !SameFencingEpoch(epoch, same) {
		t.Fatal("identical epochs must compare equal")
	}
	if SameFencingEpoch(epoch, later) {
		t.Fatal("reclaimed epoch (new dispatched_at) must not match the stale one")
	}
	if SameFencingEpoch(epoch, invalid) {
		t.Fatal("invalid current epoch must never compare equal")
	}
	if SameFencingEpoch(invalid, epoch) {
		t.Fatal("invalid observed epoch must never compare equal (legacy unfenced path)")
	}
	if SameFencingEpoch(invalid, invalid) {
		t.Fatal("two invalid epochs must never compare equal")
	}
}
