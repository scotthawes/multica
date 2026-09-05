package handler

import (
	"testing"
	"time"
)

// G4 (#57) fencing epoch parsing: the daemon echoes the claim response's
// dispatched_at (RFC3339Nano, microsecond-precise) as the idempotency-key /
// fencing epoch on autonomous mutations (start / park / extend-lease).
func TestParseFencingEpoch(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	raw := now.Format(time.RFC3339)

	epoch, err := parseFencingEpoch(raw)
	if err != nil {
		t.Fatalf("parse valid RFC3339: %v", err)
	}
	if !epoch.Valid || !epoch.Time.Equal(now) {
		t.Fatalf("parsed epoch = %+v, want %v valid", epoch, now)
	}

	if _, err := parseFencingEpoch("not-a-time"); err == nil {
		t.Fatal("malformed dispatched_at must fail closed, not downgrade to unfenced")
	}
}

func TestParseOptionalFencingEpochBody(t *testing.T) {
	// Empty body = legacy daemon, unfenced.
	epoch, err := parseOptionalFencingEpochBody(nil)
	if err != nil || epoch.Valid {
		t.Fatalf("empty body must be valid-unfenced, got %+v err %v", epoch, err)
	}

	// Body without the field = legacy daemon, unfenced.
	epoch, err = parseOptionalFencingEpochBody([]byte(`{"other":1}`))
	if err != nil || epoch.Valid {
		t.Fatalf("missing field must be valid-unfenced, got %+v err %v", epoch, err)
	}

	// Present epoch parses.
	now := time.Now().UTC().Truncate(time.Second)
	epoch, err = parseOptionalFencingEpochBody([]byte(`{"dispatched_at":` + strconvQuote(now.Format(time.RFC3339)) + `}`))
	if err != nil || !epoch.Valid {
		t.Fatalf("present epoch must parse, got %+v err %v", epoch, err)
	}

	// Present-but-malformed fails closed.
	if _, err := parseOptionalFencingEpochBody([]byte(`{"dispatched_at":"yesterday-ish"}`)); err == nil {
		t.Fatal("malformed dispatched_at must error, not silently unfence")
	}
}

func strconvQuote(s string) string {
	return `"` + s + `"`
}
