package scheduler

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/dbid"
)

func slaItem(ws, recipient, taskID string, createdAt time.Time) helpSLAItem {
	return helpSLAItem{
		ID:            uuid.NewString(),
		WorkspaceID:   ws,
		RecipientType: "member",
		RecipientID:   recipient,
		TaskID:        taskID,
		CreatedAt:     createdAt,
	}
}

func TestBreachedHelpGroups_BoundaryIsInclusive(t *testing.T) {
	now := time.Now()
	ws := "11111111-1111-1111-1111-111111111111"
	ra := "aaaaaaaa-1111-1111-1111-111111111111"
	groups := map[helpDigestKey][]helpSLAItem{
		{WorkspaceID: ws, RecipientType: "member", RecipientID: ra}: {
			slaItem(ws, ra, "fresh", now.Add(-29*time.Minute)),
			slaItem(ws, ra, "exact", now.Add(-30*time.Minute)),
			slaItem(ws, ra, "old", now.Add(-31*time.Minute)),
		},
	}
	breached := breachedHelpGroups(groups, now, 30*time.Minute)
	items, ok := breached[helpDigestKey{WorkspaceID: ws, RecipientType: "member", RecipientID: ra}]
	if !ok {
		t.Fatalf("expected breached group, got %+v", breached)
	}
	got := map[string]bool{}
	for _, it := range items {
		got[it.TaskID] = true
	}
	if !got["exact"] || !got["old"] {
		t.Errorf("breached = %v, want exact+old (deadline inclusive)", got)
	}
	if got["fresh"] {
		t.Errorf("breached = %v, want fresh (29m) excluded", got)
	}
}

func TestBreachedHelpGroups_FreshGroupDropped(t *testing.T) {
	now := time.Now()
	groups := map[helpDigestKey][]helpSLAItem{
		{WorkspaceID: "ws", RecipientType: "member", RecipientID: "r"}: {
			slaItem("ws", "r", "t1", now.Add(-time.Minute)),
		},
	}
	if breached := breachedHelpGroups(groups, now, 30*time.Minute); len(breached) != 0 {
		t.Fatalf("breached = %d groups, want 0 (nothing past deadline)", len(breached))
	}
}

func TestWidenEscalationRecipients_DedupesAndSorts(t *testing.T) {
	got := widenEscalationRecipients("u1", []string{"u3", "u1", "u2", ""})
	want := []string{"u1", "u2", "u3"}
	if len(got) != len(want) {
		t.Fatalf("widened = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("widened = %v, want %v", got, want)
		}
	}
	if solo := widenEscalationRecipients("u1", nil); len(solo) != 1 || solo[0] != "u1" {
		t.Fatalf("widened without owners = %v, want [u1]", solo)
	}
}

func TestEscalationNeedsWrite_SameSetSkips(t *testing.T) {
	if escalationNeedsWrite([]string{"t2", "t1"}, []string{"t1", "t2"}) {
		t.Error("equal sets (reordered) should skip the write")
	}
	if !escalationNeedsWrite([]string{"t1"}, []string{"t1", "t2"}) {
		t.Error("grown breached set must trigger a rewrite")
	}
	if !escalationNeedsWrite(nil, []string{"t1"}) {
		t.Error("missing escalation row must trigger a write")
	}
	if escalationNeedsWrite([]string{"t1"}, []string{"t1"}) {
		t.Error("identical sets should skip the write")
	}
}

func TestStaleEscalationKeep_ResolvedTasksDropOut(t *testing.T) {
	keep := staleEscalationKeep([]string{"t1", "t2"}, map[string]bool{"t2": true, "t3": true})
	if len(keep) != 1 || keep[0] != "t2" {
		t.Fatalf("keep = %v, want [t2]", keep)
	}
	if keep := staleEscalationKeep([]string{"t1"}, map[string]bool{"t2": true}); len(keep) != 0 {
		t.Fatalf("keep = %v, want empty (row must clear)", keep)
	}
}

func TestTaskIDFromDetails_FallsBackToItemID(t *testing.T) {
	if got := taskIDFromDetails([]byte(`{"task_id":"task-1"}`), "item-9"); got != "task-1" {
		t.Errorf("task_id = %q, want task-1", got)
	}
	if got := taskIDFromDetails([]byte(`{"needs":["x"]}`), "item-9"); got != "item-9" {
		t.Errorf("missing task_id = %q, want item fallback", got)
	}
	if got := taskIDFromDetails([]byte("not-json"), "item-9"); got != "item-9" {
		t.Errorf("malformed details = %q, want item fallback", got)
	}
}

func TestHelpSLAFromEnv_DefaultAndOverride(t *testing.T) {
	t.Setenv("MULTICA_HELP_SLA_MINUTES", "")
	if got := HelpSLAFromEnv(); got != 30*time.Minute {
		t.Errorf("unset = %v, want 30m", got)
	}
	t.Setenv("MULTICA_HELP_SLA_MINUTES", "15")
	if got := HelpSLAFromEnv(); got != 15*time.Minute {
		t.Errorf("override = %v, want 15m", got)
	}
	for _, bad := range []string{"bogus", "0", "-5"} {
		t.Setenv("MULTICA_HELP_SLA_MINUTES", bad)
		if got := HelpSLAFromEnv(); got != 30*time.Minute {
			t.Errorf("invalid %q = %v, want 30m fallback", bad, got)
		}
	}
}

// TestHelpSLAJob_EscalatesAndClearsAcrossDB seeds one breached and one fresh
// help request, asserts only the breached one escalates, then resolves both
// and asserts the escalation clears. Skips unless Postgres is reachable.
func TestHelpSLAJob_EscalatesAndClearsAcrossDB(t *testing.T) {
	pool := integrationPool(t)
	ctx := context.Background()
	q := db.New(pool)

	ws := mustUUID(t, uuid.NewString())
	recipient := mustUUID(t, uuid.NewString())
	recipientStr := util.UUIDToString(recipient)

	if _, err := pool.Exec(ctx,
		`INSERT INTO workspace (id, name, slug) VALUES ($1, $2, $3)
		 ON CONFLICT (id) DO NOTHING`,
		ws, "help-sla-test", util.UUIDToString(ws)); err != nil {
		t.Fatalf("seed workspace: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM inbox_item WHERE workspace_id = $1`, ws)
		_, _ = pool.Exec(ctx, `DELETE FROM workspace WHERE id = $1`, ws)
	})

	seedHelp := func(taskID string) pgtype.UUID {
		det, err := json.Marshal(map[string]any{"task_id": taskID, "agent_id": "agent-" + taskID})
		if err != nil {
			t.Fatalf("marshal seed details: %v", err)
		}
		row, err := q.CreateInboxItem(ctx, db.CreateInboxItemParams{
			ID:            dbid.NewV7(),
			WorkspaceID:   ws,
			RecipientType: "member",
			RecipientID:   recipient,
			Type:          InboxItemTypeAgentHelpRequested,
			Severity:      "action_required",
			IssueID:       pgtype.UUID{},
			Title:         "Agent requested help",
			Body:          pgtype.Text{String: "blocked", Valid: true},
			ActorType:     pgtype.Text{String: "agent", Valid: true},
			ActorID:       ws,
			Details:       det,
		})
		if err != nil {
			t.Fatalf("seed help item %s: %v", taskID, err)
		}
		return row.ID
	}
	oldID := seedHelp("task-old")
	seedHelp("task-fresh")
	// Age only the first row past the 30m default.
	if _, err := pool.Exec(ctx,
		`UPDATE inbox_item SET created_at = now() - interval '1 hour' WHERE id = $1`, oldID); err != nil {
		t.Fatalf("backdate breached item: %v", err)
	}

	res, err := RunHelpSLAAt(ctx, pool, time.Now(), 30*time.Minute)
	if err != nil {
		t.Fatalf("RunHelpSLAAt: %v", err)
	}
	if res.Result["breached"] != int64(1) {
		t.Fatalf("breached = %v, want 1", res.Result["breached"])
	}

	var n int64
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM inbox_item
		 WHERE type = $1 AND archived = false AND workspace_id = $2 AND recipient_id = $3`,
		InboxItemTypeAgentHelpEscalated, ws, recipient).Scan(&n); err != nil {
		t.Fatalf("count escalations: %v", err)
	}
	if n != 1 {
		t.Fatalf("escalation rows = %d, want 1 (no owners seeded, original recipient only)", n)
	}
	var raw []byte
	if err := pool.QueryRow(ctx,
		`SELECT details FROM inbox_item
		 WHERE type = $1 AND archived = false AND workspace_id = $2 AND recipient_id = $3 LIMIT 1`,
		InboxItemTypeAgentHelpEscalated, ws, recipient).Scan(&raw); err != nil {
		t.Fatalf("load escalation: %v", err)
	}
	var det HelpEscalationDetails
	if err := json.Unmarshal(raw, &det); err != nil {
		t.Fatalf("decode escalation: %v", err)
	}
	if det.BreachedCount != 1 || len(det.TaskIDs) != 1 || det.TaskIDs[0] != "task-old" {
		t.Errorf("escalation tasks = %+v, want only [task-old]", det)
	}
	if det.SLAMinutes != 30 || det.OriginalRecipient != recipientStr {
		t.Errorf("escalation meta = %+v, want sla 30 + original recipient", det)
	}

	// Idempotency: a second tick with no change writes nothing new.
	before := res.RowsAffected
	res2, err := RunHelpSLAAt(ctx, pool, time.Now(), 30*time.Minute)
	if err != nil {
		t.Fatalf("RunHelpSLAAt rerun: %v", err)
	}
	_ = before
	if res2.Result["skipped"] != int64(1) {
		t.Errorf("rerun skipped = %v, want 1 (unchanged breached set)", res2.Result["skipped"])
	}
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM inbox_item
		 WHERE type = $1 AND archived = false AND workspace_id = $2 AND recipient_id = $3`,
		InboxItemTypeAgentHelpEscalated, ws, recipient).Scan(&n); err != nil {
		t.Fatalf("recount escalations: %v", err)
	}
	if n != 1 {
		t.Errorf("escalation rows after rerun = %d, want 1 (no churn)", n)
	}

	// Resolve: archive sources, escalation must clear like the digest does.
	if _, err := pool.Exec(ctx,
		`UPDATE inbox_item SET archived = true WHERE type = $1 AND workspace_id = $2 AND recipient_id = $3`,
		InboxItemTypeAgentHelpRequested, ws, recipient); err != nil {
		t.Fatalf("archive sources: %v", err)
	}
	if _, err := RunHelpSLAAt(ctx, pool, time.Now(), 30*time.Minute); err != nil {
		t.Fatalf("RunHelpSLAAt after resolve: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM inbox_item
		 WHERE type = $1 AND archived = false AND workspace_id = $2 AND recipient_id = $3`,
		InboxItemTypeAgentHelpEscalated, ws, recipient).Scan(&n); err != nil {
		t.Fatalf("recount after resolve: %v", err)
	}
	if n != 0 {
		t.Fatalf("escalation rows after resolve = %d, want 0 (cleared)", n)
	}
}
