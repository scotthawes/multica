package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/dbid"
)

// SLA escalation for unattended agent help requests (G2, issue #54).
//
// The GAP-2 digest (jobs_help_digest.go) rolls open agent_help_requested
// items into one attention item per (workspace, recipient) and clears it on
// resolve — but nothing fires when a request sits unattended. This job adds
// the deadline: any open help request older than the SLA (default 30m) is
// escalated with a widened recipient set (original recipient plus workspace
// owners/admins), a high-severity alert row, and counts on the audit row.
// Clearing reuses the digest's resolve semantics: once the source help item
// is archived, its escalation rows are removed on the next tick.

// InboxItemTypeAgentHelpEscalated is the alert row this job maintains: one
// open row per (workspace, recipient, original_recipient) summarizing the
// breached help requests. It is distinct from agent_help_digest so clients
// can render the breach differently and so the digest reconciler (which only
// touches its own type) never interferes.
const InboxItemTypeAgentHelpEscalated = "agent_help_escalated"

// JobNameHelpSLA is the canonical audit-row name. Stable across
// releases — do not rename without a migration.
const JobNameHelpSLA = "help_sla"

// helpSLADefault is the breach deadline. 30m matches the issue spec; it is
// the longest a help request may sit open before escalation, and can be
// overridden per-environment via MULTICA_HELP_SLA_MINUTES.
const helpSLADefault = 30 * time.Minute

// helpSLADefaultInterval is how often the SLA is evaluated. 5m keeps breach
// detection prompt relative to the 30m deadline without churning the inbox:
// ticks whose breached set is unchanged write nothing (see
// escalationNeedsWrite).
const helpSLADefaultInterval = 5 * time.Minute

// helpSLAEnvVar overrides the breach deadline in whole minutes.
const helpSLAEnvVar = "MULTICA_HELP_SLA_MINUTES"

// HelpSLAFromEnv returns the breach deadline, defaulting to 30m. Garbage
// (unparsable, zero, negative) falls back to the default so a typo in the
// environment cannot disable escalation or make every fresh request breach.
func HelpSLAFromEnv() time.Duration {
	raw := strings.TrimSpace(os.Getenv(helpSLAEnvVar))
	if raw == "" {
		return helpSLADefault
	}
	mins, err := strconv.Atoi(raw)
	if err != nil || mins <= 0 {
		return helpSLADefault
	}
	return time.Duration(mins) * time.Minute
}

// HelpEscalationDetails is the jsonb payload of an agent_help_escalated
// inbox item.
type HelpEscalationDetails struct {
	SLAMinutes        int      `json:"sla_minutes"`
	BreachedCount     int      `json:"breached_count"`
	TaskIDs           []string `json:"task_ids"`
	OriginalRecipient string   `json:"original_recipient"`
	EscalatedAt       string   `json:"escalated_at"`
}

// helpSLAItem is the open-help row subset the SLA needs: identity for
// grouping, task linkage for idempotency, and age for the breach check.
type helpSLAItem struct {
	ID            string
	WorkspaceID   string
	RecipientType string
	RecipientID   string
	TaskID        string
	CreatedAt     time.Time
}

// openEscalation is one open agent_help_escalated row, keyed by
// (workspace, recipient, original_recipient).
type openEscalation struct {
	WorkspaceID       string
	RecipientType     string
	RecipientID       string
	OriginalRecipient string
	TaskIDs           []string
}

type escalationKey struct {
	WorkspaceID       string
	RecipientID       string
	OriginalRecipient string
}

// HelpSLAJob returns the JobSpec that escalates unattended
// agent_help_requested inbox items past the SLA. The job is idempotent:
// ticks with an unchanged breached set write nothing.
func HelpSLAJob(pool *pgxpool.Pool) JobSpec {
	return JobSpec{
		Name:              JobNameHelpSLA,
		Cadence:           helpSLADefaultInterval,
		ScheduleDelay:     time.Minute,
		CatchUpMode:       CatchUpLatestOnly, // breach state derives from current open rows
		CatchUpWindow:     24 * time.Hour,
		RunTimeout:        2 * time.Minute,
		StaleTimeout:      10 * time.Minute,
		HeartbeatInterval: 30 * time.Second,
		AllowStaleReentry: true, // delete+insert per (workspace, recipient, origin) is idempotent
		MaxAttempts:       3,
		RetryBackoff: []time.Duration{
			5 * time.Minute,
			30 * time.Minute,
		},
		Scopes: StaticScopes(ScopeGlobal),
		Handler: func(ctx context.Context, in HandlerInput) (HandlerResult, error) {
			return RunHelpSLA(ctx, pool)
		},
	}
}

// RunHelpSLA evaluates the SLA with the wall clock and the environment
// deadline.
func RunHelpSLA(ctx context.Context, pool *pgxpool.Pool) (HandlerResult, error) {
	return RunHelpSLAAt(ctx, pool, time.Now(), HelpSLAFromEnv())
}

// RunHelpSLAAt reconciles escalations against the open help items breaching
// the SLA at `now`. It ensures one escalation row per widened recipient of
// every breached group and deletes rows whose group is no longer breached
// (sources resolved), so escalations clear on resolve like digests do.
func RunHelpSLAAt(ctx context.Context, pool *pgxpool.Pool, now time.Time, sla time.Duration) (HandlerResult, error) {
	if sla <= 0 {
		sla = helpSLADefault
	}
	open, err := listOpenHelpForSLA(ctx, pool)
	if err != nil {
		return HandlerResult{}, fmt.Errorf("help_sla: list open help items: %w", err)
	}
	groups := groupSLAItems(open)
	breached := breachedHelpGroups(groups, now, sla)

	var breachedItems int64
	for _, items := range breached {
		breachedItems += int64(len(items))
	}

	existing, err := listOpenEscalations(ctx, pool)
	if err != nil {
		return HandlerResult{}, fmt.Errorf("help_sla: list existing escalations: %w", err)
	}
	byKey := make(map[escalationKey][]string, len(existing))
	for _, e := range existing {
		k := escalationKey{WorkspaceID: e.WorkspaceID, RecipientID: e.RecipientID, OriginalRecipient: e.OriginalRecipient}
		byKey[k] = e.TaskIDs
	}

	ownersCache := map[string][]string{}
	ownersFor := func(ws string) []string {
		if owners, ok := ownersCache[ws]; ok {
			return owners
		}
		owners, err := listWorkspaceOwnerAdmins(ctx, pool, ws)
		if err != nil {
			slog.Warn("help_sla: list owners failed, escalating to original recipient only",
				"workspace_id", ws, "error", err)
			owners = nil
		}
		ownersCache[ws] = owners
		return owners
	}

	var escalated, skipped int64
	// Ensure one escalation row per widened recipient of every breached group.
	for key, items := range breached {
		widened := widenEscalationRecipients(key.RecipientID, ownersFor(key.WorkspaceID))
		want := slaTaskIDs(items)
		for _, recipient := range widened {
			ek := escalationKey{WorkspaceID: key.WorkspaceID, RecipientID: recipient, OriginalRecipient: key.RecipientID}
			if !escalationNeedsWrite(byKey[ek], want) {
				skipped++
				continue
			}
			if err := upsertEscalationForRecipient(ctx, pool, key, recipient, items, now, sla); err != nil {
				return HandlerResult{}, fmt.Errorf("help_sla: escalate for %+v: %w", ek, err)
			}
			escalated++
		}
	}

	// Clear rows whose group is no longer breached, or whose recipient fell
	// out of the widened set (e.g. an owner demoted since the escalation).
	var cleared int64
	for _, e := range existing {
		groupKey := helpDigestKey{WorkspaceID: e.WorkspaceID, RecipientType: e.RecipientType, RecipientID: e.OriginalRecipient}
		items, stillBreached := breached[groupKey]
		if !stillBreached {
			n, err := deleteOpenEscalation(ctx, pool, e)
			if err != nil {
				return HandlerResult{}, fmt.Errorf("help_sla: clear stale escalation for %+v: %w", e, err)
			}
			cleared += n
			continue
		}
		widened := widenEscalationRecipients(groupKey.RecipientID, ownersFor(e.WorkspaceID))
		_ = items
		stillWidened := false
		for _, r := range widened {
			if r == e.RecipientID {
				stillWidened = true
				break
			}
		}
		if !stillWidened {
			n, err := deleteOpenEscalation(ctx, pool, e)
			if err != nil {
				return HandlerResult{}, fmt.Errorf("help_sla: clear narrowed escalation for %+v: %w", e, err)
			}
			cleared += n
		}
	}

	slog.Info("help_sla: reconciled",
		"open_help", len(open), "breached", breachedItems,
		"groups", len(breached), "escalated", escalated,
		"skipped", skipped, "cleared", cleared,
		"sla_minutes", int(sla.Minutes()))
	return HandlerResult{
		RowsAffected: escalated + cleared,
		Result: map[string]any{
			"open_help":   int64(len(open)),
			"breached":    breachedItems,
			"groups":      int64(len(breached)),
			"escalated":   escalated,
			"skipped":     skipped,
			"cleared":     cleared,
			"sla_minutes": int64(sla.Minutes()),
		},
	}, nil
}

// groupSLAItems buckets open help items by (workspace, recipient), mirroring
// groupHelpItems for the digest. A NULL workspace normalizes to the global
// bucket (empty WorkspaceID).
func groupSLAItems(items []helpSLAItem) map[helpDigestKey][]helpSLAItem {
	out := make(map[helpDigestKey][]helpSLAItem, len(items))
	for _, it := range items {
		key := helpDigestKey{
			WorkspaceID:   it.WorkspaceID,
			RecipientType: it.RecipientType,
			RecipientID:   it.RecipientID,
		}
		out[key] = append(out[key], it)
	}
	return out
}

// breachedHelpGroups keeps, per group, only the items at or past the
// deadline (created_at <= now - sla). The boundary is inclusive: reaching
// the deadline counts as a breach. Groups with nothing breached are dropped,
// so the caller treats every surviving group as escalation-worthy. Pure: no
// DB, driven by the passed clock for testability.
func breachedHelpGroups(groups map[helpDigestKey][]helpSLAItem, now time.Time, sla time.Duration) map[helpDigestKey][]helpSLAItem {
	cutoff := now.Add(-sla)
	out := make(map[helpDigestKey][]helpSLAItem, len(groups))
	for key, items := range groups {
		var breached []helpSLAItem
		for _, it := range items {
			if !it.CreatedAt.After(cutoff) {
				breached = append(breached, it)
			}
		}
		if len(breached) > 0 {
			out[key] = breached
		}
	}
	return out
}

// widenEscalationRecipients returns the escalation audience: the original
// recipient first, then workspace owners/admins, deduplicated. Sorted after
// the first element so concurrent ticks produce the same write order. Pure.
func widenEscalationRecipients(original string, owners []string) []string {
	seen := map[string]bool{original: true}
	out := []string{original}
	rest := make([]string, 0, len(owners))
	for _, o := range owners {
		if o == "" || seen[o] {
			continue
		}
		seen[o] = true
		rest = append(rest, o)
	}
	sort.Strings(rest)
	return append(out, rest...)
}

// slaTaskIDs returns the sorted task identities of breached items. Pure.
func slaTaskIDs(items []helpSLAItem) []string {
	ids := make([]string, 0, len(items))
	for _, it := range items {
		ids = append(ids, it.TaskID)
	}
	sort.Strings(ids)
	return ids
}

// escalationNeedsWrite reports whether the breached set differs from an open
// escalation's task list. Equal sets (order-insensitive) mean the tick can
// skip the write, keeping the job churn-free while a breach sits unchanged.
// Pure.
func escalationNeedsWrite(existing, breached []string) bool {
	if len(existing) != len(breached) {
		return true
	}
	a := append([]string(nil), existing...)
	b := append([]string(nil), breached...)
	sort.Strings(a)
	sort.Strings(b)
	for i := range a {
		if a[i] != b[i] {
			return true
		}
	}
	return false
}

// staleEscalationKeep intersects an escalation's task list with the globally
// breached set, returning the tasks that still justify the row. An empty
// result means the row's sources all resolved and it must be cleared. Pure.
func staleEscalationKeep(escalated []string, breachedSet map[string]bool) []string {
	var keep []string
	for _, tid := range escalated {
		if breachedSet[tid] {
			keep = append(keep, tid)
		}
	}
	return keep
}

// taskIDFromDetails extracts the task linkage from a help-request details
// blob, falling back to the inbox item id so rows without a task_id still
// breach and clear deterministically. Pure.
func taskIDFromDetails(raw []byte, fallback string) string {
	var src struct {
		TaskID string `json:"task_id"`
	}
	if err := json.Unmarshal(raw, &src); err != nil {
		return fallback
	}
	if src.TaskID == "" {
		return fallback
	}
	return src.TaskID
}

// upsertEscalationForRecipient atomically replaces the open escalation for
// one (workspace, recipient, original_recipient) with a fresh alert row.
// Severity stays action_required: it is the highest level the inbox_item
// CHECK allows, and adding a new level would require a schema migration for
// no additional routing value — the escalated type already distinguishes
// the alert.
func upsertEscalationForRecipient(ctx context.Context, pool *pgxpool.Pool, key helpDigestKey, recipient string, items []helpSLAItem, now time.Time, sla time.Duration) error {
	taskIDs := slaTaskIDs(items)
	payload, err := json.Marshal(HelpEscalationDetails{
		SLAMinutes:        int(sla.Minutes()),
		BreachedCount:     len(items),
		TaskIDs:           taskIDs,
		OriginalRecipient: key.RecipientID,
		EscalatedAt:       now.UTC().Format(time.RFC3339),
	})
	if err != nil {
		return fmt.Errorf("help_sla: encode escalation: %w", err)
	}

	var ws pgtype.UUID
	if key.WorkspaceID != "" {
		if id, perr := util.ParseUUID(key.WorkspaceID); perr == nil {
			ws = id
		}
	}
	recipientID, err := util.ParseUUID(recipient)
	if err != nil {
		return fmt.Errorf("help_sla: parse recipient %q: %w", recipient, err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("help_sla: begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	qt := db.New(tx)
	if _, err := tx.Exec(ctx, deleteOpenEscalationSQL,
		InboxItemTypeAgentHelpEscalated,
		workspaceParam(key.WorkspaceID),
		key.RecipientType,
		recipient,
		key.RecipientID,
	); err != nil {
		return fmt.Errorf("help_sla: clear prior escalation: %w", err)
	}
	title := fmt.Sprintf("Agent help unattended (%d past %dm SLA)", len(items), int(sla.Minutes()))
	body := fmt.Sprintf("%d help request(s) waited longer than %dm without resolution. Widened to workspace owners.", len(items), int(sla.Minutes()))
	if _, err := qt.CreateInboxItem(ctx, db.CreateInboxItemParams{
		ID:            dbid.NewV7(),
		WorkspaceID:   ws,
		RecipientType: key.RecipientType,
		RecipientID:   recipientID,
		Type:          InboxItemTypeAgentHelpEscalated,
		Severity:      "action_required",
		IssueID:       pgtype.UUID{},
		Title:         title,
		Body:          pgtype.Text{String: body, Valid: true},
		ActorType:     pgtype.Text{String: "system", Valid: true},
		ActorID:       pgtype.UUID{},
		Details:       payload,
	}); err != nil {
		return fmt.Errorf("help_sla: create escalation: %w", err)
	}
	return tx.Commit(ctx)
}

// listOpenHelpForSLA returns the open help-request rows with the columns the
// breach check needs. Details are parsed for task linkage; unparsable rows
// fall back to the item id so they still breach and clear deterministically.
func listOpenHelpForSLA(ctx context.Context, pool *pgxpool.Pool) ([]helpSLAItem, error) {
	rows, err := pool.Query(ctx, listOpenHelpForSLASQL, InboxItemTypeAgentHelpRequested)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []helpSLAItem{}
	for rows.Next() {
		var id, ws pgtype.UUID
		var rt, rid string
		var details []byte
		var createdAt time.Time
		if err := rows.Scan(&id, &ws, &rt, &rid, &details, &createdAt); err != nil {
			return nil, err
		}
		idStr := util.UUIDToString(id)
		out = append(out, helpSLAItem{
			ID:            idStr,
			WorkspaceID:   workspaceKey(ws),
			RecipientType: rt,
			RecipientID:   rid,
			TaskID:        taskIDFromDetails(details, idStr),
			CreatedAt:     createdAt,
		})
	}
	return out, rows.Err()
}

// listOpenEscalations returns every open escalation row with its task list,
// so the reconciler can skip unchanged rows and clear resolved ones.
func listOpenEscalations(ctx context.Context, pool *pgxpool.Pool) ([]openEscalation, error) {
	rows, err := pool.Query(ctx, listOpenEscalationsSQL, InboxItemTypeAgentHelpEscalated)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []openEscalation{}
	for rows.Next() {
		var ws pgtype.UUID
		var rt, rid string
		var details []byte
		if err := rows.Scan(&ws, &rt, &rid, &details); err != nil {
			return nil, err
		}
		var det HelpEscalationDetails
		if err := json.Unmarshal(details, &det); err != nil {
			return nil, fmt.Errorf("help_sla: decode escalation details: %w", err)
		}
		out = append(out, openEscalation{
			WorkspaceID:       workspaceKey(ws),
			RecipientType:     rt,
			RecipientID:       rid,
			OriginalRecipient: det.OriginalRecipient,
			TaskIDs:           det.TaskIDs,
		})
	}
	return out, rows.Err()
}

// listWorkspaceOwnerAdmins returns the user ids that form the widened
// audience for a workspace: members with owner or admin role, ordered for
// determinism. The global bucket (empty workspace) has no widening.
func listWorkspaceOwnerAdmins(ctx context.Context, pool *pgxpool.Pool, workspaceID string) ([]string, error) {
	if workspaceID == "" {
		return nil, nil
	}
	ws, err := util.ParseUUID(workspaceID)
	if err != nil {
		return nil, nil
	}
	rows, err := pool.Query(ctx, listWorkspaceOwnerAdminsSQL, ws)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var uid pgtype.UUID
		if err := rows.Scan(&uid); err != nil {
			return nil, err
		}
		out = append(out, util.UUIDToString(uid))
	}
	return out, rows.Err()
}

// deleteOpenEscalation removes the open escalation row for one
// (workspace, recipient, original_recipient).
func deleteOpenEscalation(ctx context.Context, pool *pgxpool.Pool, e openEscalation) (int64, error) {
	tag, err := pool.Exec(ctx, deleteOpenEscalationSQL,
		InboxItemTypeAgentHelpEscalated,
		workspaceParam(e.WorkspaceID),
		e.RecipientType,
		e.RecipientID,
		e.OriginalRecipient,
	)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

const listOpenHelpForSLASQL = `
SELECT id, workspace_id, recipient_type, recipient_id, details, created_at
FROM inbox_item
WHERE type = $1
  AND severity = 'action_required'
  AND archived = false
ORDER BY created_at;`

const listOpenEscalationsSQL = `
SELECT workspace_id, recipient_type, recipient_id, details
FROM inbox_item
WHERE type = $1
  AND archived = false;`

const listWorkspaceOwnerAdminsSQL = `
SELECT user_id
FROM member
WHERE workspace_id = $1
  AND role IN ('owner', 'admin')
ORDER BY user_id;`

// deleteOpenEscalationSQL scopes the delete to one original group so
// escalations widened to the same owner from different groups never clobber
// each other.
const deleteOpenEscalationSQL = `
DELETE FROM inbox_item
WHERE type = $1
  AND archived = false
  AND (workspace_id IS NOT DISTINCT FROM $2)
  AND recipient_type = $3
  AND recipient_id = $4
  AND details->>'original_recipient' = $5;`
