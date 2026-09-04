package service

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// TestEnqueueTaskCarriesConcreteModelOverride is the MUL-404/405 data-plane
// acceptance test: when a workspace has a model_tier_map override for the
// agent's service tier, the enqueued task's concrete_model column carries that
// override (and is later surfaced to the agent launch via the handler).
func TestEnqueueTaskCarriesConcreteModelOverride(t *testing.T) {
	pool := newResolveOriginatorPool(t)
	ctx := context.Background()
	q := db.New(pool)

	workspaceID, userID, agentID, issueID := seedAttributionFixture(t, pool)

	// Give the agent a tier and a workspace-scoped override for it.
	if _, err := pool.Exec(ctx, `UPDATE agent SET service_tier='cheap' WHERE id=$1`, agentID); err != nil {
		t.Fatalf("set service_tier: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO model_tier_map (workspace_id, tier, concrete) VALUES ($1,'cheap','override-model')`,
		workspaceID); err != nil {
		t.Fatalf("insert workspace override: %v", err)
	}

	svc := &TaskService{Queries: q, TxStarter: pool, Bus: events.New()}
	task, err := svc.EnqueueTaskForIssue(ctx, db.Issue{
		ID:           util.MustParseUUID(issueID),
		AssigneeID:   util.MustParseUUID(agentID),
		Priority:     "medium",
		CreatorType:  "member",
		CreatorID:    util.MustParseUUID(userID),
		WorkspaceID:  util.MustParseUUID(workspaceID),
		AssigneeType: pgtype.Text{String: "agent", Valid: true},
	})
	if err != nil {
		t.Fatalf("EnqueueTaskForIssue: %v", err)
	}

	var concrete pgtype.Text
	if err := pool.QueryRow(ctx, `SELECT concrete_model FROM agent_task_queue WHERE id=$1`, task.ID).Scan(&concrete); err != nil {
		t.Fatalf("read concrete_model: %v", err)
	}
	if !concrete.Valid || concrete.String != "override-model" {
		t.Fatalf("concrete_model = %q (valid=%v), want override-model", concrete.String, concrete.Valid)
	}
}

// TestEnqueueTaskTierlessAgentDefaultsTier is the P0 #52 regression test:
// an agent with no stored service_tier must still resolve through the tier
// map (via defaultModelTier) so concrete_model is populated and the claim
// payload override + health marking engage, instead of bypassing with empty
// concrete and dispatching the raw agent.model pin.
func TestEnqueueTaskTierlessAgentDefaultsTier(t *testing.T) {
	pool := newResolveOriginatorPool(t)
	ctx := context.Background()
	q := db.New(pool)

	workspaceID, userID, agentID, issueID := seedAttributionFixture(t, pool)

	// The fixture agent carries no service_tier; the workspace override
	// targets the default tier the enqueue path assumes.
	if _, err := pool.Exec(ctx,
		`INSERT INTO model_tier_map (workspace_id, tier, concrete) VALUES ($1,'balanced','default-tier-model')`,
		workspaceID); err != nil {
		t.Fatalf("insert workspace override: %v", err)
	}

	svc := &TaskService{Queries: q, TxStarter: pool, Bus: events.New()}
	task, err := svc.EnqueueTaskForIssue(ctx, db.Issue{
		ID:           util.MustParseUUID(issueID),
		AssigneeID:   util.MustParseUUID(agentID),
		Priority:     "medium",
		CreatorType:  "member",
		CreatorID:    util.MustParseUUID(userID),
		WorkspaceID:  util.MustParseUUID(workspaceID),
		AssigneeType: pgtype.Text{String: "agent", Valid: true},
	})
	if err != nil {
		t.Fatalf("EnqueueTaskForIssue: %v", err)
	}

	var concrete, requested pgtype.Text
	if err := pool.QueryRow(ctx, `SELECT concrete_model, requested_concrete_model FROM agent_task_queue WHERE id=$1`, task.ID).Scan(&concrete, &requested); err != nil {
		t.Fatalf("read concrete models: %v", err)
	}
	if !concrete.Valid || concrete.String != "default-tier-model" {
		t.Fatalf("concrete_model = %q (valid=%v), want default-tier-model", concrete.String, concrete.Valid)
	}
	if !requested.Valid || requested.String != "default-tier-model" {
		t.Fatalf("requested_concrete_model = %q (valid=%v), want default-tier-model", requested.String, requested.Valid)
	}
}

// TestEffectiveModelTier pins the tier-default contract: a stored tier wins,
// NULL or empty falls back to defaultModelTier (never "").
func TestEffectiveModelTier(t *testing.T) {
	if got := effectiveModelTier(pgtype.Text{String: "cheap", Valid: true}); got != "cheap" {
		t.Fatalf("stored tier = %q, want cheap", got)
	}
	if got := effectiveModelTier(pgtype.Text{}); got != defaultModelTier {
		t.Fatalf("NULL tier = %q, want %q", got, defaultModelTier)
	}
	if got := effectiveModelTier(pgtype.Text{String: "", Valid: true}); got != defaultModelTier {
		t.Fatalf("empty tier = %q, want %q", got, defaultModelTier)
	}
	if defaultModelTier == "" {
		t.Fatal("defaultModelTier must be non-empty")
	}
}

// TestResolveConcreteModel_WorkspaceOverride pins the resolver precedence
// (workspace_map[tier] ?? global_map[tier] ?? tier) used by the enqueue path.
func TestResolveConcreteModel_WorkspaceOverride(t *testing.T) {
	pool := newResolveOriginatorPool(t)
	ctx := context.Background()
	q := db.New(pool)

	workspaceID, _, _, _ := seedAttributionFixture(t, pool)
	if _, err := pool.Exec(ctx,
		`INSERT INTO model_tier_map (workspace_id, tier, concrete) VALUES ($1,'cheap','ws-model')`,
		workspaceID); err != nil {
		t.Fatalf("insert workspace override: %v", err)
	}

	svc := &TaskService{Queries: q}
	got := svc.resolveConcreteModel(ctx, util.MustParseUUID(workspaceID), "cheap")
	if got != "ws-model" {
		t.Fatalf("resolveConcreteModel = %q, want ws-model", got)
	}
}
