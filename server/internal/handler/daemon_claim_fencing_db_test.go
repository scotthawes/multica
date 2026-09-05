package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// G4 (#57) DB-backed fencing: the full dispatched_at claim-epoch round trip
// through the HTTP start path.
//
//   - current-epoch Start → 200 running (the CAS must match — this pins the
//     nano-precision wire format; a second-truncated epoch never matches the
//     microsecond-precision row and every fenced start would 409);
//   - same-epoch retry after success → 200 idempotent running;
//   - stale-epoch Start after the row was reclaimed (new dispatched_at) → 409.
func TestStartTaskCASFencingDB(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()

	var agentID, runtimeID string
	dbfx.QueryRow(t, `SELECT a.id, a.runtime_id FROM agent a WHERE a.workspace_id = $1 LIMIT 1`, testWorkspaceID).Scan(&agentID, &runtimeID)
	issueID := dbfx.Issue(t, "G4 fencing fixture")

	newDispatchedTask := func(t *testing.T) string {
		t.Helper()
		var taskID string
		if err := testPool.QueryRow(ctx, `
			INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, status, priority, dispatched_at)
			VALUES ($1, $2, $3, 'dispatched', 0, now())
			RETURNING id
		`, agentID, runtimeID, issueID).Scan(&taskID); err != nil {
			t.Fatalf("insert dispatched task: %v", err)
		}
		t.Cleanup(func() {
			testPool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE id = $1`, taskID)
		})
		return taskID
	}

	// claimEpoch mirrors what the daemon holds: the claim response's
	// dispatched_at verbatim.
	claimEpoch := func(t *testing.T, taskID string) string {
		t.Helper()
		task, err := testHandler.Queries.GetAgentTask(ctx, parseUUID(taskID))
		if err != nil {
			t.Fatalf("GetAgentTask: %v", err)
		}
		epoch := taskToResponse(task, testWorkspaceID).DispatchedAt
		if epoch == nil || *epoch == "" {
			t.Fatal("claim response carries no dispatched_at fencing epoch")
		}
		return *epoch
	}

	startWithEpoch := func(t *testing.T, taskID string, epoch *string) *httptest.ResponseRecorder {
		t.Helper()
		var body any
		if epoch != nil {
			body = map[string]any{"dispatched_at": *epoch}
		}
		w := httptest.NewRecorder()
		req := newDaemonTokenRequest(http.MethodPost, "/api/daemon/tasks/"+taskID+"/start", body, testWorkspaceID, "fencing-test-daemon")
		req = withURLParam(req, "taskId", taskID)
		testHandler.StartTask(w, req)
		return w
	}

	taskStatus := func(t *testing.T, taskID string) string {
		t.Helper()
		var status string
		if err := testPool.QueryRow(ctx, `SELECT status FROM agent_task_queue WHERE id = $1`, taskID).Scan(&status); err != nil {
			t.Fatalf("load task status: %v", err)
		}
		return status
	}

	t.Run("current epoch starts, same-epoch retry is idempotent", func(t *testing.T) {
		taskID := newDispatchedTask(t)
		epoch := claimEpoch(t, taskID)

		if w := startWithEpoch(t, taskID, &epoch); w.Code != http.StatusOK {
			t.Fatalf("current-epoch start: got %d, want 200: %s", w.Code, w.Body.String())
		}
		if got := taskStatus(t, taskID); got != "running" {
			t.Fatalf("status after start = %q, want running", got)
		}

		if w := startWithEpoch(t, taskID, &epoch); w.Code != http.StatusOK {
			t.Fatalf("same-epoch retry: got %d, want idempotent 200: %s", w.Code, w.Body.String())
		}
		if got := taskStatus(t, taskID); got != "running" {
			t.Fatalf("status after retry = %q, want running", got)
		}
	})

	t.Run("stale epoch is fenced", func(t *testing.T) {
		taskID := newDispatchedTask(t)
		stale := claimEpoch(t, taskID)

		// Simulate a reclaim: a new claim generation owns the row now.
		dbfx.Exec(t, `UPDATE agent_task_queue SET dispatched_at = now() + interval '1 minute' WHERE id = $1`, taskID)

		if w := startWithEpoch(t, taskID, &stale); w.Code != http.StatusConflict {
			t.Fatalf("stale-epoch start: got %d, want 409: %s", w.Code, w.Body.String())
		}
		if got := taskStatus(t, taskID); got != "dispatched" {
			t.Fatalf("status after fenced start = %q, want dispatched (no double-run)", got)
		}
	})

	t.Run("legacy start without epoch stays unfenced", func(t *testing.T) {
		taskID := newDispatchedTask(t)
		if w := startWithEpoch(t, taskID, nil); w.Code != http.StatusOK {
			t.Fatalf("epochless start: got %d, want 200: %s", w.Code, w.Body.String())
		}
		if got := taskStatus(t, taskID); got != "running" {
			t.Fatalf("status after epochless start = %q, want running", got)
		}
	})
}
