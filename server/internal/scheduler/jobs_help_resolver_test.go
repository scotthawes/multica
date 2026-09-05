package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// errTestRequeueBoom is the canned requeue failure for fail-open tests.
var errTestRequeueBoom = errors.New("test requeue boom")

// mapLookup is a SecretLookup over an in-memory store for tests.
func mapLookup(store map[string]string) SecretLookup {
	return func(name string) (string, bool) {
		v, ok := store[name]
		if !ok || strings.TrimSpace(v) == "" {
			return "", false
		}
		return v, true
	}
}

func TestClassifyHelpNeeds(t *testing.T) {
	cases := []struct {
		name  string
		needs []string
		want  HelpNeedClass
	}{
		{"nil is empty", nil, HelpNeedEmpty},
		{"empty list is empty", []string{}, HelpNeedEmpty},
		{"whitespace-only is empty", []string{"", "   "}, HelpNeedEmpty},
		{"credential keyword", []string{"credential"}, HelpNeedCredential},
		{"secret phrase", []string{"missing Stripe API key"}, HelpNeedCredential},
		{"token phrase", []string{"need oauth token for repo"}, HelpNeedCredential},
		{"password", []string{"db PASSWORD"}, HelpNeedCredential},
		{"approval is human-only", []string{"approval"}, HelpNeedHumanOnly},
		{"clarification is human-only", []string{"clarify requirement"}, HelpNeedHumanOnly},
		{"decision is human-only", []string{"decision: ship or revert?"}, HelpNeedHumanOnly},
		{"credential wins mixed", []string{"approval", "missing secret"}, HelpNeedCredential},
		{"explicit secret name", []string{"missing STRIPE_API_KEY from vault"}, HelpNeedCredential},
		{"explicit token name", []string{"VAULT_TOKEN not mounted"}, HelpNeedCredential},
		{"build metadata is not a secret", []string{"check BUILD_ID output"}, HelpNeedHumanOnly},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ClassifyHelpNeeds(tc.needs); got != tc.want {
				t.Errorf("ClassifyHelpNeeds(%v) = %q, want %q", tc.needs, got, tc.want)
			}
		})
	}
}

func TestIsAutoResolvableHelpClass_OnlyCredentialResolvesInSlice2(t *testing.T) {
	// Slice 2 (issue #53) flips the class guard: credential is the only
	// auto-resolvable class, and only with a satisfied secret-presence
	// check (see IsHelpItemAutoResolvable). Empty and human-only never
	// resolve.
	if !IsAutoResolvableHelpClass(HelpNeedCredential) {
		t.Errorf("IsAutoResolvableHelpClass(credential) = false, want true in slice 2")
	}
	for _, class := range []HelpNeedClass{HelpNeedEmpty, HelpNeedHumanOnly} {
		if IsAutoResolvableHelpClass(class) {
			t.Errorf("IsAutoResolvableHelpClass(%q) = true, want false", class)
		}
	}
}

func TestExtractSecretCandidates(t *testing.T) {
	cases := []struct {
		name  string
		needs []string
		want  []string
	}{
		{
			"explicit upper-snake token",
			[]string{"missing STRIPE_API_KEY from vault"},
			[]string{"STRIPE_API_KEY"},
		},
		{
			"derived provider prefix plus generic",
			[]string{"missing Stripe API key for payouts"},
			[]string{"API_KEY", "STRIPE_API_KEY"},
		},
		{
			"oauth token derives prefix",
			[]string{"need oauth token for repo"},
			[]string{"TOKEN", "OAUTH_TOKEN"},
		},
		{
			"stopword never becomes a prefix",
			[]string{"missing api key"},
			[]string{"API_KEY"},
		},
		{
			"bare credential word",
			[]string{"credential"},
			[]string{"CREDENTIAL"},
		},
		{
			"non-credential need has no candidates",
			[]string{"approval"},
			nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ExtractSecretCandidates(tc.needs)
			if len(got) != len(tc.want) {
				t.Fatalf("ExtractSecretCandidates(%v) = %v, want %v", tc.needs, got, tc.want)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Fatalf("ExtractSecretCandidates(%v) = %v, want %v", tc.needs, got, tc.want)
				}
			}
		})
	}
}

func TestCredentialNeedSatisfied_PresenceTrueResolvable(t *testing.T) {
	lookup := mapLookup(map[string]string{"STRIPE_API_KEY": "sk-live-123"})
	if !CredentialNeedSatisfied([]string{"missing Stripe API key for payouts"}, lookup) {
		t.Errorf("CredentialNeedSatisfied = false, want true when the named secret is present")
	}
}

func TestCredentialNeedSatisfied_PresenceFalseStaysHuman(t *testing.T) {
	lookup := mapLookup(map[string]string{"OTHER_SECRET": "x"})
	if CredentialNeedSatisfied([]string{"missing Stripe API key for payouts"}, lookup) {
		t.Errorf("CredentialNeedSatisfied = true, want false when no named secret is present")
	}
}

func TestIsHelpItemAutoResolvable(t *testing.T) {
	full := mapLookup(map[string]string{"API_KEY": "k", "STRIPE_API_KEY": "s"})
	empty := mapLookup(nil)
	cases := []struct {
		name   string
		needs  []string
		lookup SecretLookup
		want   bool
	}{
		{"satisfiable credential resolves", []string{"missing api key"}, full, true},
		{"unsatisfied credential stays human", []string{"missing api key"}, empty, false},
		{"human-only never resolves even with secrets", []string{"approval"}, full, false},
		{"empty never resolves", nil, full, false},
		{"nil lookup fails closed", []string{"missing api key"}, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsHelpItemAutoResolvable(tc.needs, tc.lookup); got != tc.want {
				t.Errorf("IsHelpItemAutoResolvable(%v) = %v, want %v", tc.needs, got, tc.want)
			}
		})
	}
}

func TestPartitionAndResolveHelpItems_ResolvesSatisfiableAndClearsDigest(t *testing.T) {
	// Digest-cycle proof for #53 at unit level: a satisfiable credential
	// item is re-enqueued + archived (so it leaves the digest's open set)
	// while a human-only item in the same group stays.
	ctx := context.Background()
	ws := "11111111-1111-1111-1111-111111111111"
	ra := "aaaaaaaa-1111-1111-1111-111111111111"
	items := []db.InboxItem{
		mustHelpItem(t, ws, ra, "task-cred", "agent-1", map[string]any{
			"task_id": "task-cred", "agent_id": "agent-1",
			"needs": []string{"missing STRIPE_API_KEY from vault"},
		}),
		mustHelpItem(t, ws, ra, "task-human", "agent-2", map[string]any{
			"task_id": "task-human", "agent_id": "agent-2",
			"needs": []string{"approval"},
		}),
	}
	var requeued []string
	var archived int
	deps := helpAutoResolveDeps{
		lookup:      mapLookup(map[string]string{"STRIPE_API_KEY": "sk-live-123"}),
		isRetryTask: func(_ context.Context, _ string) (bool, error) { return false, nil },
		requeue: func(_ context.Context, taskID string) error {
			requeued = append(requeued, taskID)
			return nil
		},
		archive: func(_ context.Context, _ db.InboxItem) error {
			archived++
			return nil
		},
	}
	remaining, resolved := partitionAndResolveHelpItems(ctx, items, deps)
	if resolved != 1 {
		t.Fatalf("resolved = %d, want 1", resolved)
	}
	if len(remaining) != 1 {
		t.Fatalf("remaining = %d, want 1 (human-only item stays)", len(remaining))
	}
	if len(requeued) != 1 || requeued[0] != "task-cred" {
		t.Fatalf("requeued = %v, want [task-cred]", requeued)
	}
	if archived != 1 {
		t.Fatalf("archived = %d, want 1", archived)
	}

	// The reconciler groups what remains: one group of one — the digest is
	// upserted without the resolved item.
	groups := groupHelpItems(remaining)
	if len(groups) != 1 {
		t.Fatalf("groups = %d, want 1", len(groups))
	}
	det, err := buildHelpDigestDetails(remaining)
	if err != nil {
		t.Fatalf("buildHelpDigestDetails: %v", err)
	}
	if det.Count != 1 || det.Items[0].TaskID != "task-human" {
		t.Fatalf("digest = %+v, want only task-human", det)
	}

	// Once the human item is archived too, the open set is empty and every
	// existing digest is stale — the reconciler clears them.
	if len(groupHelpItems(nil)) != 0 {
		t.Fatalf("empty open set must yield no groups (all digests cleared)")
	}
}

func TestPartitionAndResolveHelpItems_RequeueFailureKeepsItemOpen(t *testing.T) {
	// Fail-open: a requeue error must NOT drop the help item.
	ctx := context.Background()
	ws := "11111111-1111-1111-1111-111111111111"
	ra := "aaaaaaaa-1111-1111-1111-111111111111"
	items := []db.InboxItem{
		mustHelpItem(t, ws, ra, "task-cred", "agent-1", map[string]any{
			"task_id": "task-cred", "agent_id": "agent-1",
			"needs": []string{"missing api key"},
		}),
	}
	archived := 0
	deps := helpAutoResolveDeps{
		lookup:      mapLookup(map[string]string{"API_KEY": "k"}),
		isRetryTask: func(_ context.Context, _ string) (bool, error) { return false, nil },
		requeue:     func(_ context.Context, _ string) error { return errTestRequeueBoom },
		archive: func(_ context.Context, _ db.InboxItem) error {
			archived++
			return nil
		},
	}
	remaining, resolved := partitionAndResolveHelpItems(ctx, items, deps)
	if resolved != 0 || len(remaining) != 1 {
		t.Fatalf("resolved = %d remaining = %d, want 0 and 1 (fail open)", resolved, len(remaining))
	}
	if archived != 0 {
		t.Fatalf("archived = %d, want 0 (no archive without requeue)", archived)
	}
}

func TestPartitionAndResolveHelpItems_RetryChildStaysHumanVisible(t *testing.T) {
	// Loop guard: a task that already consumed its one auto-resolve (it is
	// itself a retry child) and is STILL blocked must not requeue again —
	// otherwise every 15m digest tick would mint a fresh task forever.
	ctx := context.Background()
	ws := "11111111-1111-1111-1111-111111111111"
	ra := "aaaaaaaa-1111-1111-1111-111111111111"
	items := []db.InboxItem{
		mustHelpItem(t, ws, ra, "task-retry", "agent-1", map[string]any{
			"task_id": "task-retry", "agent_id": "agent-1",
			"needs": []string{"missing api key"},
		}),
	}
	requeued := 0
	deps := helpAutoResolveDeps{
		lookup:      mapLookup(map[string]string{"API_KEY": "k"}),
		isRetryTask: func(_ context.Context, _ string) (bool, error) { return true, nil },
		requeue: func(_ context.Context, _ string) error {
			requeued++
			return nil
		},
		archive: func(_ context.Context, _ db.InboxItem) error { return nil },
	}
	remaining, resolved := partitionAndResolveHelpItems(ctx, items, deps)
	if resolved != 0 || len(remaining) != 1 {
		t.Fatalf("resolved = %d remaining = %d, want 0 and 1 (one auto-resolve per lineage)", resolved, len(remaining))
	}
	if requeued != 0 {
		t.Fatalf("requeued = %d, want 0 (no second retry)", requeued)
	}
}

func TestPartitionAndResolveHelpItems_MalformedDetailsStaysOpen(t *testing.T) {
	ctx := context.Background()
	ws := "11111111-1111-1111-1111-111111111111"
	ra := "aaaaaaaa-1111-1111-1111-111111111111"
	bad := mustHelpItem(t, ws, ra, "task-bad", "agent-1", map[string]any{
		"task_id": "task-bad", "agent_id": "agent-1",
		"needs": []string{"missing api key"},
	})
	bad.Details = []byte("not-json")
	deps := helpAutoResolveDeps{
		lookup:      mapLookup(map[string]string{"API_KEY": "k"}),
		isRetryTask: func(_ context.Context, _ string) (bool, error) { return false, nil },
		requeue:     func(_ context.Context, _ string) error { return errTestRequeueBoom },
		archive:     func(_ context.Context, _ db.InboxItem) error { return nil },
	}
	remaining, resolved := partitionAndResolveHelpItems(ctx, []db.InboxItem{bad}, deps)
	if resolved != 0 || len(remaining) != 1 {
		t.Fatalf("resolved = %d remaining = %d, want 0 and 1 (malformed stays open)", resolved, len(remaining))
	}
}

func TestBuildHelpDigestDetails_TagsNeedClass(t *testing.T) {
	ws := "11111111-1111-1111-1111-111111111111"
	ra := "aaaaaaaa-1111-1111-1111-111111111111"
	cases := []struct {
		task    string
		details map[string]any
		want    HelpNeedClass
	}{
		{"task-empty", map[string]any{"task_id": "task-empty", "agent_id": "agent-1"}, HelpNeedEmpty},
		{"task-cred", map[string]any{"task_id": "task-cred", "agent_id": "agent-1", "needs": []string{"missing api key"}}, HelpNeedCredential},
		{"task-human", map[string]any{"task_id": "task-human", "agent_id": "agent-1", "needs": []string{"approval"}}, HelpNeedHumanOnly},
	}
	inbox := make([]db.InboxItem, 0, len(cases))
	for _, tc := range cases {
		inbox = append(inbox, mustHelpItem(t, ws, ra, tc.task, "agent-1", tc.details))
	}
	det, err := buildHelpDigestDetails(inbox)
	if err != nil {
		t.Fatalf("buildHelpDigestDetails: %v", err)
	}
	byTask := map[string]HelpDigestItem{}
	for _, it := range det.Items {
		byTask[it.TaskID] = it
	}
	for _, tc := range cases {
		got, ok := byTask[tc.task]
		if !ok {
			t.Fatalf("digest missing task %q", tc.task)
		}
		if got.NeedClass != tc.want {
			t.Errorf("task %q need_class = %q, want %q", tc.task, got.NeedClass, tc.want)
		}
	}
	// need_class must survive the JSON round-trip (it is read by the digest UI).
	enc, err := json.Marshal(det)
	if err != nil {
		t.Fatalf("marshal digest: %v", err)
	}
	var dec HelpDigestDetails
	if err := json.Unmarshal(enc, &dec); err != nil {
		t.Fatalf("unmarshal digest: %v", err)
	}
	for _, it := range dec.Items {
		if it.NeedClass == "" {
			t.Errorf("task %q lost need_class in JSON round-trip", it.TaskID)
		}
	}
}
