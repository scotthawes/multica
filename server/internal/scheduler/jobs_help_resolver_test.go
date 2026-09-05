package scheduler

import (
	"encoding/json"
	"testing"

	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

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
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ClassifyHelpNeeds(tc.needs); got != tc.want {
				t.Errorf("ClassifyHelpNeeds(%v) = %q, want %q", tc.needs, got, tc.want)
			}
		})
	}
}

func TestIsAutoResolvableHelpClass_NothingResolvesInSlice1(t *testing.T) {
	// Slice 1 (issue #53) ships classify + explicit human-only tagging only.
	// No category may auto-resolve until the secret-presence check lands.
	for _, class := range []HelpNeedClass{HelpNeedEmpty, HelpNeedCredential, HelpNeedHumanOnly} {
		if IsAutoResolvableHelpClass(class) {
			t.Errorf("IsAutoResolvableHelpClass(%q) = true, want false in slice 1", class)
		}
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
