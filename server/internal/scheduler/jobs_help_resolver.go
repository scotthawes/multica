package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/dbid"
)

// Help need classification for G1 auto-resolve (fork issue #53).
//
// Slice 1 shipped the PURE classifier plus digest need_class tagging.
// Slice 2 (this file's second half) adds the credential auto-resolve path:
// a secret-presence check over the existing env store plus a
// re-enqueue-with-delay hook that RunHelpDigest calls BEFORE reconciling,
// so a satisfiable credential request is archived and its task retried
// instead of paging a human. Everything else stays human-only.

// HelpNeedClass is the resolver's verdict on one help item's needs.
type HelpNeedClass string

const (
	// HelpNeedEmpty: the agent reported blocked with no actionable needs
	// (empty or whitespace-only list). Nothing to self-satisfy — human-only.
	HelpNeedEmpty HelpNeedClass = "empty"
	// HelpNeedCredential: at least one need names a credential/secret-shaped
	// thing. Auto-resolve candidate: re-enqueue with delay once the named
	// secret is present (see CredentialNeedSatisfied). Stays human-visible
	// while the secret is absent.
	HelpNeedCredential HelpNeedClass = "credential"
	// HelpNeedHumanOnly: approval, clarification, decision, or anything else
	// only a human can provide. Explicitly tagged so the digest never looks
	// silently unclassified.
	HelpNeedHumanOnly HelpNeedClass = "human_only"
)

// credentialNeedSubstrings matches need entries that name credential-shaped
// things. Substring (not exact) match: agents write "api key for stripe",
// "missing oauth token", etc.
var credentialNeedSubstrings = []string{
	"credential",
	"secret",
	"api key",
	"apikey",
	"token",
	"password",
	"cert",
}

// credentialSuffixPattern matches explicit UPPER_SNAKE secret names with
// credential-shaped suffixes ("STRIPE_API_KEY", "VAULT_TOKEN"). Plain
// prose keywords miss those ("missing STRIPE_API_KEY from vault" contains
// no bare "api key"), so without this the classifier would file an
// explicitly-named secret under human_only. Suffix-allowlisted (not "any
// UPPER token") so build metadata like BUILD_ID stays human-only.
var credentialSuffixPattern = regexp.MustCompile(`_(KEY|TOKEN|SECRET|PASSWORD|CREDENTIALS?|CERT|AUTH)\b`)

// ClassifyHelpNeeds maps a help item's needs list to its resolver class.
// Pure: no DB, no clock, no randomness.
func ClassifyHelpNeeds(needs []string) HelpNeedClass {
	trimmed := make([]string, 0, len(needs))
	for _, n := range needs {
		if t := strings.TrimSpace(n); t != "" {
			trimmed = append(trimmed, t)
		}
	}
	if len(trimmed) == 0 {
		return HelpNeedEmpty
	}
	for _, n := range trimmed {
		lower := strings.ToLower(n)
		for _, sub := range credentialNeedSubstrings {
			if strings.Contains(lower, sub) {
				return HelpNeedCredential
			}
		}
		if credentialSuffixPattern.MatchString(strings.ToUpper(n)) {
			return HelpNeedCredential
		}
	}
	return HelpNeedHumanOnly
}

// IsAutoResolvableHelpClass reports whether the resolver may auto-resolve a
// help item of this class WITHOUT human action. Slice 2: credential only,
// AND only when CredentialNeedSatisfied proves the named secret is present
// (see IsHelpItemAutoResolvable). Flipping the class gate without that
// presence check would silently drop real blockers. Kept as a function (not
// a constant) so the digest has one hook point to consult.
func IsAutoResolvableHelpClass(class HelpNeedClass) bool {
	return class == HelpNeedCredential
}

// SecretLookup is the existing secret store read as a function: name in,
// value + presence out. The production wiring is EnvSecretLookup (process
// env — no new infra); tests inject a map. A nil lookup never satisfies:
// fail closed so a miswired check keeps the human in the loop.
type SecretLookup func(name string) (string, bool)

// EnvSecretLookup reads the process env store. Empty-valued names count as
// absent — a secret set to "" satisfies nothing.
func EnvSecretLookup(name string) (string, bool) {
	v, ok := os.LookupEnv(name)
	if !ok || strings.TrimSpace(v) == "" {
		return "", false
	}
	return v, true
}

// credentialPhraseSuffix maps a credential-shaped phrase in a need entry to
// the generic env-store name that would satisfy it. Keys mirror
// credentialNeedSubstrings so every class=credential verdict has at least
// one candidate to check.
var credentialPhraseSuffix = []struct {
	phrase string
	suffix string
}{
	{"api key", "API_KEY"},
	{"apikey", "API_KEY"},
	{"password", "PASSWORD"},
	{"credential", "CREDENTIAL"},
	{"secret", "SECRET"},
	{"token", "TOKEN"},
	{"cert", "CERT"},
}

// helpNeedStopwords are filler words that must never become a secret-name
// prefix: "missing api key" must yield API_KEY, not MISSING_API_KEY.
var helpNeedStopwords = map[string]bool{
	"missing": true, "miss": true, "need": true, "needs": true,
	"needed": true, "require": true, "required": true, "the": true,
	"a": true, "an": true, "for": true, "of": true, "to": true,
	"my": true, "our": true, "please": true, "add": true,
	"with": true, "new": true, "get": true, "set": true,
	"and": true, "or": true,
}

// secretTokenPattern extracts explicit UPPER_SNAKE secret names agents name
// directly (e.g. "missing STRIPE_API_KEY from vault"). The underscore is
// mandatory: a bare "API" in "API key" is prose, not a secret name, and
// matching it would suppress the derived/generic candidates below.
var secretTokenPattern = regexp.MustCompile(`\b[A-Z][A-Z0-9_]*_[A-Z0-9_]+\b`)

// wordPattern splits a need entry into lowercase alphanumeric words for the
// derived-prefix heuristic.
var wordPattern = regexp.MustCompile(`[a-z0-9]+`)

// ExtractSecretCandidates derives the env-store names that would satisfy a
// credential need list. Pure: no DB, no clock, no randomness.
//
// Three sources, deduped in first-seen order:
//  1. explicit UPPER_SNAKE tokens in the text ("STRIPE_API_KEY");
//  2. derived <WORD>_<SUFFIX> from "<word> <credential-phrase>" pairs
//     ("stripe api key" → STRIPE_API_KEY, "oauth token" → OAUTH_TOKEN);
//  3. the generic suffix itself ("api key" → API_KEY).
func ExtractSecretCandidates(needs []string) []string {
	var out []string
	seen := map[string]bool{}
	add := func(c string) {
		if c == "" || seen[c] {
			return
		}
		seen[c] = true
		out = append(out, c)
	}
	for _, n := range needs {
		// An explicitly named secret wins outright: checking the generic
		// fallback alongside it would let a coincidental API_KEY resolve a
		// blocker that specifically needs STRIPE_API_KEY, burning a retry
		// that can only fail again.
		if toks := secretTokenPattern.FindAllString(n, -1); len(toks) > 0 {
			for _, tok := range toks {
				add(tok)
			}
			continue
		}
		lower := strings.ToLower(n)
		words := wordPattern.FindAllString(lower, -1)
		for i := 0; i < len(words); i++ {
			for _, ps := range credentialPhraseSuffix {
				parts := strings.Split(ps.phrase, " ")
				if !matchWordsAt(words, i, parts) {
					continue
				}
				add(ps.suffix)
				if i > 0 {
					if prev := words[i-1]; len(prev) >= 2 && !helpNeedStopwords[prev] {
						add(strings.ToUpper(prev) + "_" + ps.suffix)
					}
				}
			}
		}
	}
	return out
}

func matchWordsAt(words []string, at int, parts []string) bool {
	if at+len(parts) > len(words) {
		return false
	}
	for j, p := range parts {
		if words[at+j] != p {
			return false
		}
	}
	return true
}

// CredentialNeedSatisfied reports whether any secret named (explicitly or
// derivably) by the needs list is present in the store. A nil lookup, an
// empty need list, or no candidate hit all return false — fail closed.
func CredentialNeedSatisfied(needs []string, lookup SecretLookup) bool {
	if lookup == nil || len(needs) == 0 {
		return false
	}
	for _, cand := range ExtractSecretCandidates(needs) {
		if _, ok := lookup(cand); ok {
			return true
		}
	}
	return false
}

// IsHelpItemAutoResolvable is the full slice-2 verdict: the class gate AND
// the secret-presence check. Human-only and empty needs never resolve, and
// credential needs with no satisfiable secret stay human-visible.
func IsHelpItemAutoResolvable(needs []string, lookup SecretLookup) bool {
	if !IsAutoResolvableHelpClass(ClassifyHelpNeeds(needs)) {
		return false
	}
	return CredentialNeedSatisfied(needs, lookup)
}

// HelpAutoResolveRequeueDelay is the backoff before an auto-resolved task
// becomes claimable again. Non-zero so the PromoteDueDeferredTasksForRuntime
// sweeper (deferred via fire_at) paces the retry instead of hot-looping a
// task whose secret only just landed.
const HelpAutoResolveRequeueDelay = 5 * time.Minute

// HelpSecretLookup is the store read RunHelpDigest consults. Package var so
// tests can inject a fake without a DB; production value reads process env.
var HelpSecretLookup SecretLookup = EnvSecretLookup

// HelpRequeueTask re-enqueues the blocked task with a claim delay. Package
// var so tests can stub the DB write; the default clones the parent via the
// existing CreateRetryTask path (fire_at arms the delay — same mechanism as
// the runtime_offline / provider_network backoffs). A yielded slot
// (pgx.ErrNoRows: successor pending or workspace gone) counts as resolved:
// the work is already re-running or nowhere to run, so keeping the help
// item open would page a human for nothing.
var HelpRequeueTask = func(ctx context.Context, pool *pgxpool.Pool, taskID string, delay time.Duration) error {
	parentID, err := util.ParseUUID(taskID)
	if err != nil {
		return fmt.Errorf("help auto-resolve: parse task_id %q: %w", taskID, err)
	}
	fireAt := pgtype.Timestamptz{Time: time.Now().Add(delay), Valid: true}
	if _, err := db.New(pool).CreateRetryTask(ctx, db.CreateRetryTaskParams{
		ID:        parentID,
		NewTaskID: dbid.NewV7(),
		FireAt:    fireAt,
	}); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("help auto-resolve: requeue task %q: %w", taskID, err)
	}
	return nil
}

// archiveHelpItemSQL retires a resolved agent_help_requested source row so
// it drops out of the digest's open set.
const archiveHelpItemSQL = `
UPDATE inbox_item
SET archived = true
WHERE id = $1
  AND archived = false;`

// helpAutoResolveDeps carries the effect boundary of the auto-resolve step
// so unit tests can prove the reconcile without a DB: RunHelpDigest wires
// the pool-backed defaults, tests inject fakes.
type helpAutoResolveDeps struct {
	lookup  SecretLookup
	requeue func(ctx context.Context, taskID string) error
	archive func(ctx context.Context, item db.InboxItem) error
	// isRetryTask reports whether the blocked task is itself a retry child.
	// The loop guard consults it: a task that already consumed its one
	// auto-resolve (retry_of_task_id IS NOT NULL) and is STILL blocked stays
	// human-visible instead of requeueing on every digest tick. A nil func
	// means "not a retry" (tests that don't exercise the guard).
	isRetryTask func(ctx context.Context, taskID string) (bool, error)
}

// autoResolveSatisfiableHelp is the RunHelpDigest pre-step: every open help
// item whose needs are BOTH credential-class AND secret-satisfied is
// re-enqueued with delay and archived, so the reconciler below only ever
// sees genuinely human-blocked items. Fail-open throughout: a requeue or
// archive error keeps the item open rather than dropping a real blocker.
func autoResolveSatisfiableHelp(ctx context.Context, pool *pgxpool.Pool, open []db.InboxItem) ([]db.InboxItem, int64) {
	deps := helpAutoResolveDeps{
		lookup: HelpSecretLookup,
		// Loop guard: exactly one auto-resolve per task lineage. The
		// requeue clone records retry_of_task_id = parent, so a child that
		// fails with help again is recognizable and stays human-visible
		// instead of cycling every digest tick. A missing task row reads as
		// "not a retry" and falls through to the requeue path, whose
		// slot/workspace fence (pgx.ErrNoRows → resolved) owns that case.
		isRetryTask: func(ctx context.Context, taskID string) (bool, error) {
			parentID, err := util.ParseUUID(taskID)
			if err != nil {
				return false, err
			}
			var isRetry bool
			if err := pool.QueryRow(ctx,
				`SELECT retry_of_task_id IS NOT NULL FROM agent_task_queue WHERE id = $1`,
				parentID).Scan(&isRetry); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return false, nil
				}
				return false, err
			}
			return isRetry, nil
		},
		requeue: func(ctx context.Context, taskID string) error {
			return HelpRequeueTask(ctx, pool, taskID, HelpAutoResolveRequeueDelay)
		},
		archive: func(ctx context.Context, item db.InboxItem) error {
			tag, err := pool.Exec(ctx, archiveHelpItemSQL, item.ID)
			if err != nil {
				return err
			}
			if tag.RowsAffected() == 0 {
				return errors.New("help auto-resolve: archive affected no rows")
			}
			return nil
		},
	}
	return partitionAndResolveHelpItems(ctx, open, deps)
}

// partitionAndResolveHelpItems splits open help items into the remaining
// human-visible set, resolving the satisfiable ones via deps. Malformed
// source details or a missing task_id keep the item open (fail open).
func partitionAndResolveHelpItems(ctx context.Context, items []db.InboxItem, deps helpAutoResolveDeps) ([]db.InboxItem, int64) {
	remaining := make([]db.InboxItem, 0, len(items))
	var resolved int64
	for _, it := range items {
		needs, taskID := decodeHelpItemNeeds(it)
		if taskID == "" || !IsHelpItemAutoResolvable(needs, deps.lookup) {
			remaining = append(remaining, it)
			continue
		}
		if deps.isRetryTask != nil {
			isRetry, err := deps.isRetryTask(ctx, taskID)
			if err != nil {
				slog.Warn("help_digest: auto-resolve lineage check failed, keeping help item open",
					"task_id", taskID, "error", err)
				remaining = append(remaining, it)
				continue
			}
			if isRetry {
				slog.Info("help_digest: help item already auto-resolved once, keeping human-visible",
					"task_id", taskID, "needs", needs)
				remaining = append(remaining, it)
				continue
			}
		}
		if err := deps.requeue(ctx, taskID); err != nil {
			slog.Warn("help_digest: auto-resolve requeue failed, keeping help item open",
				"task_id", taskID, "error", err)
			remaining = append(remaining, it)
			continue
		}
		if err := deps.archive(ctx, it); err != nil {
			slog.Warn("help_digest: auto-resolve archive failed, keeping help item open",
				"task_id", taskID, "error", err)
			remaining = append(remaining, it)
			continue
		}
		resolved++
		slog.Info("help_digest: auto-resolved satisfiable help",
			"task_id", taskID, "needs", needs)
	}
	return remaining, resolved
}

// decodeHelpItemNeeds pulls the (needs, task_id) pair out of a source
// inbox row's details. The shape mirrors buildHelpDigestDetails; any
// decode failure yields ("", nil-needs) and the caller keeps the item open.
func decodeHelpItemNeeds(it db.InboxItem) ([]string, string) {
	var src struct {
		TaskID string   `json:"task_id"`
		Needs  []string `json:"needs"`
	}
	if err := json.Unmarshal(it.Details, &src); err != nil {
		return nil, ""
	}
	return src.Needs, strings.TrimSpace(src.TaskID)
}
