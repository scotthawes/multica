package scheduler

import (
	"strings"
)

// Help need classification for G1 auto-resolve (fork issue #53).
//
// Scope of this first slice: PURE classifier only, no DB, no clock, no
// re-enqueue. The digest reconciler (RunHelpDigest in jobs_help_digest.go)
// tags every item with its NeedClass so human-only requests are explicit in
// the digest payload. No category auto-resolves yet — see
// IsAutoResolvableHelpClass: the credential re-enqueue path needs a
// secret-presence check plus a task re-enqueue hook that do not exist yet.
// That is the digest-cycle proof remaining for #53.

// HelpNeedClass is the resolver's verdict on one help item's needs.
type HelpNeedClass string

const (
	// HelpNeedEmpty: the agent reported blocked with no actionable needs
	// (empty or whitespace-only list). Nothing to self-satisfy — human-only.
	HelpNeedEmpty HelpNeedClass = "empty"
	// HelpNeedCredential: at least one need names a credential/secret-shaped
	// thing. FUTURE auto-resolve candidate: re-enqueue with delay once the
	// named secret is present. NOT auto-resolved in this slice.
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
	}
	return HelpNeedHumanOnly
}

// IsAutoResolvableHelpClass reports whether the resolver may auto-resolve a
// help item of this class WITHOUT human action. Slice 1: always false.
// The credential path (re-enqueue with delay when the secret is now present)
// needs a secret-presence check that does not exist yet — flipping this to
// true before that check lands would silently drop real blockers. Kept as a
// function (not a constant) so the digest-cycle proof for #53 has one hook
// point to extend.
func IsAutoResolvableHelpClass(class HelpNeedClass) bool {
	return false
}
