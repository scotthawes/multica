-- P0 #52 rollback: restore the empty fallback chain on free-pin tier rows
-- that this migration updated. Scoped to the exact value it set.
UPDATE model_tier_map
SET fallback_concrete = '{}'
WHERE concrete = 'opencode-go/muse-spark-1.2-contributor-free'
  AND fallback_concrete = ARRAY['hy3-free'];
