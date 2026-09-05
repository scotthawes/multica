-- P0 #52 follow-up: bump free-pin concrete 1.2 -> 1.3 (unprefixed match;
-- 438 matched only the opencode-go/ prefixed value, so 0 live rows moved).
-- Then fill the empty fallback chain on 1.3 free-pin rows with healthy
-- hy3-free (global + workspace 55e95948 covered generically). Re-runnable.
UPDATE model_tier_map
SET concrete = 'muse-spark-1.3-contributor-free'
WHERE concrete = 'muse-spark-1.2-contributor-free';

UPDATE model_tier_map
SET fallback_concrete = ARRAY['hy3-free']
WHERE concrete = 'muse-spark-1.3-contributor-free'
  AND fallback_concrete = '{}';
