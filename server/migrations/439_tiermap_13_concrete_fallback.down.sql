-- P0 #52 rollback: restore the 1.2 concrete and empty fallback chain on
-- free-pin tier rows that this migration updated. Reverses up in order:
-- clear the hy3-free fallback it set, then restore the 1.2 concrete.
UPDATE model_tier_map
SET fallback_concrete = '{}'
WHERE concrete = 'muse-spark-1.3-contributor-free'
  AND fallback_concrete = ARRAY['hy3-free'];

UPDATE model_tier_map
SET concrete = 'muse-spark-1.2-contributor-free'
WHERE concrete = 'muse-spark-1.3-contributor-free';
