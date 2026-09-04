-- P0 #52: free-pin tier rows ship with an empty fallback chain, so once the
-- enqueue tier default populates concrete_model, a failing free pin still has
-- nowhere to go. Give those rows the healthy hy3-free fallback without
-- touching any agent pin. Scoped to empty-fallback rows only; re-runnable.
UPDATE model_tier_map
SET fallback_concrete = ARRAY['hy3-free']
WHERE concrete = 'opencode-go/muse-spark-1.2-contributor-free'
  AND fallback_concrete = '{}';
