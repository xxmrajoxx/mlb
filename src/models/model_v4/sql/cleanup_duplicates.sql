-- ============================================================================
-- ONE-TIME CLEANUP: Remove duplicate rows from previous pipeline runs
-- ----------------------------------------------------------------------------
-- Background: The original pipeline appended rows on every run instead of
-- replacing them. If you ran pipeline.py more than once, your prediction
-- tables now contain duplicates.
--
-- This script:
--   1. Removes duplicates from the three prediction tables
--   2. Keeps your bet history (rows in fact_pitcher_strikeout_betting_ev
--      where you've already entered odds) - those are NOT touched
--   3. Leaves fact_model_evaluation_metrics alone (the historical run log
--      is fine to keep, even if some rows came from failed runs)
--
-- Run this ONCE, then re-run pipeline.py. The updated pipeline.py will
-- prevent duplicates from happening again on future runs.
-- ============================================================================

-- ============================================================================
-- Step 1: Show current state (before cleanup)
-- ============================================================================
SELECT 'fact_pa_strikeout_predictions' AS table_name,
       COUNT(*) AS total_rows,
       COUNT(DISTINCT CAST(gamePk AS VARCHAR(20)) + '|' + CAST(pitcher_id AS VARCHAR(20)) + '|' + CAST(hitter_id AS VARCHAR(20)) + '|' + split_set) AS unique_rows,
       COUNT(*) - COUNT(DISTINCT CAST(gamePk AS VARCHAR(20)) + '|' + CAST(pitcher_id AS VARCHAR(20)) + '|' + CAST(hitter_id AS VARCHAR(20)) + '|' + split_set) AS duplicates
FROM mlb.dbo.fact_pa_strikeout_predictions
UNION ALL
SELECT 'fact_pitcher_game_strikeout_predictions',
       COUNT(*),
       COUNT(DISTINCT CAST(gamePk AS VARCHAR(20)) + '|' + CAST(pitcher_id AS VARCHAR(20)) + '|' + split_set),
       COUNT(*) - COUNT(DISTINCT CAST(gamePk AS VARCHAR(20)) + '|' + CAST(pitcher_id AS VARCHAR(20)) + '|' + split_set)
FROM mlb.dbo.fact_pitcher_game_strikeout_predictions
UNION ALL
SELECT 'fact_pitcher_strikeout_betting_ev (with odds)',
       COUNT(*), COUNT(*), 0
FROM mlb.dbo.fact_pitcher_strikeout_betting_ev WHERE sportsbook IS NOT NULL
UNION ALL
SELECT 'fact_pitcher_strikeout_betting_ev (no odds yet)',
       COUNT(*),
       COUNT(DISTINCT CAST(gamePk AS VARCHAR(20)) + '|' + CAST(pitcher_id AS VARCHAR(20))),
       COUNT(*) - COUNT(DISTINCT CAST(gamePk AS VARCHAR(20)) + '|' + CAST(pitcher_id AS VARCHAR(20)))
FROM mlb.dbo.fact_pitcher_strikeout_betting_ev WHERE sportsbook IS NULL;


-- ============================================================================
-- Step 2: Remove duplicates from fact_pa_strikeout_predictions
-- Keep only the most recent scored_at for each (gamePk, pitcher_id, hitter_id, split_set)
-- ============================================================================
WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY gamePk, pitcher_id, hitter_id, split_set
               ORDER BY scored_at DESC
           ) AS rn
    FROM mlb.dbo.fact_pa_strikeout_predictions
)
DELETE FROM ranked WHERE rn > 1;

PRINT 'Cleaned fact_pa_strikeout_predictions';


-- ============================================================================
-- Step 3: Remove duplicates from fact_pitcher_game_strikeout_predictions
-- ============================================================================
WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY gamePk, pitcher_id, split_set
               ORDER BY scored_at DESC
           ) AS rn
    FROM mlb.dbo.fact_pitcher_game_strikeout_predictions
)
DELETE FROM ranked WHERE rn > 1;

PRINT 'Cleaned fact_pitcher_game_strikeout_predictions';


-- ============================================================================
-- Step 4: Remove duplicates from fact_pitcher_strikeout_betting_ev
-- IMPORTANT: We only deduplicate rows where sportsbook IS NULL.
-- Rows where you've manually entered odds are kept verbatim - we never touch
-- your bet history.
-- ============================================================================
WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY gamePk, pitcher_id
               ORDER BY scored_at DESC
           ) AS rn
    FROM mlb.dbo.fact_pitcher_strikeout_betting_ev
    WHERE sportsbook IS NULL
)
DELETE FROM ranked WHERE rn > 1;

PRINT 'Cleaned fact_pitcher_strikeout_betting_ev (preserved rows with odds entered)';


-- ============================================================================
-- Step 5: Show post-cleanup state
-- ============================================================================
SELECT 'fact_pa_strikeout_predictions' AS table_name,
       COUNT(*) AS total_rows,
       COUNT(DISTINCT CAST(gamePk AS VARCHAR(20)) + '|' + CAST(pitcher_id AS VARCHAR(20)) + '|' + CAST(hitter_id AS VARCHAR(20)) + '|' + split_set) AS unique_rows,
       COUNT(*) - COUNT(DISTINCT CAST(gamePk AS VARCHAR(20)) + '|' + CAST(pitcher_id AS VARCHAR(20)) + '|' + CAST(hitter_id AS VARCHAR(20)) + '|' + split_set) AS duplicates_remaining
FROM mlb.dbo.fact_pa_strikeout_predictions
UNION ALL
SELECT 'fact_pitcher_game_strikeout_predictions',
       COUNT(*),
       COUNT(DISTINCT CAST(gamePk AS VARCHAR(20)) + '|' + CAST(pitcher_id AS VARCHAR(20)) + '|' + split_set),
       COUNT(*) - COUNT(DISTINCT CAST(gamePk AS VARCHAR(20)) + '|' + CAST(pitcher_id AS VARCHAR(20)) + '|' + split_set)
FROM mlb.dbo.fact_pitcher_game_strikeout_predictions
UNION ALL
SELECT 'fact_pitcher_strikeout_betting_ev (no odds yet)',
       COUNT(*),
       COUNT(DISTINCT CAST(gamePk AS VARCHAR(20)) + '|' + CAST(pitcher_id AS VARCHAR(20))),
       COUNT(*) - COUNT(DISTINCT CAST(gamePk AS VARCHAR(20)) + '|' + CAST(pitcher_id AS VARCHAR(20)))
FROM mlb.dbo.fact_pitcher_strikeout_betting_ev WHERE sportsbook IS NULL;


-- ============================================================================
-- Step 6: Verify Parker Messick (or any pitcher) is now clean
-- ============================================================================
SELECT gamePk, game_date, pitcher_name, predicted_strikeouts, actual_strikeouts, scored_at
FROM mlb.dbo.fact_pitcher_game_strikeout_predictions
WHERE pitcher_name = 'Parker Messick'
ORDER BY game_date DESC;
