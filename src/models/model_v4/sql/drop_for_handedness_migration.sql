-- ============================================================================
-- Drop the prediction tables so the pipeline recreates them with the
-- new handedness columns
-- ----------------------------------------------------------------------------
-- Run this ONCE after creating the handedness view but BEFORE re-running
-- pipeline.py.
--
-- WHY THIS IS NEEDED: the pipeline only creates a table if it doesn't exist.
-- Since the columns were changed (added handedness fields), we need to drop
-- the existing tables so they get rebuilt with the new schema.
--
-- WHAT IS PRESERVED:
--   * fact_model_evaluation_metrics is NOT dropped (history is kept)
--   * Any rows in fact_pitcher_strikeout_betting_ev with sportsbook entered
--     will be LOST. If you've been logging bets, save them first:
--
--     SELECT * INTO #my_bets FROM mlb.dbo.fact_pitcher_strikeout_betting_ev
--     WHERE sportsbook IS NOT NULL;
--
--     ...do the rebuild...
--     ...then re-insert from #my_bets manually if you want to keep them.
-- ============================================================================

-- Optional: save your historical bets first
-- SELECT *
-- INTO mlb.dbo.bets_backup_handedness_migration
-- FROM mlb.dbo.fact_pitcher_strikeout_betting_ev
-- WHERE sportsbook IS NOT NULL;

-- Drop the three prediction tables
IF OBJECT_ID('mlb.dbo.fact_pa_strikeout_predictions', 'U') IS NOT NULL
    DROP TABLE mlb.dbo.fact_pa_strikeout_predictions;

IF OBJECT_ID('mlb.dbo.fact_pitcher_game_strikeout_predictions', 'U') IS NOT NULL
    DROP TABLE mlb.dbo.fact_pitcher_game_strikeout_predictions;

IF OBJECT_ID('mlb.dbo.fact_pitcher_strikeout_betting_ev', 'U') IS NOT NULL
    DROP TABLE mlb.dbo.fact_pitcher_strikeout_betting_ev;

-- Note: fact_model_evaluation_metrics is NOT dropped - we preserve run history

PRINT 'Tables dropped. Run pipeline.py to recreate them with new handedness columns.';
