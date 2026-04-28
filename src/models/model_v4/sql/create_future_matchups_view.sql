-- ============================================================================
-- Build synthetic future-game matchup rows
-- ----------------------------------------------------------------------------
-- New simpler approach: instead of reconstructing 334 features from raw
-- rolling tables (which don't exist as standalone tables in your schema),
-- we pull each hitter's MOST RECENT row from the existing matchup view
-- and reuse those features. This works because:
--
--   * Rolling features change slowly between games
--   * The matchup view already has all 334 features pre-computed
--   * Feature drift over 1-3 days is small enough not to materially
--     hurt the prediction
--
-- WORKFLOW:
--   1. Identify upcoming games + probable pitchers (from mlb_schedule + dim_probable_pitchers)
--   2. For each opposing team: find the 9 most-frequent recent hitters
--   3. For each (hitter, future-game): pull that hitter's most recent feature row
--   4. Override the pitcher columns with the future game's pitcher info
--   5. Override handedness columns to reflect the new pitcher's throwing arm
--
-- LIMITATION: pitcher-side rolling features come from the hitter's most
-- recent OPPONENT pitcher's stats, not the actual probable pitcher.
-- We fix this in step 4 by overlaying the probable pitcher's most recent
-- feature row from the matchup view.
-- ============================================================================

USE mlb;
GO

IF OBJECT_ID('dbo.fact_future_matchups', 'V') IS NOT NULL
    DROP VIEW dbo.fact_future_matchups;
GO

CREATE VIEW dbo.fact_future_matchups AS
WITH
-- Step 1: Identify upcoming scheduled games
upcoming_games AS (
    SELECT
        s.gamePk,
        CAST(s.gameDate AS DATE) AS game_date,
        s.season,
        s.teams_home_team_id AS home_team_id,
        s.teams_home_team_name AS home_team_name,
        s.teams_away_team_id AS away_team_id,
        s.teams_away_team_name AS away_team_name
    FROM dbo.mlb_schedule s
    WHERE s.gameType = 'R'
      AND s.season = YEAR(GETDATE())
      AND CAST(s.gameDate AS DATE) >= CAST(GETDATE() AS DATE)
      AND s.status_detailedState IN ('Scheduled', 'Pre-Game', 'Warmup', 'Preview')
),

-- Step 2: Attach probable pitchers
games_with_pitchers AS (
    SELECT
        g.gamePk           AS future_gamePk,
        g.game_date        AS future_game_date,
        g.season           AS future_season,
        pp.pitcher_id      AS future_pitcher_id,
        pp.pitcher_name    AS future_pitcher_name,
        pp.team_id         AS future_pitcher_team_id,
        pp.team_name       AS future_pitcher_team_name,
        CASE WHEN pp.side = 'home' THEN g.away_team_id   ELSE g.home_team_id   END AS future_opponent_team_id,
        CASE WHEN pp.side = 'home' THEN g.away_team_name ELSE g.home_team_name END AS future_opponent_team_name
    FROM upcoming_games g
    JOIN dbo.dim_probable_pitchers pp ON pp.gamePk = g.gamePk
    WHERE pp.pitcher_id IS NOT NULL
),

-- Step 3: For each opponent team, identify their top 9 most-frequent hitters
-- in the last 30 days. These are our estimated lineups.
recent_team_hitters AS (
    SELECT
        h.hitter_team_id,
        h.hitter_id,
        h.hitter_name,
        COUNT(DISTINCT h.gamePk) AS games_played,
        MAX(h.game_date) AS last_seen_date,
        ROW_NUMBER() OVER (
            PARTITION BY h.hitter_team_id
            ORDER BY COUNT(DISTINCT h.gamePk) DESC, MAX(h.game_date) DESC
        ) AS lineup_rank
    FROM dbo.fact_hitter_pitcher_matchup_with_handedness h
    WHERE h.game_date >= DATEADD(day, -30, CAST(GETDATE() AS DATE))
      AND h.season = YEAR(GETDATE())
      AND h.hitter_plate_appearances >= 1
    GROUP BY h.hitter_team_id, h.hitter_id, h.hitter_name
),

estimated_lineups AS (
    SELECT *
    FROM recent_team_hitters
    WHERE lineup_rank <= 9
),

-- Step 4: For each hitter, find their most recent feature snapshot from
-- the matchup view. We use a row-number window so we get exactly one row
-- per hitter (their latest historical row).
latest_hitter_snapshots AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY hitter_id
               ORDER BY game_date DESC, gamePk DESC
           ) AS recency_rank
    FROM dbo.fact_hitter_pitcher_matchup_with_handedness
    WHERE season = YEAR(GETDATE())
      AND game_date < CAST(GETDATE() AS DATE)
),

-- Step 5: For each pitcher, find their most recent feature snapshot. This
-- gives us the pitcher's rolling stats as of their last appearance.
latest_pitcher_snapshots AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY pitcher_id
               ORDER BY game_date DESC, gamePk DESC
           ) AS recency_rank
    FROM dbo.fact_hitter_pitcher_matchup_with_handedness
    WHERE season = YEAR(GETDATE())
      AND game_date < CAST(GETDATE() AS DATE)
      AND pitcher_is_starter = 1
)

-- Step 6: Final assembly. Output one row per (future_game, lineup_hitter).
-- Hitter-side features come from the hitter's latest snapshot (h).
-- Pitcher-side features come from the pitcher's latest snapshot (p).
-- Identifiers and handedness come from the new future game context.
SELECT
    -- Future game identifiers (override the historical row's IDs)
    gp.future_gamePk         AS gamePk,
    gp.future_game_date      AS game_date,
    gp.future_season         AS season,

    -- Hitter info (from estimated lineup + latest snapshot)
    h.hitter_id, h.hitter_name, h.hitter_position,
    el.hitter_team_id, h.hitter_team_name,
    h.hitter_lineup_position,

    -- Pitcher info (from probable pitcher + latest snapshot)
    gp.future_pitcher_id          AS pitcher_id,
    gp.future_pitcher_name        AS pitcher_name,
    gp.future_pitcher_team_id     AS pitcher_team_id,
    gp.future_pitcher_team_name   AS pitcher_team_name,

    -- Estimated lineup rank (where in the 9 we ranked this hitter)
    el.lineup_rank AS lineup_rank_estimate,

    -- Synthetic PA assumptions for future games
    3 AS hitter_plate_appearances,    -- approximation: 3 PA vs starter
    NULL AS hitter_strikeouts,         -- unknown for future
    1 AS pitcher_is_starter,
    1 AS first_inning_faced,
    NULL AS y_k_rate,
    3 AS sample_weight,

    -- Handedness - re-derive from dim_player using the FUTURE pitcher
    bat.bat_side_code AS hitter_bats,
    pit.pitch_hand_code AS pitcher_throws,
    CASE
        WHEN bat.bat_side_code = 'S' THEN 'Switch'
        WHEN bat.bat_side_code = pit.pitch_hand_code THEN 'Same'
        WHEN bat.bat_side_code IS NOT NULL AND pit.pitch_hand_code IS NOT NULL THEN 'Opposite'
        ELSE NULL
    END AS platoon_matchup,
    CASE
        WHEN bat.bat_side_code = 'S' THEN NULL
        WHEN bat.bat_side_code = pit.pitch_hand_code THEN 1
        WHEN bat.bat_side_code IS NOT NULL AND pit.pitch_hand_code IS NOT NULL THEN 0
        ELSE NULL
    END AS is_same_side_matchup,
    CASE WHEN bat.bat_side_code = 'L' THEN 1 ELSE 0 END AS hitter_is_lefty,
    CASE WHEN pit.pitch_hand_code = 'L' THEN 1 ELSE 0 END AS pitcher_is_lefty,
    CASE WHEN bat.bat_side_code = 'S' THEN 1 ELSE 0 END AS hitter_is_switch,

    -- ALL HITTER FEATURES from the hitter's latest snapshot row.
    -- These are the same column names as in the matchup view, so the
    -- model sees them in the same shape it was trained on.
    h.hitter_days_since_last_game,
    h.hitter_avg_k_last_3, h.hitter_avg_pa_last_3, h.hitter_avg_ab_last_3,
    h.hitter_avg_hits_last_3, h.hitter_avg_hr_last_3, h.hitter_avg_bb_last_3,
    h.hitter_avg_pitches_last_3, h.hitter_avg_tb_last_3, h.hitter_avg_rbi_last_3,
    h.hitter_avg_obp_last_3, h.hitter_avg_slg_last_3, h.hitter_avg_ops_last_3,
    h.hitter_avg_batting_avg_last_3, h.hitter_avg_k_rate_last_3,
    h.hitter_avg_walk_rate_last_3, h.hitter_avg_hit_rate_last_3,
    h.hitter_avg_tb_rate_last_3, h.hitter_avg_hr_rate_last_3,
    h.hitter_sum_pa_last_3, h.hitter_sum_ab_last_3,
    h.hitter_pct_1plus_k_last_3, h.hitter_pct_2plus_k_last_3,
    h.hitter_weighted_k_rate_last_3, h.hitter_weighted_walk_rate_last_3,
    h.hitter_weighted_hit_rate_last_3, h.hitter_weighted_tb_rate_last_3,
    h.hitter_weighted_hr_rate_last_3, h.hitter_weighted_batting_avg_last_3,
    h.hitter_weighted_pitches_per_pa_last_3, h.hitter_weighted_obp_last_3,
    h.hitter_weighted_slg_last_3, h.hitter_weighted_ops_last_3,

    h.hitter_avg_k_last_5, h.hitter_avg_pa_last_5, h.hitter_avg_ab_last_5,
    h.hitter_avg_hits_last_5, h.hitter_avg_hr_last_5, h.hitter_avg_bb_last_5,
    h.hitter_avg_pitches_last_5, h.hitter_avg_obp_last_5, h.hitter_avg_slg_last_5,
    h.hitter_avg_ops_last_5, h.hitter_avg_batting_avg_last_5,
    h.hitter_avg_k_rate_last_5, h.hitter_avg_walk_rate_last_5,
    h.hitter_avg_hit_rate_last_5, h.hitter_avg_tb_rate_last_5,
    h.hitter_avg_hr_rate_last_5, h.hitter_sum_pa_last_5, h.hitter_sum_ab_last_5,
    h.hitter_pct_1plus_k_last_5, h.hitter_pct_2plus_k_last_5,
    h.hitter_weighted_k_rate_last_5, h.hitter_weighted_walk_rate_last_5,
    h.hitter_weighted_hit_rate_last_5, h.hitter_weighted_tb_rate_last_5,
    h.hitter_weighted_hr_rate_last_5, h.hitter_weighted_batting_avg_last_5,
    h.hitter_weighted_pitches_per_pa_last_5, h.hitter_weighted_obp_last_5,
    h.hitter_weighted_slg_last_5, h.hitter_weighted_ops_last_5,

    h.hitter_avg_k_last_10, h.hitter_avg_pa_last_10, h.hitter_avg_ab_last_10,
    h.hitter_avg_hits_last_10, h.hitter_avg_hr_last_10, h.hitter_avg_bb_last_10,
    h.hitter_avg_pitches_last_10, h.hitter_avg_obp_last_10, h.hitter_avg_slg_last_10,
    h.hitter_avg_ops_last_10, h.hitter_avg_batting_avg_last_10,
    h.hitter_avg_k_rate_last_10, h.hitter_avg_walk_rate_last_10,
    h.hitter_avg_hit_rate_last_10, h.hitter_avg_tb_rate_last_10,
    h.hitter_avg_hr_rate_last_10, h.hitter_sum_pa_last_10, h.hitter_sum_ab_last_10,
    h.hitter_pct_1plus_k_last_10, h.hitter_pct_2plus_k_last_10,
    h.hitter_weighted_k_rate_last_10, h.hitter_weighted_walk_rate_last_10,
    h.hitter_weighted_hit_rate_last_10, h.hitter_weighted_tb_rate_last_10,
    h.hitter_weighted_hr_rate_last_10, h.hitter_weighted_batting_avg_last_10,
    h.hitter_weighted_pitches_per_pa_last_10, h.hitter_weighted_obp_last_10,
    h.hitter_weighted_slg_last_10, h.hitter_weighted_ops_last_10,

    h.hitter_prev_k, h.hitter_prev_pa, h.hitter_prev_ab, h.hitter_prev_hits,
    h.hitter_prev_hr, h.hitter_prev_bb, h.hitter_prev_ops, h.hitter_prev_k_rate,

    -- Hitter Statcast features (the ones that were missing before)
    h.hitter_avg_whiff_rate_last_3, h.hitter_avg_contact_rate_last_3,
    h.hitter_avg_swing_rate_last_3, h.hitter_avg_chase_rate_last_3,
    h.hitter_avg_zone_swing_rate_last_3, h.hitter_avg_zone_rate_last_3,
    h.hitter_avg_called_strike_rate_last_3, h.hitter_avg_csw_against_rate_last_3,
    h.hitter_avg_two_strike_whiff_rate_last_3, h.hitter_avg_whiff_rate_0_2_last_3,
    h.hitter_avg_whiff_rate_1_2_last_3, h.hitter_avg_whiff_rate_2_2_last_3,
    h.hitter_avg_exit_velocity_last_3, h.hitter_avg_xwoba_last_3,
    h.hitter_avg_bat_speed_last_3,
    h.hitter_avg_whiff_rate_vs_rhp_last_3, h.hitter_avg_whiff_rate_vs_lhp_last_3,
    h.hitter_weighted_whiff_rate_last_3, h.hitter_weighted_contact_rate_last_3,
    h.hitter_weighted_chase_rate_last_3, h.hitter_weighted_csw_against_rate_last_3,
    h.hitter_weighted_two_strike_whiff_rate_last_3,
    h.hitter_weighted_whiff_rate_vs_rhp_last_3, h.hitter_weighted_whiff_rate_vs_lhp_last_3,

    h.hitter_avg_whiff_rate_last_5, h.hitter_avg_contact_rate_last_5,
    h.hitter_avg_chase_rate_last_5, h.hitter_avg_csw_against_rate_last_5,
    h.hitter_avg_two_strike_whiff_rate_last_5, h.hitter_avg_exit_velocity_last_5,
    h.hitter_avg_xwoba_last_5,
    h.hitter_avg_whiff_rate_vs_rhp_last_5, h.hitter_avg_whiff_rate_vs_lhp_last_5,
    h.hitter_weighted_whiff_rate_last_5, h.hitter_weighted_contact_rate_last_5,
    h.hitter_weighted_chase_rate_last_5, h.hitter_weighted_csw_against_rate_last_5,
    h.hitter_weighted_two_strike_whiff_rate_last_5,
    h.hitter_weighted_whiff_rate_vs_rhp_last_5, h.hitter_weighted_whiff_rate_vs_lhp_last_5,

    h.hitter_avg_whiff_rate_last_10, h.hitter_avg_contact_rate_last_10,
    h.hitter_avg_chase_rate_last_10, h.hitter_avg_csw_against_rate_last_10,
    h.hitter_avg_two_strike_whiff_rate_last_10, h.hitter_avg_exit_velocity_last_10,
    h.hitter_avg_xwoba_last_10,
    h.hitter_avg_whiff_rate_vs_rhp_last_10, h.hitter_avg_whiff_rate_vs_lhp_last_10,
    h.hitter_weighted_whiff_rate_last_10, h.hitter_weighted_contact_rate_last_10,
    h.hitter_weighted_chase_rate_last_10, h.hitter_weighted_csw_against_rate_last_10,
    h.hitter_weighted_two_strike_whiff_rate_last_10,
    h.hitter_weighted_whiff_rate_vs_rhp_last_10, h.hitter_weighted_whiff_rate_vs_lhp_last_10,

    h.hitter_prev_whiff_rate, h.hitter_prev_contact_rate,
    h.hitter_prev_chase_rate, h.hitter_prev_exit_velocity, h.hitter_prev_xwoba,

    -- ALL PITCHER FEATURES from the pitcher's latest snapshot row p
    p.pitcher_gamesStarted, p.pitcher_days_since_last_appearance,
    p.pitcher_is_reliever,
    p.pitcher_avg_k_last_3, p.pitcher_avg_ip_last_3, p.pitcher_avg_bf_last_3,
    p.pitcher_avg_pitches_last_3, p.pitcher_avg_k9_last_3, p.pitcher_avg_whip_last_3,
    p.pitcher_avg_bb_last_3, p.pitcher_avg_hr_last_3, p.pitcher_avg_hits_last_3,
    p.pitcher_avg_strike_pct_last_3, p.pitcher_avg_kbb_last_3,
    p.pitcher_pct_5plus_ip_last_3, p.pitcher_pct_6plus_ip_last_3, p.pitcher_pct_5plus_k_last_3,

    p.pitcher_avg_k_last_5, p.pitcher_avg_ip_last_5, p.pitcher_avg_bf_last_5,
    p.pitcher_avg_pitches_last_5, p.pitcher_avg_k9_last_5, p.pitcher_avg_whip_last_5,
    p.pitcher_avg_bb_last_5, p.pitcher_avg_hr_last_5, p.pitcher_avg_hits_last_5,
    p.pitcher_avg_strike_pct_last_5, p.pitcher_avg_kbb_last_5,
    p.pitcher_pct_5plus_ip_last_5, p.pitcher_pct_6plus_ip_last_5, p.pitcher_pct_5plus_k_last_5,

    p.pitcher_avg_k_last_10, p.pitcher_avg_ip_last_10, p.pitcher_avg_bf_last_10,
    p.pitcher_avg_pitches_last_10, p.pitcher_avg_k9_last_10, p.pitcher_avg_whip_last_10,
    p.pitcher_avg_bb_last_10, p.pitcher_avg_hr_last_10, p.pitcher_avg_hits_last_10,
    p.pitcher_avg_strike_pct_last_10, p.pitcher_avg_kbb_last_10,
    p.pitcher_pct_5plus_ip_last_10, p.pitcher_pct_6plus_ip_last_10, p.pitcher_pct_5plus_k_last_10,

    p.pitcher_weighted_k_per_bf_last_3, p.pitcher_weighted_bb_per_bf_last_3,
    p.pitcher_weighted_baa_last_3, p.pitcher_weighted_hr_per_bf_last_3,
    p.pitcher_weighted_strike_pct_last_3, p.pitcher_weighted_pitches_per_inning_last_3,
    p.pitcher_weighted_k9_last_3, p.pitcher_weighted_kbb_last_3,
    p.pitcher_weighted_k_per_bf_last_5, p.pitcher_weighted_bb_per_bf_last_5,
    p.pitcher_weighted_baa_last_5, p.pitcher_weighted_hr_per_bf_last_5,
    p.pitcher_weighted_strike_pct_last_5, p.pitcher_weighted_pitches_per_inning_last_5,
    p.pitcher_weighted_k9_last_5, p.pitcher_weighted_kbb_last_5,
    p.pitcher_weighted_k_per_bf_last_10, p.pitcher_weighted_bb_per_bf_last_10,
    p.pitcher_weighted_baa_last_10, p.pitcher_weighted_hr_per_bf_last_10,
    p.pitcher_weighted_strike_pct_last_10, p.pitcher_weighted_pitches_per_inning_last_10,
    p.pitcher_weighted_k9_last_10, p.pitcher_weighted_kbb_last_10,

    p.pitcher_prev_k, p.pitcher_prev_ip, p.pitcher_prev_bf,
    p.pitcher_prev_pitches, p.pitcher_prev_k9,

    -- Pitcher Statcast features
    p.pitcher_avg_whiff_rate_last_3, p.pitcher_avg_csw_rate_last_3,
    p.pitcher_avg_putaway_rate_last_3, p.pitcher_avg_swing_rate_last_3,
    p.pitcher_avg_chase_rate_last_3, p.pitcher_avg_zone_rate_last_3,
    p.pitcher_avg_whiff_rate_0_2_last_3, p.pitcher_avg_whiff_rate_1_2_last_3,
    p.pitcher_avg_whiff_rate_2_2_last_3,
    p.pitcher_avg_velocity_last_3, p.pitcher_avg_spin_rate_last_3,
    p.pitcher_avg_extension_last_3,
    p.pitcher_avg_ff_pct_last_3, p.pitcher_avg_si_pct_last_3,
    p.pitcher_avg_fc_pct_last_3, p.pitcher_avg_sl_pct_last_3,
    p.pitcher_avg_cu_pct_last_3, p.pitcher_avg_ch_pct_last_3,
    p.pitcher_avg_fs_pct_last_3,
    p.pitcher_avg_sl_whiff_rate_last_3, p.pitcher_avg_ff_whiff_rate_last_3,
    p.pitcher_avg_whiff_vs_rhb_last_3, p.pitcher_avg_whiff_vs_lhb_last_3,

    p.pitcher_avg_whiff_rate_last_5, p.pitcher_avg_csw_rate_last_5,
    p.pitcher_avg_putaway_rate_last_5, p.pitcher_avg_swing_rate_last_5,
    p.pitcher_avg_chase_rate_last_5, p.pitcher_avg_zone_rate_last_5,
    p.pitcher_avg_velocity_last_5, p.pitcher_avg_spin_rate_last_5,
    p.pitcher_avg_sl_whiff_rate_last_5, p.pitcher_avg_ff_whiff_rate_last_5,
    p.pitcher_avg_whiff_vs_rhb_last_5, p.pitcher_avg_whiff_vs_lhb_last_5,

    p.pitcher_avg_whiff_rate_last_10, p.pitcher_avg_csw_rate_last_10,
    p.pitcher_avg_putaway_rate_last_10, p.pitcher_avg_swing_rate_last_10,
    p.pitcher_avg_chase_rate_last_10, p.pitcher_avg_zone_rate_last_10,
    p.pitcher_avg_velocity_last_10, p.pitcher_avg_spin_rate_last_10,
    p.pitcher_avg_sl_whiff_rate_last_10, p.pitcher_avg_ff_whiff_rate_last_10,
    p.pitcher_avg_whiff_vs_rhb_last_10, p.pitcher_avg_whiff_vs_lhb_last_10,

    p.pitcher_weighted_whiff_rate_last_3, p.pitcher_weighted_csw_rate_last_3,
    p.pitcher_weighted_sc_strike_rate_last_3, p.pitcher_weighted_chase_rate_last_3,
    p.pitcher_weighted_putaway_rate_last_3,
    p.pitcher_weighted_whiff_rate_vs_rhb_last_3, p.pitcher_weighted_whiff_rate_vs_lhb_last_3,
    p.pitcher_weighted_whiff_rate_last_5, p.pitcher_weighted_csw_rate_last_5,
    p.pitcher_weighted_sc_strike_rate_last_5, p.pitcher_weighted_chase_rate_last_5,
    p.pitcher_weighted_putaway_rate_last_5,
    p.pitcher_weighted_whiff_rate_vs_rhb_last_5, p.pitcher_weighted_whiff_rate_vs_lhb_last_5,
    p.pitcher_weighted_whiff_rate_last_10, p.pitcher_weighted_csw_rate_last_10,
    p.pitcher_weighted_sc_strike_rate_last_10, p.pitcher_weighted_chase_rate_last_10,
    p.pitcher_weighted_putaway_rate_last_10,
    p.pitcher_weighted_whiff_rate_vs_rhb_last_10, p.pitcher_weighted_whiff_rate_vs_lhb_last_10,

    p.pitcher_prev_whiff_rate, p.pitcher_prev_csw_rate,
    p.pitcher_prev_chase_rate, p.pitcher_prev_velocity, p.pitcher_prev_spin_rate,

    -- Head-to-head features (NULL for future games is fine - trees handle missing)
    NULL AS h2h_career_pa, NULL AS h2h_career_k, NULL AS h2h_career_bb,
    NULL AS h2h_career_hits, NULL AS h2h_career_hr, NULL AS h2h_career_games,
    NULL AS h2h_career_k_rate, NULL AS h2h_career_bb_rate, NULL AS h2h_career_hit_rate,
    1 AS is_first_matchup, NULL AS h2h_sample_small,
    NULL AS h2h_sample_medium, NULL AS h2h_sample_large

FROM games_with_pitchers gp
CROSS APPLY (
    SELECT * FROM estimated_lineups el WHERE el.hitter_team_id = gp.future_opponent_team_id
) el
LEFT JOIN latest_hitter_snapshots h
       ON h.hitter_id = el.hitter_id
      AND h.recency_rank = 1
LEFT JOIN latest_pitcher_snapshots p
       ON p.pitcher_id = gp.future_pitcher_id
      AND p.recency_rank = 1
LEFT JOIN dbo.dim_player bat ON bat.player_id = el.hitter_id
LEFT JOIN dbo.dim_player pit ON pit.player_id = gp.future_pitcher_id;
GO

-- ============================================================================
-- Verification queries
-- ============================================================================

-- 1. How many future games are ready, and how many missing values do we have?
SELECT
    game_date,
    COUNT(DISTINCT gamePk) AS unique_games,
    COUNT(DISTINCT pitcher_id) AS unique_pitchers,
    COUNT(*) AS synthetic_matchup_rows,
    SUM(CASE WHEN hitter_bats IS NULL THEN 1 ELSE 0 END) AS missing_hitter_bats,
    SUM(CASE WHEN pitcher_throws IS NULL THEN 1 ELSE 0 END) AS missing_pitcher_throws,
    SUM(CASE WHEN hitter_avg_whiff_rate_last_3 IS NULL THEN 1 ELSE 0 END) AS missing_hitter_whiff,
    SUM(CASE WHEN pitcher_avg_whiff_rate_last_3 IS NULL THEN 1 ELSE 0 END) AS missing_pitcher_whiff
FROM mlb.dbo.fact_future_matchups
GROUP BY game_date
ORDER BY game_date;

-- 2. Sample preview of tomorrow's first game
SELECT TOP 15
    game_date,
    pitcher_name, pitcher_throws,
    hitter_name, hitter_bats, platoon_matchup,
    lineup_rank_estimate,
    hitter_avg_whiff_rate_last_5,
    pitcher_avg_whiff_rate_last_5
FROM mlb.dbo.fact_future_matchups
WHERE game_date = (SELECT MIN(game_date) FROM mlb.dbo.fact_future_matchups WHERE game_date >= CAST(GETDATE() AS DATE))
ORDER BY pitcher_name, lineup_rank_estimate;
