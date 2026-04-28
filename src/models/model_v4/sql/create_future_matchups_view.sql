-- ============================================================================
-- Build synthetic future-game matchup rows
-- ----------------------------------------------------------------------------
-- For each upcoming game with a probable pitcher, generates ~9 rows
-- (one per estimated lineup hitter) shaped like the training feature view.
--
-- LINEUP ESTIMATION STRATEGY:
--   For each opposing team, we take the 9 hitters who appeared most
--   frequently in the team's last 7 completed games. This approximates
--   the typical starting lineup. Pitchers and bench-only players are
--   excluded by filtering on hitter_plate_appearances >= 1 in those games.
--
-- WHY NOT EXACT LINEUPS:
--   Real lineups aren't confirmed until 2-4 hours before first pitch.
--   The "9 most frequent" approach is the best estimate we can produce
--   the morning before. If a star sits, the prediction will be slightly
--   off - but on average across many games, this approximation is fine.
--
-- HOW TO USE:
--   1. Run fetch_probable_pitchers.py to populate dim_probable_pitchers
--   2. Query fact_future_matchups - returns synthetic rows ready for scoring
-- ============================================================================

USE mlb;
GO

IF OBJECT_ID('dbo.fact_future_matchups', 'V') IS NOT NULL
    DROP VIEW dbo.fact_future_matchups;
GO

CREATE VIEW dbo.fact_future_matchups AS
WITH
-- Step 1: Identify upcoming scheduled games with probable pitchers
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
    WHERE s.gameType = 'R'                              -- Regular season only
      AND s.season = YEAR(GETDATE())
      AND CAST(s.gameDate AS DATE) >= CAST(GETDATE() AS DATE)
      AND s.status_detailedState IN ('Scheduled', 'Pre-Game', 'Warmup', 'Preview')
),

-- Step 2: Attach probable pitchers from dim_probable_pitchers
games_with_pitchers AS (
    SELECT
        g.gamePk,
        g.game_date,
        g.season,
        pp.pitcher_id,
        pp.pitcher_name,
        pp.team_id AS pitcher_team_id,
        pp.team_name AS pitcher_team_name,
        -- The opponent is whichever team the pitcher's team isn't
        CASE WHEN pp.side = 'home' THEN g.away_team_id  ELSE g.home_team_id  END AS opponent_team_id,
        CASE WHEN pp.side = 'home' THEN g.away_team_name ELSE g.home_team_name END AS opponent_team_name
    FROM upcoming_games g
    JOIN dbo.dim_probable_pitchers pp ON pp.gamePk = g.gamePk
    WHERE pp.pitcher_id IS NOT NULL
),

-- Step 3: For each opposing team, find their 9 most-frequent recent hitters.
-- We look at the last 30 days of regular-season data. We rank hitters by
-- frequency (how many games they appeared in) and PA volume. The top 9
-- are our estimated starting lineup.
recent_team_hitters AS (
    SELECT
        h.team_id,
        h.player_id AS hitter_id,
        h.player_name AS hitter_name,
        COUNT(DISTINCT h.gamePk) AS games_played,
        SUM(COALESCE(h.avg_pa_last_3, 0)) AS recent_pa_proxy,
        MAX(h.gamePk) AS most_recent_gamePk,
        ROW_NUMBER() OVER (
            PARTITION BY h.team_id
            ORDER BY COUNT(DISTINCT h.gamePk) DESC, MAX(h.gamePk) DESC
        ) AS rank_in_lineup
    FROM dbo.fact_hitter_rolling_featuresv2 h
    WHERE h.game_date >= DATEADD(day, -30, CAST(GETDATE() AS DATE))
      AND h.season = YEAR(GETDATE())
    GROUP BY h.team_id, h.player_id, h.player_name
),

estimated_lineups AS (
    SELECT *
    FROM recent_team_hitters
    WHERE rank_in_lineup <= 9
),

-- Step 4: For each (pitcher game, lineup hitter), look up the most recent
-- hitter rolling features as of "today minus 1 day" (i.e. the latest data
-- we'd have BEFORE tomorrow's game).
latest_hitter_features AS (
    SELECT h.*,
           ROW_NUMBER() OVER (
               PARTITION BY h.player_id
               ORDER BY h.game_date DESC, h.gamePk DESC
           ) AS recency_rank
    FROM dbo.fact_hitter_rolling_featuresv2 h
    WHERE h.game_date < CAST(GETDATE() AS DATE)
      AND h.season = YEAR(GETDATE())
),

-- Step 5: For each pitcher, look up their most recent rolling features
latest_pitcher_features AS (
    SELECT p.*,
           ROW_NUMBER() OVER (
               PARTITION BY p.player_id
               ORDER BY p.game_date DESC, p.gamePk DESC
           ) AS recency_rank
    FROM dbo.fact_pitcher_model_featuresv2 p
    WHERE p.game_date < CAST(GETDATE() AS DATE)
      AND p.season = YEAR(GETDATE())
)

-- Step 6: Final assembly - one row per (pitcher game, lineup hitter)
SELECT
    -- Identifiers
    gp.gamePk,
    gp.game_date,
    gp.season,
    h.player_id AS hitter_id,
    h.player_name AS hitter_name,
    h.position AS hitter_position,
    el.team_id AS hitter_team_id,
    (SELECT TOP 1 team_name FROM dbo.fact_hitter_rolling_featuresv2
     WHERE team_id = el.team_id ORDER BY game_date DESC) AS hitter_team_name,
    NULL AS hitter_lineup_position,           -- unknown until lineup confirmed
    el.rank_in_lineup AS lineup_rank_estimate, -- our estimated batting order
    gp.pitcher_id,
    gp.pitcher_name,
    gp.pitcher_team_id,
    gp.pitcher_team_name,

    -- Synthetic sample weight - assume each hitter takes 3 PAs vs starter
    -- (typical for a 6-inning outing facing the lineup ~3 times through)
    3 AS hitter_plate_appearances,
    NULL AS hitter_strikeouts,                -- unknown - game hasn't happened
    1 AS pitcher_is_starter,
    1 AS first_inning_faced,
    NULL AS y_k_rate,                          -- target unknown
    3 AS sample_weight,

    -- Handedness from dim_player
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

    -- All hitter rolling features (most recent observation)
    h.avg_k_last_3, h.avg_pa_last_3, h.avg_ab_last_3, h.avg_hits_last_3,
    h.avg_hr_last_3, h.avg_bb_last_3, h.avg_pitches_last_3, h.avg_tb_last_3,
    h.avg_rbi_last_3, h.avg_obp_last_3, h.avg_slg_last_3, h.avg_ops_last_3,
    h.avg_batting_avg_last_3, h.avg_k_rate_last_3, h.avg_walk_rate_last_3,
    h.avg_hit_rate_last_3, h.avg_tb_rate_last_3, h.avg_hr_rate_last_3,
    h.sum_pa_last_3, h.sum_ab_last_3, h.pct_1plus_k_last_3, h.pct_2plus_k_last_3,
    h.weighted_k_rate_last_3, h.weighted_walk_rate_last_3,
    h.weighted_hit_rate_last_3, h.weighted_tb_rate_last_3,
    h.weighted_hr_rate_last_3, h.weighted_batting_avg_last_3,
    h.weighted_pitches_per_pa_last_3, h.weighted_obp_last_3,
    h.weighted_slg_last_3, h.weighted_ops_last_3,

    h.avg_k_last_5, h.avg_pa_last_5, h.avg_ab_last_5, h.avg_hits_last_5,
    h.avg_hr_last_5, h.avg_bb_last_5, h.avg_pitches_last_5,
    h.avg_obp_last_5, h.avg_slg_last_5, h.avg_ops_last_5,
    h.avg_batting_avg_last_5, h.avg_k_rate_last_5, h.avg_walk_rate_last_5,
    h.avg_hit_rate_last_5, h.avg_tb_rate_last_5, h.avg_hr_rate_last_5,
    h.sum_pa_last_5, h.sum_ab_last_5, h.pct_1plus_k_last_5, h.pct_2plus_k_last_5,
    h.weighted_k_rate_last_5, h.weighted_walk_rate_last_5,
    h.weighted_hit_rate_last_5, h.weighted_tb_rate_last_5,
    h.weighted_hr_rate_last_5, h.weighted_batting_avg_last_5,
    h.weighted_pitches_per_pa_last_5, h.weighted_obp_last_5,
    h.weighted_slg_last_5, h.weighted_ops_last_5,

    h.avg_k_last_10, h.avg_pa_last_10, h.avg_ab_last_10, h.avg_hits_last_10,
    h.avg_hr_last_10, h.avg_bb_last_10, h.avg_pitches_last_10,
    h.avg_obp_last_10, h.avg_slg_last_10, h.avg_ops_last_10,
    h.avg_batting_avg_last_10, h.avg_k_rate_last_10, h.avg_walk_rate_last_10,
    h.avg_hit_rate_last_10, h.avg_tb_rate_last_10, h.avg_hr_rate_last_10,
    h.sum_pa_last_10, h.sum_ab_last_10, h.pct_1plus_k_last_10, h.pct_2plus_k_last_10,
    h.weighted_k_rate_last_10, h.weighted_walk_rate_last_10,
    h.weighted_hit_rate_last_10, h.weighted_tb_rate_last_10,
    h.weighted_hr_rate_last_10, h.weighted_batting_avg_last_10,
    h.weighted_pitches_per_pa_last_10, h.weighted_obp_last_10,
    h.weighted_slg_last_10, h.weighted_ops_last_10,

    h.prev_k AS hitter_prev_k, h.prev_pa AS hitter_prev_pa,
    h.prev_ab AS hitter_prev_ab, h.prev_hits AS hitter_prev_hits,
    h.prev_hr AS hitter_prev_hr, h.prev_bb AS hitter_prev_bb,
    h.prev_ops AS hitter_prev_ops, h.prev_k_rate AS hitter_prev_k_rate,

    -- All pitcher rolling features
    p.gamesStarted AS pitcher_gamesStarted,
    p.days_since_last_appearance AS pitcher_days_since_last_appearance,
    p.avg_k_last_3 AS pitcher_avg_k_last_3,
    p.avg_ip_last_3 AS pitcher_avg_ip_last_3,
    p.avg_bf_last_3 AS pitcher_avg_bf_last_3,
    p.avg_pitches_last_3 AS pitcher_avg_pitches_last_3,
    p.avg_k9_last_3 AS pitcher_avg_k9_last_3,
    p.avg_whip_last_3 AS pitcher_avg_whip_last_3,
    p.avg_bb_last_3 AS pitcher_avg_bb_last_3,
    p.avg_hr_last_3 AS pitcher_avg_hr_last_3,
    p.avg_hits_last_3 AS pitcher_avg_hits_last_3,
    p.avg_strike_pct_last_3 AS pitcher_avg_strike_pct_last_3,
    p.avg_kbb_last_3 AS pitcher_avg_kbb_last_3,
    p.pct_5plus_ip_last_3 AS pitcher_pct_5plus_ip_last_3,
    p.pct_6plus_ip_last_3 AS pitcher_pct_6plus_ip_last_3,
    p.pct_5plus_k_last_3 AS pitcher_pct_5plus_k_last_3,

    p.avg_k_last_5 AS pitcher_avg_k_last_5,
    p.avg_ip_last_5 AS pitcher_avg_ip_last_5,
    p.avg_bf_last_5 AS pitcher_avg_bf_last_5,
    p.avg_pitches_last_5 AS pitcher_avg_pitches_last_5,
    p.avg_k9_last_5 AS pitcher_avg_k9_last_5,
    p.avg_whip_last_5 AS pitcher_avg_whip_last_5,
    p.avg_bb_last_5 AS pitcher_avg_bb_last_5,
    p.avg_hr_last_5 AS pitcher_avg_hr_last_5,
    p.avg_hits_last_5 AS pitcher_avg_hits_last_5,
    p.avg_strike_pct_last_5 AS pitcher_avg_strike_pct_last_5,
    p.avg_kbb_last_5 AS pitcher_avg_kbb_last_5,
    p.pct_5plus_ip_last_5 AS pitcher_pct_5plus_ip_last_5,
    p.pct_6plus_ip_last_5 AS pitcher_pct_6plus_ip_last_5,
    p.pct_5plus_k_last_5 AS pitcher_pct_5plus_k_last_5,

    p.avg_k_last_10 AS pitcher_avg_k_last_10,
    p.avg_ip_last_10 AS pitcher_avg_ip_last_10,
    p.avg_bf_last_10 AS pitcher_avg_bf_last_10,
    p.avg_pitches_last_10 AS pitcher_avg_pitches_last_10,
    p.avg_k9_last_10 AS pitcher_avg_k9_last_10,
    p.avg_whip_last_10 AS pitcher_avg_whip_last_10,
    p.avg_bb_last_10 AS pitcher_avg_bb_last_10,
    p.avg_hr_last_10 AS pitcher_avg_hr_last_10,
    p.avg_hits_last_10 AS pitcher_avg_hits_last_10,
    p.avg_strike_pct_last_10 AS pitcher_avg_strike_pct_last_10,
    p.avg_kbb_last_10 AS pitcher_avg_kbb_last_10,
    p.pct_5plus_ip_last_10 AS pitcher_pct_5plus_ip_last_10,
    p.pct_6plus_ip_last_10 AS pitcher_pct_6plus_ip_last_10,
    p.pct_5plus_k_last_10 AS pitcher_pct_5plus_k_last_10,

    p.weighted_k_per_bf_last_3 AS pitcher_weighted_k_per_bf_last_3,
    p.weighted_bb_per_bf_last_3 AS pitcher_weighted_bb_per_bf_last_3,
    p.weighted_baa_last_3 AS pitcher_weighted_baa_last_3,
    p.weighted_hr_per_bf_last_3 AS pitcher_weighted_hr_per_bf_last_3,
    p.weighted_strike_pct_last_3 AS pitcher_weighted_strike_pct_last_3,
    p.weighted_pitches_per_inning_last_3 AS pitcher_weighted_pitches_per_inning_last_3,
    p.weighted_k9_last_3 AS pitcher_weighted_k9_last_3,
    p.weighted_kbb_last_3 AS pitcher_weighted_kbb_last_3,
    p.weighted_k_per_bf_last_5 AS pitcher_weighted_k_per_bf_last_5,
    p.weighted_bb_per_bf_last_5 AS pitcher_weighted_bb_per_bf_last_5,
    p.weighted_baa_last_5 AS pitcher_weighted_baa_last_5,
    p.weighted_hr_per_bf_last_5 AS pitcher_weighted_hr_per_bf_last_5,
    p.weighted_strike_pct_last_5 AS pitcher_weighted_strike_pct_last_5,
    p.weighted_pitches_per_inning_last_5 AS pitcher_weighted_pitches_per_inning_last_5,
    p.weighted_k9_last_5 AS pitcher_weighted_k9_last_5,
    p.weighted_kbb_last_5 AS pitcher_weighted_kbb_last_5,
    p.weighted_k_per_bf_last_10 AS pitcher_weighted_k_per_bf_last_10,
    p.weighted_bb_per_bf_last_10 AS pitcher_weighted_bb_per_bf_last_10,
    p.weighted_baa_last_10 AS pitcher_weighted_baa_last_10,
    p.weighted_hr_per_bf_last_10 AS pitcher_weighted_hr_per_bf_last_10,
    p.weighted_strike_pct_last_10 AS pitcher_weighted_strike_pct_last_10,
    p.weighted_pitches_per_inning_last_10 AS pitcher_weighted_pitches_per_inning_last_10,
    p.weighted_k9_last_10 AS pitcher_weighted_k9_last_10,
    p.weighted_kbb_last_10 AS pitcher_weighted_kbb_last_10,

    p.prev_k AS pitcher_prev_k,
    p.prev_ip AS pitcher_prev_ip,
    p.prev_bf AS pitcher_prev_bf,
    p.prev_pitches AS pitcher_prev_pitches,
    p.prev_k9 AS pitcher_prev_k9,

    p.avg_whiff_rate_last_3 AS pitcher_avg_whiff_rate_last_3,
    p.avg_csw_rate_last_3 AS pitcher_avg_csw_rate_last_3,
    p.avg_putaway_rate_last_3 AS pitcher_avg_putaway_rate_last_3,
    p.avg_swing_rate_last_3 AS pitcher_avg_swing_rate_last_3,
    p.avg_chase_rate_last_3 AS pitcher_avg_chase_rate_last_3,
    p.avg_zone_rate_last_3 AS pitcher_avg_zone_rate_last_3,
    p.avg_velocity_last_3 AS pitcher_avg_velocity_last_3,
    p.avg_spin_rate_last_3 AS pitcher_avg_spin_rate_last_3,
    p.avg_extension_last_3 AS pitcher_avg_extension_last_3,
    p.avg_ff_pct_last_3 AS pitcher_avg_ff_pct_last_3,
    p.avg_si_pct_last_3 AS pitcher_avg_si_pct_last_3,
    p.avg_fc_pct_last_3 AS pitcher_avg_fc_pct_last_3,
    p.avg_sl_pct_last_3 AS pitcher_avg_sl_pct_last_3,
    p.avg_cu_pct_last_3 AS pitcher_avg_cu_pct_last_3,
    p.avg_ch_pct_last_3 AS pitcher_avg_ch_pct_last_3,
    p.avg_fs_pct_last_3 AS pitcher_avg_fs_pct_last_3,
    p.avg_sl_whiff_rate_last_3 AS pitcher_avg_sl_whiff_rate_last_3,
    p.avg_ff_whiff_rate_last_3 AS pitcher_avg_ff_whiff_rate_last_3,
    p.avg_whiff_vs_rhb_last_3 AS pitcher_avg_whiff_vs_rhb_last_3,
    p.avg_whiff_vs_lhb_last_3 AS pitcher_avg_whiff_vs_lhb_last_3,

    p.avg_whiff_rate_last_5 AS pitcher_avg_whiff_rate_last_5,
    p.avg_csw_rate_last_5 AS pitcher_avg_csw_rate_last_5,
    p.avg_putaway_rate_last_5 AS pitcher_avg_putaway_rate_last_5,
    p.avg_swing_rate_last_5 AS pitcher_avg_swing_rate_last_5,
    p.avg_chase_rate_last_5 AS pitcher_avg_chase_rate_last_5,
    p.avg_zone_rate_last_5 AS pitcher_avg_zone_rate_last_5,
    p.avg_velocity_last_5 AS pitcher_avg_velocity_last_5,
    p.avg_spin_rate_last_5 AS pitcher_avg_spin_rate_last_5,
    p.avg_sl_whiff_rate_last_5 AS pitcher_avg_sl_whiff_rate_last_5,
    p.avg_ff_whiff_rate_last_5 AS pitcher_avg_ff_whiff_rate_last_5,
    p.avg_whiff_vs_rhb_last_5 AS pitcher_avg_whiff_vs_rhb_last_5,
    p.avg_whiff_vs_lhb_last_5 AS pitcher_avg_whiff_vs_lhb_last_5,

    p.avg_whiff_rate_last_10 AS pitcher_avg_whiff_rate_last_10,
    p.avg_csw_rate_last_10 AS pitcher_avg_csw_rate_last_10,
    p.avg_putaway_rate_last_10 AS pitcher_avg_putaway_rate_last_10,
    p.avg_swing_rate_last_10 AS pitcher_avg_swing_rate_last_10,
    p.avg_chase_rate_last_10 AS pitcher_avg_chase_rate_last_10,
    p.avg_zone_rate_last_10 AS pitcher_avg_zone_rate_last_10,
    p.avg_velocity_last_10 AS pitcher_avg_velocity_last_10,
    p.avg_spin_rate_last_10 AS pitcher_avg_spin_rate_last_10,
    p.avg_sl_whiff_rate_last_10 AS pitcher_avg_sl_whiff_rate_last_10,
    p.avg_ff_whiff_rate_last_10 AS pitcher_avg_ff_whiff_rate_last_10,
    p.avg_whiff_vs_rhb_last_10 AS pitcher_avg_whiff_vs_rhb_last_10,
    p.avg_whiff_vs_lhb_last_10 AS pitcher_avg_whiff_vs_lhb_last_10,

    p.weighted_whiff_rate_last_3 AS pitcher_weighted_whiff_rate_last_3,
    p.weighted_csw_rate_last_3 AS pitcher_weighted_csw_rate_last_3,
    p.weighted_chase_rate_last_3 AS pitcher_weighted_chase_rate_last_3,
    p.weighted_putaway_rate_last_3 AS pitcher_weighted_putaway_rate_last_3,
    p.weighted_whiff_rate_vs_rhb_last_3 AS pitcher_weighted_whiff_rate_vs_rhb_last_3,
    p.weighted_whiff_rate_vs_lhb_last_3 AS pitcher_weighted_whiff_rate_vs_lhb_last_3,
    p.weighted_whiff_rate_last_5 AS pitcher_weighted_whiff_rate_last_5,
    p.weighted_csw_rate_last_5 AS pitcher_weighted_csw_rate_last_5,
    p.weighted_chase_rate_last_5 AS pitcher_weighted_chase_rate_last_5,
    p.weighted_putaway_rate_last_5 AS pitcher_weighted_putaway_rate_last_5,
    p.weighted_whiff_rate_vs_rhb_last_5 AS pitcher_weighted_whiff_rate_vs_rhb_last_5,
    p.weighted_whiff_rate_vs_lhb_last_5 AS pitcher_weighted_whiff_rate_vs_lhb_last_5,
    p.weighted_whiff_rate_last_10 AS pitcher_weighted_whiff_rate_last_10,
    p.weighted_csw_rate_last_10 AS pitcher_weighted_csw_rate_last_10,
    p.weighted_chase_rate_last_10 AS pitcher_weighted_chase_rate_last_10,
    p.weighted_putaway_rate_last_10 AS pitcher_weighted_putaway_rate_last_10,
    p.weighted_whiff_rate_vs_rhb_last_10 AS pitcher_weighted_whiff_rate_vs_rhb_last_10,
    p.weighted_whiff_rate_vs_lhb_last_10 AS pitcher_weighted_whiff_rate_vs_lhb_last_10,

    p.prev_whiff_rate AS pitcher_prev_whiff_rate,
    p.prev_csw_rate AS pitcher_prev_csw_rate,
    p.prev_chase_rate AS pitcher_prev_chase_rate,
    p.prev_velocity AS pitcher_prev_velocity,
    p.prev_spin_rate AS pitcher_prev_spin_rate,

    -- Head-to-head historical features (NULL for future games is fine -
    -- the trees handle missing values natively)
    NULL AS h2h_career_pa, NULL AS h2h_career_k, NULL AS h2h_career_bb,
    NULL AS h2h_career_hits, NULL AS h2h_career_hr, NULL AS h2h_career_games,
    NULL AS h2h_career_k_rate, NULL AS h2h_career_bb_rate, NULL AS h2h_career_hit_rate,
    1 AS is_first_matchup, NULL AS h2h_sample_small,
    NULL AS h2h_sample_medium, NULL AS h2h_sample_large

FROM games_with_pitchers gp
CROSS APPLY (
    SELECT * FROM estimated_lineups el
    WHERE el.team_id = gp.opponent_team_id
) el
LEFT JOIN latest_hitter_features h
       ON h.player_id = el.hitter_id
      AND h.recency_rank = 1
LEFT JOIN latest_pitcher_features p
       ON p.player_id = gp.pitcher_id
      AND p.recency_rank = 1
LEFT JOIN dbo.dim_player bat ON bat.player_id = el.hitter_id
LEFT JOIN dbo.dim_player pit ON pit.player_id = gp.pitcher_id;
GO

-- ============================================================================
-- Verification queries
-- ============================================================================

-- How many future games do we have ready to score?
SELECT
    game_date,
    COUNT(DISTINCT gamePk) AS unique_games,
    COUNT(DISTINCT pitcher_id) AS unique_pitchers,
    COUNT(*) AS synthetic_matchup_rows,
    SUM(CASE WHEN hitter_bats IS NULL THEN 1 ELSE 0 END) AS missing_hitter_bats,
    SUM(CASE WHEN pitcher_throws IS NULL THEN 1 ELSE 0 END) AS missing_pitcher_throws
FROM mlb.dbo.fact_future_matchups
GROUP BY game_date
ORDER BY game_date;

-- Sample preview of tomorrow's games
SELECT TOP 20
    game_date, pitcher_name, pitcher_throws,
    hitter_name, hitter_bats, platoon_matchup,
    lineup_rank_estimate
FROM mlb.dbo.fact_future_matchups
WHERE game_date = DATEADD(day, 1, CAST(GETDATE() AS DATE))
ORDER BY pitcher_name, lineup_rank_estimate;
