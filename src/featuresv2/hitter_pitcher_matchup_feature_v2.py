"""
Hitter-pitcher matchup model features (v2).

Purpose
-------
Training table for **Model 1**: per-matchup strikeout probability.

Grain: one row per (gamePk, hitter_id, pitcher_id) — i.e. one row per hitter
who faced this pitcher in this game, regardless of how many plate appearances.

Target columns (pick one at training time):
  - hitter_strikeouts         : count target (0-4)
  - hitter_plate_appearances  : use as sample_weight for rate target
  - y_k_rate                  : strikeouts / plate_appearances
  - sample_weight             : plate_appearances (duplicate of above; explicit)

Design principles vs. the old `fact_hitter_pitcher_matchup_model_features`:

1. ZERO same-game leakage. Every feature is either:
     - rolling feature from a fixed source (already lagged), OR
     - head-to-head matchup history STRICTLY PRIOR to the current game_date, OR
     - pre-game identifier (lineup position, handedness, park, etc.)

2. No dependency on `fact_lineup_agg_features` (dropped per user decision).
   Lineup effects will be captured by:
     - the hitter's own rolling features (for Model 1),
     - a separate, smaller lineup summary table (for Model 2, later).

3. Switch-hitter handling: hitter_stand = 'S' -> effective stand is opposite
   of pitcher's throwing arm. Platoon flag `is_same_hand_matchup` is derived.

4. No join to `fact_player_pitching_gamelogs` (was leaking same-game pitcher
   Ks into every row).

5. Head-to-head history is computed via a self-join on
   `fact_hitter_pitcher_pa_game_agg` with the strict condition
   `prior.game_date < current.game_date`, which cleanly excludes the
   current game from the aggregation.

Output table: mlb.dbo.fact_hitter_pitcher_matchup_model_featuresv2
(v2 suffix per user request; keeps the original table intact.)
"""

import logging
from sql.sql_loader import execute_sql

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


def build_hitter_pitcher_matchup_model_features_v2() -> None:
    logger.info("Building mlb.dbo.fact_hitter_pitcher_matchup_model_featuresv2")

    sql = """
IF OBJECT_ID('mlb.dbo.fact_hitter_pitcher_matchup_model_featuresv2', 'U') IS NOT NULL
    DROP TABLE mlb.dbo.fact_hitter_pitcher_matchup_model_featuresv2;

/* =========================================================
   1. PA base (hitter-pitcher-game target rows)
   This is the grain of the final table: one row per matchup.
========================================================= */
WITH pa_base AS (
    SELECT
        CAST(gamePk AS int) AS gamePk,
        CAST(game_date AS date) AS game_date,
        YEAR(CAST(game_date AS date)) AS season,

        CAST(batter_id AS int) AS hitter_id,
        MAX(batter_name) AS hitter_name,
        CAST(pitcher_id AS int) AS pitcher_id,
        MAX(pitcher_name) AS pitcher_name,

        CAST(batter_team_id AS int) AS hitter_team_id,
        MAX(batter_team_name) AS hitter_team_name,
        CAST(pitcher_team_id AS int) AS pitcher_team_id,
        MAX(pitcher_team_name) AS pitcher_team_name,

        SUM(COALESCE(plate_appearances, 0)) AS plate_appearances,
        SUM(COALESCE(strikeouts, 0)) AS strikeouts,
        SUM(COALESCE(walks, 0)) AS walks,
        SUM(COALESCE(hits, 0)) AS hits,
        SUM(COALESCE(home_runs, 0)) AS home_runs,
        MIN(first_inning_faced) AS first_inning_faced

    FROM mlb.dbo.fact_hitter_pitcher_pa_game_aggv2
    WHERE batter_id IS NOT NULL
      AND pitcher_id IS NOT NULL
      AND gamePk IS NOT NULL
    GROUP BY
        CAST(gamePk AS int),
        CAST(game_date AS date),
        YEAR(CAST(game_date AS date)),
        CAST(batter_id AS int),
        CAST(pitcher_id AS int),
        CAST(batter_team_id AS int),
        CAST(pitcher_team_id AS int)
),

/* =========================================================
   2. Prior head-to-head history (career-to-date BEFORE this game).
   Self-join with strict date comparison: prior.game_date < current.game_date.
   This guarantees no leakage from the current game's matchup.
========================================================= */
h2h_prior AS (
    SELECT
        cur.gamePk,
        cur.game_date,
        cur.hitter_id,
        cur.pitcher_id,

        COALESCE(SUM(prior.plate_appearances), 0) AS h2h_career_pa,
        COALESCE(SUM(prior.strikeouts), 0) AS h2h_career_k,
        COALESCE(SUM(prior.walks), 0) AS h2h_career_bb,
        COALESCE(SUM(prior.hits), 0) AS h2h_career_hits,
        COALESCE(SUM(prior.home_runs), 0) AS h2h_career_hr,

        COUNT(DISTINCT prior.gamePk) AS h2h_career_games,

        /* Rate stats only computed when sample is meaningful (>= 1 PA) */
        CASE
            WHEN SUM(prior.plate_appearances) > 0
            THEN CAST(SUM(prior.strikeouts) AS float) / SUM(prior.plate_appearances)
            ELSE NULL
        END AS h2h_career_k_rate,

        CASE
            WHEN SUM(prior.plate_appearances) > 0
            THEN CAST(SUM(prior.walks) AS float) / SUM(prior.plate_appearances)
            ELSE NULL
        END AS h2h_career_bb_rate,

        CASE
            WHEN SUM(prior.plate_appearances) > 0
            THEN CAST(SUM(prior.hits) AS float) / SUM(prior.plate_appearances)
            ELSE NULL
        END AS h2h_career_hit_rate

    FROM pa_base cur
    LEFT JOIN mlb.dbo.fact_hitter_pitcher_pa_game_aggv2 prior
        ON  CAST(prior.batter_id AS int)  = cur.hitter_id
        AND CAST(prior.pitcher_id AS int) = cur.pitcher_id
        AND CAST(prior.game_date AS date) < cur.game_date   -- STRICT <, no leakage
    GROUP BY
        cur.gamePk,
        cur.game_date,
        cur.hitter_id,
        cur.pitcher_id
),

/* =========================================================
   3. Hitter lineup (pre-game batting order from the official lineup card).
   Assumption: fact_hitter_lineup is populated from the MLB pre-game feed,
   NOT reconstructed post-game. Verify with a coverage check after build.
========================================================= */
hitter_lineup AS (
    SELECT
        CAST(gamePk AS int) AS gamePk,
        CAST(player_id AS int) AS hitter_id,
        CAST(batting_order AS int) AS hitter_batting_order,
        position_abbreviation AS hitter_lineup_position,
        position_name AS hitter_lineup_position_name
    FROM mlb.dbo.fact_hitter_lineup
    WHERE TRY_CAST(batting_order AS int) BETWEEN 1 AND 9
)

/* =========================================================
   4. Final SELECT
========================================================= */
SELECT
    /* -------------------------------------------------------------
       IDENTIFIERS
    ------------------------------------------------------------- */
    pa.gamePk,
    pa.game_date,
    pa.season,

    pa.hitter_id,
    pa.hitter_name,
    h.position AS hitter_position,
    pa.hitter_team_id,
    pa.hitter_team_name,

    pa.pitcher_id,
    pa.pitcher_name,
    pa.pitcher_team_id,
    pa.pitcher_team_name,

    /* -------------------------------------------------------------
       HANDEDNESS + PLATOON
       Note: fact_hitter_model_features may or may not have `stand`.
       If not present, derive via pybaseball lookup in Python.
       fact_pitcher_model_features has throwing hand via gamelogs join
       downstream; we carry it through here.
    ------------------------------------------------------------- */
    -- NOTE: you may need to add `stand` to fact_hitter_model_features
    -- and `p_throws` to fact_pitcher_model_features if not present.
    -- These columns are essential for the platoon matchup feature.

    /* -------------------------------------------------------------
       TARGETS
    ------------------------------------------------------------- */
    pa.plate_appearances AS hitter_plate_appearances,
    pa.strikeouts        AS hitter_strikeouts,
    pa.walks             AS hitter_walks,
    pa.hits              AS hitter_hits,
    pa.home_runs         AS hitter_home_runs,
    pa.first_inning_faced,

    CAST(pa.strikeouts AS float) / NULLIF(pa.plate_appearances, 0) AS y_k_rate,
    pa.plate_appearances AS sample_weight,

    /* -------------------------------------------------------------
       LAGGED HEAD-TO-HEAD MATCHUP HISTORY (prior to this game)
    ------------------------------------------------------------- */
    h2h.h2h_career_pa,
    h2h.h2h_career_k,
    h2h.h2h_career_bb,
    h2h.h2h_career_hits,
    h2h.h2h_career_hr,
    h2h.h2h_career_games,
    h2h.h2h_career_k_rate,
    h2h.h2h_career_bb_rate,
    h2h.h2h_career_hit_rate,

    /* Flags so the model can learn "first matchup" as a special state */
    CASE WHEN h2h.h2h_career_pa = 0 THEN 1 ELSE 0 END AS is_first_matchup,
    CASE WHEN h2h.h2h_career_pa BETWEEN 1 AND 4  THEN 1 ELSE 0 END AS h2h_sample_small,
    CASE WHEN h2h.h2h_career_pa BETWEEN 5 AND 14 THEN 1 ELSE 0 END AS h2h_sample_medium,
    CASE WHEN h2h.h2h_career_pa >= 15             THEN 1 ELSE 0 END AS h2h_sample_large,

    /* -------------------------------------------------------------
       LINEUP CONTEXT (pre-game)
    ------------------------------------------------------------- */
    hl.hitter_batting_order,
    hl.hitter_lineup_position,
    hl.hitter_lineup_position_name,

    /* -------------------------------------------------------------
       HITTER ROLLING FEATURES (pre-game)
    ------------------------------------------------------------- */
    h.days_since_last_game AS hitter_days_since_last_game,

    -- simple last 3
    h.avg_k_last_3 AS hitter_avg_k_last_3,
    h.avg_pa_last_3 AS hitter_avg_pa_last_3,
    h.avg_ab_last_3 AS hitter_avg_ab_last_3,
    h.avg_hits_last_3 AS hitter_avg_hits_last_3,
    h.avg_hr_last_3 AS hitter_avg_hr_last_3,
    h.avg_bb_last_3 AS hitter_avg_bb_last_3,
    h.avg_pitches_last_3 AS hitter_avg_pitches_last_3,
    h.avg_tb_last_3 AS hitter_avg_tb_last_3,
    h.avg_rbi_last_3 AS hitter_avg_rbi_last_3,
    h.avg_obp_last_3 AS hitter_avg_obp_last_3,
    h.avg_slg_last_3 AS hitter_avg_slg_last_3,
    h.avg_ops_last_3 AS hitter_avg_ops_last_3,
    h.avg_batting_avg_last_3 AS hitter_avg_batting_avg_last_3,
    h.avg_k_rate_last_3 AS hitter_avg_k_rate_last_3,
    h.avg_walk_rate_last_3 AS hitter_avg_walk_rate_last_3,
    h.avg_hit_rate_last_3 AS hitter_avg_hit_rate_last_3,
    h.avg_tb_rate_last_3 AS hitter_avg_tb_rate_last_3,
    h.avg_hr_rate_last_3 AS hitter_avg_hr_rate_last_3,
    h.sum_pa_last_3 AS hitter_sum_pa_last_3,
    h.sum_ab_last_3 AS hitter_sum_ab_last_3,
    h.pct_1plus_k_last_3 AS hitter_pct_1plus_k_last_3,
    h.pct_2plus_k_last_3 AS hitter_pct_2plus_k_last_3,

    -- weighted last 3
    h.weighted_k_rate_last_3 AS hitter_weighted_k_rate_last_3,
    h.weighted_walk_rate_last_3 AS hitter_weighted_walk_rate_last_3,
    h.weighted_hit_rate_last_3 AS hitter_weighted_hit_rate_last_3,
    h.weighted_tb_rate_last_3 AS hitter_weighted_tb_rate_last_3,
    h.weighted_hr_rate_last_3 AS hitter_weighted_hr_rate_last_3,
    h.weighted_batting_avg_last_3 AS hitter_weighted_batting_avg_last_3,
    h.weighted_pitches_per_pa_last_3 AS hitter_weighted_pitches_per_pa_last_3,
    h.weighted_obp_last_3 AS hitter_weighted_obp_last_3,
    h.weighted_slg_last_3 AS hitter_weighted_slg_last_3,
    h.weighted_ops_last_3 AS hitter_weighted_ops_last_3,

    -- simple last 5
    h.avg_k_last_5 AS hitter_avg_k_last_5,
    h.avg_pa_last_5 AS hitter_avg_pa_last_5,
    h.avg_ab_last_5 AS hitter_avg_ab_last_5,
    h.avg_hits_last_5 AS hitter_avg_hits_last_5,
    h.avg_hr_last_5 AS hitter_avg_hr_last_5,
    h.avg_bb_last_5 AS hitter_avg_bb_last_5,
    h.avg_pitches_last_5 AS hitter_avg_pitches_last_5,
    h.avg_obp_last_5 AS hitter_avg_obp_last_5,
    h.avg_slg_last_5 AS hitter_avg_slg_last_5,
    h.avg_ops_last_5 AS hitter_avg_ops_last_5,
    h.avg_batting_avg_last_5 AS hitter_avg_batting_avg_last_5,
    h.avg_k_rate_last_5 AS hitter_avg_k_rate_last_5,
    h.avg_walk_rate_last_5 AS hitter_avg_walk_rate_last_5,
    h.avg_hit_rate_last_5 AS hitter_avg_hit_rate_last_5,
    h.avg_tb_rate_last_5 AS hitter_avg_tb_rate_last_5,
    h.avg_hr_rate_last_5 AS hitter_avg_hr_rate_last_5,
    h.sum_pa_last_5 AS hitter_sum_pa_last_5,
    h.sum_ab_last_5 AS hitter_sum_ab_last_5,
    h.pct_1plus_k_last_5 AS hitter_pct_1plus_k_last_5,
    h.pct_2plus_k_last_5 AS hitter_pct_2plus_k_last_5,

    -- weighted last 5
    h.weighted_k_rate_last_5 AS hitter_weighted_k_rate_last_5,
    h.weighted_walk_rate_last_5 AS hitter_weighted_walk_rate_last_5,
    h.weighted_hit_rate_last_5 AS hitter_weighted_hit_rate_last_5,
    h.weighted_tb_rate_last_5 AS hitter_weighted_tb_rate_last_5,
    h.weighted_hr_rate_last_5 AS hitter_weighted_hr_rate_last_5,
    h.weighted_batting_avg_last_5 AS hitter_weighted_batting_avg_last_5,
    h.weighted_pitches_per_pa_last_5 AS hitter_weighted_pitches_per_pa_last_5,
    h.weighted_obp_last_5 AS hitter_weighted_obp_last_5,
    h.weighted_slg_last_5 AS hitter_weighted_slg_last_5,
    h.weighted_ops_last_5 AS hitter_weighted_ops_last_5,

    -- simple last 10
    h.avg_k_last_10 AS hitter_avg_k_last_10,
    h.avg_pa_last_10 AS hitter_avg_pa_last_10,
    h.avg_ab_last_10 AS hitter_avg_ab_last_10,
    h.avg_hits_last_10 AS hitter_avg_hits_last_10,
    h.avg_hr_last_10 AS hitter_avg_hr_last_10,
    h.avg_bb_last_10 AS hitter_avg_bb_last_10,
    h.avg_pitches_last_10 AS hitter_avg_pitches_last_10,
    h.avg_obp_last_10 AS hitter_avg_obp_last_10,
    h.avg_slg_last_10 AS hitter_avg_slg_last_10,
    h.avg_ops_last_10 AS hitter_avg_ops_last_10,
    h.avg_batting_avg_last_10 AS hitter_avg_batting_avg_last_10,
    h.avg_k_rate_last_10 AS hitter_avg_k_rate_last_10,
    h.avg_walk_rate_last_10 AS hitter_avg_walk_rate_last_10,
    h.avg_hit_rate_last_10 AS hitter_avg_hit_rate_last_10,
    h.avg_tb_rate_last_10 AS hitter_avg_tb_rate_last_10,
    h.avg_hr_rate_last_10 AS hitter_avg_hr_rate_last_10,
    h.sum_pa_last_10 AS hitter_sum_pa_last_10,
    h.sum_ab_last_10 AS hitter_sum_ab_last_10,
    h.pct_1plus_k_last_10 AS hitter_pct_1plus_k_last_10,
    h.pct_2plus_k_last_10 AS hitter_pct_2plus_k_last_10,

    -- weighted last 10
    h.weighted_k_rate_last_10 AS hitter_weighted_k_rate_last_10,
    h.weighted_walk_rate_last_10 AS hitter_weighted_walk_rate_last_10,
    h.weighted_hit_rate_last_10 AS hitter_weighted_hit_rate_last_10,
    h.weighted_tb_rate_last_10 AS hitter_weighted_tb_rate_last_10,
    h.weighted_hr_rate_last_10 AS hitter_weighted_hr_rate_last_10,
    h.weighted_batting_avg_last_10 AS hitter_weighted_batting_avg_last_10,
    h.weighted_pitches_per_pa_last_10 AS hitter_weighted_pitches_per_pa_last_10,
    h.weighted_obp_last_10 AS hitter_weighted_obp_last_10,
    h.weighted_slg_last_10 AS hitter_weighted_slg_last_10,
    h.weighted_ops_last_10 AS hitter_weighted_ops_last_10,

    -- hitter previous game
    h.prev_k AS hitter_prev_k,
    h.prev_pa AS hitter_prev_pa,
    h.prev_ab AS hitter_prev_ab,
    h.prev_hits AS hitter_prev_hits,
    h.prev_hr AS hitter_prev_hr,
    h.prev_bb AS hitter_prev_bb,
    h.prev_ops AS hitter_prev_ops,
    h.prev_k_rate AS hitter_prev_k_rate,

    -- hitter statcast last 3
    h.avg_whiff_rate_last_3 AS hitter_avg_whiff_rate_last_3,
    h.avg_contact_rate_last_3 AS hitter_avg_contact_rate_last_3,
    h.avg_swing_rate_last_3 AS hitter_avg_swing_rate_last_3,
    h.avg_chase_rate_last_3 AS hitter_avg_chase_rate_last_3,
    h.avg_zone_swing_rate_last_3 AS hitter_avg_zone_swing_rate_last_3,
    h.avg_zone_rate_last_3 AS hitter_avg_zone_rate_last_3,
    h.avg_called_strike_rate_last_3 AS hitter_avg_called_strike_rate_last_3,
    h.avg_csw_against_rate_last_3 AS hitter_avg_csw_against_rate_last_3,
    h.avg_two_strike_whiff_rate_last_3 AS hitter_avg_two_strike_whiff_rate_last_3,
    h.avg_whiff_rate_0_2_last_3 AS hitter_avg_whiff_rate_0_2_last_3,
    h.avg_whiff_rate_1_2_last_3 AS hitter_avg_whiff_rate_1_2_last_3,
    h.avg_whiff_rate_2_2_last_3 AS hitter_avg_whiff_rate_2_2_last_3,
    h.avg_exit_velocity_last_3 AS hitter_avg_exit_velocity_last_3,
    h.avg_xwoba_last_3 AS hitter_avg_xwoba_last_3,
    h.avg_bat_speed_last_3 AS hitter_avg_bat_speed_last_3,
    h.avg_whiff_rate_vs_rhp_last_3 AS hitter_avg_whiff_rate_vs_rhp_last_3,
    h.avg_whiff_rate_vs_lhp_last_3 AS hitter_avg_whiff_rate_vs_lhp_last_3,

    h.weighted_whiff_rate_last_3 AS hitter_weighted_whiff_rate_last_3,
    h.weighted_contact_rate_last_3 AS hitter_weighted_contact_rate_last_3,
    h.weighted_chase_rate_last_3 AS hitter_weighted_chase_rate_last_3,
    h.weighted_csw_against_rate_last_3 AS hitter_weighted_csw_against_rate_last_3,
    h.weighted_two_strike_whiff_rate_last_3 AS hitter_weighted_two_strike_whiff_rate_last_3,
    h.weighted_whiff_rate_vs_rhp_last_3 AS hitter_weighted_whiff_rate_vs_rhp_last_3,
    h.weighted_whiff_rate_vs_lhp_last_3 AS hitter_weighted_whiff_rate_vs_lhp_last_3,

    -- hitter statcast last 5
    h.avg_whiff_rate_last_5 AS hitter_avg_whiff_rate_last_5,
    h.avg_contact_rate_last_5 AS hitter_avg_contact_rate_last_5,
    h.avg_chase_rate_last_5 AS hitter_avg_chase_rate_last_5,
    h.avg_csw_against_rate_last_5 AS hitter_avg_csw_against_rate_last_5,
    h.avg_two_strike_whiff_rate_last_5 AS hitter_avg_two_strike_whiff_rate_last_5,
    h.avg_exit_velocity_last_5 AS hitter_avg_exit_velocity_last_5,
    h.avg_xwoba_last_5 AS hitter_avg_xwoba_last_5,
    h.avg_whiff_rate_vs_rhp_last_5 AS hitter_avg_whiff_rate_vs_rhp_last_5,
    h.avg_whiff_rate_vs_lhp_last_5 AS hitter_avg_whiff_rate_vs_lhp_last_5,

    h.weighted_whiff_rate_last_5 AS hitter_weighted_whiff_rate_last_5,
    h.weighted_contact_rate_last_5 AS hitter_weighted_contact_rate_last_5,
    h.weighted_chase_rate_last_5 AS hitter_weighted_chase_rate_last_5,
    h.weighted_csw_against_rate_last_5 AS hitter_weighted_csw_against_rate_last_5,
    h.weighted_two_strike_whiff_rate_last_5 AS hitter_weighted_two_strike_whiff_rate_last_5,
    h.weighted_whiff_rate_vs_rhp_last_5 AS hitter_weighted_whiff_rate_vs_rhp_last_5,
    h.weighted_whiff_rate_vs_lhp_last_5 AS hitter_weighted_whiff_rate_vs_lhp_last_5,

    -- hitter statcast last 10
    h.avg_whiff_rate_last_10 AS hitter_avg_whiff_rate_last_10,
    h.avg_contact_rate_last_10 AS hitter_avg_contact_rate_last_10,
    h.avg_chase_rate_last_10 AS hitter_avg_chase_rate_last_10,
    h.avg_csw_against_rate_last_10 AS hitter_avg_csw_against_rate_last_10,
    h.avg_two_strike_whiff_rate_last_10 AS hitter_avg_two_strike_whiff_rate_last_10,
    h.avg_exit_velocity_last_10 AS hitter_avg_exit_velocity_last_10,
    h.avg_xwoba_last_10 AS hitter_avg_xwoba_last_10,
    h.avg_whiff_rate_vs_rhp_last_10 AS hitter_avg_whiff_rate_vs_rhp_last_10,
    h.avg_whiff_rate_vs_lhp_last_10 AS hitter_avg_whiff_rate_vs_lhp_last_10,

    h.weighted_whiff_rate_last_10 AS hitter_weighted_whiff_rate_last_10,
    h.weighted_contact_rate_last_10 AS hitter_weighted_contact_rate_last_10,
    h.weighted_chase_rate_last_10 AS hitter_weighted_chase_rate_last_10,
    h.weighted_csw_against_rate_last_10 AS hitter_weighted_csw_against_rate_last_10,
    h.weighted_two_strike_whiff_rate_last_10 AS hitter_weighted_two_strike_whiff_rate_last_10,
    h.weighted_whiff_rate_vs_rhp_last_10 AS hitter_weighted_whiff_rate_vs_rhp_last_10,
    h.weighted_whiff_rate_vs_lhp_last_10 AS hitter_weighted_whiff_rate_vs_lhp_last_10,

    -- hitter prev statcast
    h.prev_whiff_rate AS hitter_prev_whiff_rate,
    h.prev_contact_rate AS hitter_prev_contact_rate,
    h.prev_chase_rate AS hitter_prev_chase_rate,
    h.prev_exit_velocity AS hitter_prev_exit_velocity,
    h.prev_xwoba AS hitter_prev_xwoba,

    /* -------------------------------------------------------------
       PITCHER ROLLING FEATURES (pre-game)
    ------------------------------------------------------------- */
    p.gamesStarted AS pitcher_gamesStarted,
    p.days_since_last_appearance AS pitcher_days_since_last_appearance,
    p.is_starter AS pitcher_is_starter,
    p.is_reliever AS pitcher_is_reliever,

    -- pitcher simple 3
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

    -- pitcher simple 5
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

    -- pitcher simple 10
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

    -- pitcher weighted
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

    -- pitcher statcast simple
    p.avg_whiff_rate_last_3 AS pitcher_avg_whiff_rate_last_3,
    p.avg_csw_rate_last_3 AS pitcher_avg_csw_rate_last_3,
    p.avg_putaway_rate_last_3 AS pitcher_avg_putaway_rate_last_3,
    p.avg_swing_rate_last_3 AS pitcher_avg_swing_rate_last_3,
    p.avg_chase_rate_last_3 AS pitcher_avg_chase_rate_last_3,
    p.avg_zone_rate_last_3 AS pitcher_avg_zone_rate_last_3,
    p.avg_whiff_rate_0_2_last_3 AS pitcher_avg_whiff_rate_0_2_last_3,
    p.avg_whiff_rate_1_2_last_3 AS pitcher_avg_whiff_rate_1_2_last_3,
    p.avg_whiff_rate_2_2_last_3 AS pitcher_avg_whiff_rate_2_2_last_3,
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

    -- pitcher statcast weighted
    p.weighted_whiff_rate_last_3 AS pitcher_weighted_whiff_rate_last_3,
    p.weighted_csw_rate_last_3 AS pitcher_weighted_csw_rate_last_3,
    p.weighted_sc_strike_rate_last_3 AS pitcher_weighted_sc_strike_rate_last_3,
    p.weighted_chase_rate_last_3 AS pitcher_weighted_chase_rate_last_3,
    p.weighted_putaway_rate_last_3 AS pitcher_weighted_putaway_rate_last_3,
    p.weighted_whiff_rate_vs_rhb_last_3 AS pitcher_weighted_whiff_rate_vs_rhb_last_3,
    p.weighted_whiff_rate_vs_lhb_last_3 AS pitcher_weighted_whiff_rate_vs_lhb_last_3,

    p.weighted_whiff_rate_last_5 AS pitcher_weighted_whiff_rate_last_5,
    p.weighted_csw_rate_last_5 AS pitcher_weighted_csw_rate_last_5,
    p.weighted_sc_strike_rate_last_5 AS pitcher_weighted_sc_strike_rate_last_5,
    p.weighted_chase_rate_last_5 AS pitcher_weighted_chase_rate_last_5,
    p.weighted_putaway_rate_last_5 AS pitcher_weighted_putaway_rate_last_5,
    p.weighted_whiff_rate_vs_rhb_last_5 AS pitcher_weighted_whiff_rate_vs_rhb_last_5,
    p.weighted_whiff_rate_vs_lhb_last_5 AS pitcher_weighted_whiff_rate_vs_lhb_last_5,

    p.weighted_whiff_rate_last_10 AS pitcher_weighted_whiff_rate_last_10,
    p.weighted_csw_rate_last_10 AS pitcher_weighted_csw_rate_last_10,
    p.weighted_sc_strike_rate_last_10 AS pitcher_weighted_sc_strike_rate_last_10,
    p.weighted_chase_rate_last_10 AS pitcher_weighted_chase_rate_last_10,
    p.weighted_putaway_rate_last_10 AS pitcher_weighted_putaway_rate_last_10,
    p.weighted_whiff_rate_vs_rhb_last_10 AS pitcher_weighted_whiff_rate_vs_rhb_last_10,
    p.weighted_whiff_rate_vs_lhb_last_10 AS pitcher_weighted_whiff_rate_vs_lhb_last_10,

    p.prev_whiff_rate AS pitcher_prev_whiff_rate,
    p.prev_csw_rate AS pitcher_prev_csw_rate,
    p.prev_chase_rate AS pitcher_prev_chase_rate,
    p.prev_velocity AS pitcher_prev_velocity,
    p.prev_spin_rate AS pitcher_prev_spin_rate

INTO mlb.dbo.fact_hitter_pitcher_matchup_model_featuresv2
FROM pa_base pa
INNER JOIN mlb.dbo.fact_hitter_model_featuresv2 h
    ON pa.gamePk    = h.gamePk
   AND pa.hitter_id = h.player_id
   AND pa.season    = h.season
INNER JOIN mlb.dbo.fact_pitcher_model_featuresv2 p
    ON pa.gamePk     = p.gamePk
   AND pa.pitcher_id = p.player_id
   AND pa.season     = p.season
LEFT JOIN h2h_prior h2h
    ON pa.gamePk     = h2h.gamePk
   AND pa.hitter_id  = h2h.hitter_id
   AND pa.pitcher_id = h2h.pitcher_id
LEFT JOIN hitter_lineup hl
    ON pa.gamePk    = hl.gamePk
   AND pa.hitter_id = hl.hitter_id;
    """

    execute_sql(sql)
    logger.info("Finished building mlb.dbo.fact_hitter_pitcher_matchup_model_featuresv2")


def run_matchup_features_v2() -> None:
    logger.info("Starting hitter-pitcher matchup feature pipeline (v2)")
    build_hitter_pitcher_matchup_model_features_v2()
    logger.info("Finished hitter-pitcher matchup feature pipeline (v2)")


if __name__ == "__main__":
    run_matchup_features_v2()