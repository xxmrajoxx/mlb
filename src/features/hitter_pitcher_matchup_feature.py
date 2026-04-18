import logging
from sql.sql_loader import execute_sql

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

def build_hitter_pitcher_matchup_model_features() -> None:
    logger.info("Building mlb.dbo.fact_hitter_pitcher_matchup_model_features")

    sql = """
IF OBJECT_ID('mlb.dbo.fact_hitter_pitcher_matchup_model_features', 'U') IS NOT NULL
    DROP TABLE mlb.dbo.fact_hitter_pitcher_matchup_model_features;

WITH matchup_counts AS (
    SELECT
        TRY_CAST(game_pk AS int) AS gamePk,
        CAST(game_date AS date) AS game_date,
        TRY_CAST(game_year AS int) AS season,
        TRY_CAST(batter AS int) AS hitter_id,
        TRY_CAST(pitcher AS int) AS pitcher_id,

        COUNT(*) AS pitches_seen_vs_pitcher,

        SUM(CASE
                WHEN description IN (
                    'swinging_strike',
                    'swinging_strike_blocked',
                    'foul',
                    'foul_tip',
                    'hit_into_play',
                    'hit_into_play_no_out',
                    'hit_into_play_score'
                )
                THEN 1 ELSE 0
            END) AS swings_vs_pitcher,

        SUM(CASE
                WHEN description IN ('swinging_strike', 'swinging_strike_blocked')
                THEN 1 ELSE 0
            END) AS whiffs_vs_pitcher,

        SUM(CASE
                WHEN description = 'called_strike'
                THEN 1 ELSE 0
            END) AS called_strikes_vs_pitcher,

        MAX(p_throws) AS pitcher_throws,
        MAX(stand) AS hitter_stand

    FROM mlb.dbo.fact_player_hit_statcast
    WHERE batter IS NOT NULL
      AND pitcher IS NOT NULL
      AND game_pk IS NOT NULL
    GROUP BY
        game_pk,
        CAST(game_date AS date),
        game_year,
        batter,
        pitcher
),

matchup_features AS (
    SELECT
        gamePk,
        game_date,
        season,
        hitter_id,
        pitcher_id,
        pitcher_throws,
        hitter_stand,
        pitches_seen_vs_pitcher,
        swings_vs_pitcher,
        whiffs_vs_pitcher,
        called_strikes_vs_pitcher,
        whiffs_vs_pitcher * 1.0 / NULLIF(swings_vs_pitcher, 0) AS matchup_whiff_rate,
        called_strikes_vs_pitcher * 1.0 / NULLIF(pitches_seen_vs_pitcher, 0) AS matchup_called_strike_rate,
        (whiffs_vs_pitcher + called_strikes_vs_pitcher) * 1.0 / NULLIF(pitches_seen_vs_pitcher, 0) AS matchup_csw_rate
    FROM matchup_counts
),

pa_base AS (
    SELECT
        CAST(gamePk AS int) AS gamePk,
        CAST(game_date AS date) AS game_date,
        YEAR(CAST(game_date AS date)) AS season,

        CAST(batter_id AS int) AS hitter_id,
        batter_name AS hitter_name,
        CAST(pitcher_id AS int) AS pitcher_id,
        pitcher_name,

        CAST(batter_team_id AS int) AS hitter_team_id,
        batter_team_name AS hitter_team_name,
        CAST(pitcher_team_id AS int) AS pitcher_team_id,
        pitcher_team_name,

        SUM(COALESCE(plate_appearances, 0)) AS plate_appearances,
        SUM(COALESCE(hits, 0)) AS hits,
        SUM(COALESCE(singles, 0)) AS singles,
        SUM(COALESCE(doubles, 0)) AS doubles,
        SUM(COALESCE(triples, 0)) AS triples,
        SUM(COALESCE(home_runs, 0)) AS home_runs,
        SUM(COALESCE(walks, 0)) AS walks,
        SUM(COALESCE(strikeouts, 0)) AS strikeouts,
        SUM(COALESCE(hit_by_pitch, 0)) AS hit_by_pitch,
        SUM(COALESCE(sac_flies, 0)) AS sac_flies,
        SUM(COALESCE(sac_bunts, 0)) AS sac_bunts,
        SUM(COALESCE(outs_recorded, 0)) AS outs_recorded,
        SUM(COALESCE(rbi, 0)) AS rbi,
        MIN(COALESCE(first_inning_faced, 0)) AS first_inning_faced

    FROM mlb.dbo.fact_hitter_pitcher_pa_game_agg
    GROUP BY
        CAST(gamePk AS int),
        CAST(game_date AS date),
        YEAR(CAST(game_date AS date)),
        CAST(batter_id AS int),
        batter_name,
        CAST(pitcher_id AS int),
        pitcher_name,
        CAST(batter_team_id AS int),
        batter_team_name,
        CAST(pitcher_team_id AS int),
        pitcher_team_name
),

hitter_pa_game AS (
    SELECT
        CAST(gamePk AS int) AS gamePk,
        CAST(game_date AS date) AS game_date,
        YEAR(CAST(game_date AS date)) AS season,
        CAST(batter_id AS int) AS hitter_id,
        batter_name AS hitter_name,
        SUM(COALESCE(plate_appearances, 0)) AS hitter_game_plate_appearances,
        SUM(COALESCE(strikeouts, 0)) AS hitter_game_strikeouts
    FROM mlb.dbo.fact_hitter_pitcher_pa_game_agg
    GROUP BY
        CAST(gamePk AS int),
        CAST(game_date AS date),
        YEAR(CAST(game_date AS date)),
        CAST(batter_id AS int),
        batter_name
),

hitter_pa_game_ranked AS (
    SELECT
        x.*,
        ROW_NUMBER() OVER (
            PARTITION BY x.hitter_id, x.season
            ORDER BY x.game_date, x.gamePk
        ) AS rn
    FROM hitter_pa_game x
),

hitter_pa_game_features AS (
    SELECT
        cur.gamePk,
        cur.game_date,
        cur.season,
        cur.hitter_id,
        cur.hitter_name,
        cur.hitter_game_plate_appearances,
        cur.hitter_game_strikeouts,

        COALESCE(
            AVG(CASE WHEN prev.rn BETWEEN cur.rn - 3 AND cur.rn - 1
                     THEN prev.hitter_game_plate_appearances * 1.0 END),
            cur.hitter_game_plate_appearances * 1.0
        ) AS hitter_game_avg_pa_last_3,

        COALESCE(
            AVG(CASE WHEN prev.rn BETWEEN cur.rn - 5 AND cur.rn - 1
                     THEN prev.hitter_game_plate_appearances * 1.0 END),
            cur.hitter_game_plate_appearances * 1.0
        ) AS hitter_game_avg_pa_last_5,

        COALESCE(
            AVG(CASE WHEN prev.rn BETWEEN cur.rn - 10 AND cur.rn - 1
                     THEN prev.hitter_game_plate_appearances * 1.0 END),
            cur.hitter_game_plate_appearances * 1.0
        ) AS hitter_game_avg_pa_last_10,

        COALESCE(
            AVG(CASE WHEN prev.rn BETWEEN cur.rn - 3 AND cur.rn - 1
                     THEN prev.hitter_game_strikeouts * 1.0 END),
            cur.hitter_game_strikeouts * 1.0
        ) AS hitter_game_avg_strikeouts_last_3,

        COALESCE(
            AVG(CASE WHEN prev.rn BETWEEN cur.rn - 5 AND cur.rn - 1
                     THEN prev.hitter_game_strikeouts * 1.0 END),
            cur.hitter_game_strikeouts * 1.0
        ) AS hitter_game_avg_strikeouts_last_5,

        COALESCE(
            AVG(CASE WHEN prev.rn BETWEEN cur.rn - 10 AND cur.rn - 1
                     THEN prev.hitter_game_strikeouts * 1.0 END),
            cur.hitter_game_strikeouts * 1.0
        ) AS hitter_game_avg_strikeouts_last_10,

        /* volume-weighted by plate appearances */
        COALESCE(
            SUM(CASE WHEN prev.rn BETWEEN cur.rn - 3 AND cur.rn - 1
                     THEN prev.hitter_game_plate_appearances * prev.hitter_game_plate_appearances END)
            / NULLIF(SUM(CASE WHEN prev.rn BETWEEN cur.rn - 3 AND cur.rn - 1
                              THEN prev.hitter_game_plate_appearances END), 0),
            cur.hitter_game_plate_appearances * 1.0
        ) AS hitter_game_weighted_pa_last_3,

        COALESCE(
            SUM(CASE WHEN prev.rn BETWEEN cur.rn - 3 AND cur.rn - 1
                     THEN prev.hitter_game_strikeouts * prev.hitter_game_plate_appearances END)
            / NULLIF(SUM(CASE WHEN prev.rn BETWEEN cur.rn - 3 AND cur.rn - 1
                              THEN prev.hitter_game_plate_appearances END), 0),
            cur.hitter_game_strikeouts * 1.0
        ) AS hitter_game_weighted_strikeouts_last_3,

        COALESCE(
            SUM(CASE WHEN prev.rn BETWEEN cur.rn - 5 AND cur.rn - 1
                     THEN prev.hitter_game_plate_appearances * prev.hitter_game_plate_appearances END)
            / NULLIF(SUM(CASE WHEN prev.rn BETWEEN cur.rn - 5 AND cur.rn - 1
                              THEN prev.hitter_game_plate_appearances END), 0),
            cur.hitter_game_plate_appearances * 1.0
        ) AS hitter_game_weighted_pa_last_5,

        COALESCE(
            SUM(CASE WHEN prev.rn BETWEEN cur.rn - 5 AND cur.rn - 1
                     THEN prev.hitter_game_strikeouts * prev.hitter_game_plate_appearances END)
            / NULLIF(SUM(CASE WHEN prev.rn BETWEEN cur.rn - 5 AND cur.rn - 1
                              THEN prev.hitter_game_plate_appearances END), 0),
            cur.hitter_game_strikeouts * 1.0
        ) AS hitter_game_weighted_strikeouts_last_5,

        COALESCE(
            SUM(CASE WHEN prev.rn BETWEEN cur.rn - 10 AND cur.rn - 1
                     THEN prev.hitter_game_plate_appearances * prev.hitter_game_plate_appearances END)
            / NULLIF(SUM(CASE WHEN prev.rn BETWEEN cur.rn - 10 AND cur.rn - 1
                              THEN prev.hitter_game_plate_appearances END), 0),
            cur.hitter_game_plate_appearances * 1.0
        ) AS hitter_game_weighted_pa_last_10,

        COALESCE(
            SUM(CASE WHEN prev.rn BETWEEN cur.rn - 10 AND cur.rn - 1
                     THEN prev.hitter_game_strikeouts * prev.hitter_game_plate_appearances END)
            / NULLIF(SUM(CASE WHEN prev.rn BETWEEN cur.rn - 10 AND cur.rn - 1
                              THEN prev.hitter_game_plate_appearances END), 0),
            cur.hitter_game_strikeouts * 1.0
        ) AS hitter_game_weighted_strikeouts_last_10

    FROM hitter_pa_game_ranked cur
    LEFT JOIN hitter_pa_game_ranked prev
        ON cur.hitter_id = prev.hitter_id
       AND cur.season = prev.season
       AND prev.rn BETWEEN cur.rn - 10 AND cur.rn - 1
    GROUP BY
        cur.gamePk,
        cur.game_date,
        cur.season,
        cur.hitter_id,
        cur.hitter_name,
        cur.hitter_game_plate_appearances,
        cur.hitter_game_strikeouts,
        cur.rn
),

hitter_lineup AS (
    SELECT
        CAST(gamePk AS int) AS gamePk,
        CAST(player_id AS int) AS hitter_id,
        CAST(batting_order AS int) AS hitter_batting_order,
        position_abbreviation AS hitter_lineup_position,
        position_name AS hitter_lineup_position_name
    FROM mlb.dbo.fact_hitter_lineup
)

SELECT
    /* =========================
       IDENTIFIERS
       ========================= */
    pa.gamePk,
    pa.game_date,
    pa.season,

    pa.hitter_id,
    pa.hitter_name,
    h.position AS hitter_position,
    pa.hitter_team_id,
    pa.hitter_team_name,

    pa.pitcher_id,
    p.player_name AS pitcher_name,
    p.team_id AS pitcher_team_id,
    p.team_name AS pitcher_team_name,

    m.pitcher_throws,
    m.hitter_stand,

    /* =========================
       TARGET
       ========================= */
    pa.strikeouts AS hitter_strikeOuts,

    /* =========================
       DIRECT MATCHUP FEATURES
       ========================= */
    m.pitches_seen_vs_pitcher,
    m.swings_vs_pitcher,
    m.whiffs_vs_pitcher,
    m.called_strikes_vs_pitcher,
    m.matchup_whiff_rate,
    m.matchup_called_strike_rate,
    m.matchup_csw_rate,

    /* =========================
       PA-GAME OUTCOME FEATURES
       ========================= */
    pa.plate_appearances AS hitter_pitcher_plate_appearances,
    pa.hits AS hitter_pitcher_hits,
    pa.singles AS hitter_pitcher_singles,
    pa.doubles AS hitter_pitcher_doubles,
    pa.triples AS hitter_pitcher_triples,
    pa.home_runs AS hitter_pitcher_home_runs,
    pa.walks AS hitter_pitcher_walks,
    pa.hit_by_pitch AS hitter_pitcher_hit_by_pitch,
    pa.sac_flies AS hitter_pitcher_sac_flies,
    pa.sac_bunts AS hitter_pitcher_sac_bunts,
    pa.outs_recorded AS hitter_pitcher_outs_recorded,
    pa.rbi AS hitter_pitcher_rbi,
    pa.first_inning_faced AS hitter_pitcher_first_inning_faced,

    /* =========================
       HITTER FEATURES
       ========================= */
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
    h.avg_lob_last_3 AS hitter_avg_lob_last_3,
    h.avg_obp_last_3 AS hitter_avg_obp_last_3,
    h.avg_slg_last_3 AS hitter_avg_slg_last_3,
    h.avg_ops_last_3 AS hitter_avg_ops_last_3,
    h.avg_babip_last_3 AS hitter_avg_babip_last_3,
    h.avg_batting_avg_last_3 AS hitter_avg_batting_avg_last_3,
    h.avg_hbp_last_3 AS hitter_avg_hbp_last_3,
    h.avg_sf_last_3 AS hitter_avg_sf_last_3,
    h.avg_sbunts_last_3 AS hitter_avg_sbunts_last_3,
    h.avg_stolen_bases_last_3 AS hitter_avg_stolen_bases_last_3,
    h.avg_caught_stealing_last_3 AS hitter_avg_caught_stealing_last_3,
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
    h.weighted_babip_last_3 AS hitter_weighted_babip_last_3,

    -- simple last 5
    h.avg_k_last_5 AS hitter_avg_k_last_5,
    h.avg_pa_last_5 AS hitter_avg_pa_last_5,
    h.avg_ab_last_5 AS hitter_avg_ab_last_5,
    h.avg_hits_last_5 AS hitter_avg_hits_last_5,
    h.avg_hr_last_5 AS hitter_avg_hr_last_5,
    h.avg_bb_last_5 AS hitter_avg_bb_last_5,
    h.avg_pitches_last_5 AS hitter_avg_pitches_last_5,
    h.avg_tb_last_5 AS hitter_avg_tb_last_5,
    h.avg_rbi_last_5 AS hitter_avg_rbi_last_5,
    h.avg_lob_last_5 AS hitter_avg_lob_last_5,
    h.avg_obp_last_5 AS hitter_avg_obp_last_5,
    h.avg_slg_last_5 AS hitter_avg_slg_last_5,
    h.avg_ops_last_5 AS hitter_avg_ops_last_5,
    h.avg_babip_last_5 AS hitter_avg_babip_last_5,
    h.avg_batting_avg_last_5 AS hitter_avg_batting_avg_last_5,
    h.avg_hbp_last_5 AS hitter_avg_hbp_last_5,
    h.avg_sf_last_5 AS hitter_avg_sf_last_5,
    h.avg_sbunts_last_5 AS hitter_avg_sbunts_last_5,
    h.avg_stolen_bases_last_5 AS hitter_avg_stolen_bases_last_5,
    h.avg_caught_stealing_last_5 AS hitter_avg_caught_stealing_last_5,
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
    h.weighted_babip_last_5 AS hitter_weighted_babip_last_5,

    -- simple last 10
    h.avg_k_last_10 AS hitter_avg_k_last_10,
    h.avg_pa_last_10 AS hitter_avg_pa_last_10,
    h.avg_ab_last_10 AS hitter_avg_ab_last_10,
    h.avg_hits_last_10 AS hitter_avg_hits_last_10,
    h.avg_hr_last_10 AS hitter_avg_hr_last_10,
    h.avg_bb_last_10 AS hitter_avg_bb_last_10,
    h.avg_pitches_last_10 AS hitter_avg_pitches_last_10,
    h.avg_tb_last_10 AS hitter_avg_tb_last_10,
    h.avg_rbi_last_10 AS hitter_avg_rbi_last_10,
    h.avg_lob_last_10 AS hitter_avg_lob_last_10,
    h.avg_obp_last_10 AS hitter_avg_obp_last_10,
    h.avg_slg_last_10 AS hitter_avg_slg_last_10,
    h.avg_ops_last_10 AS hitter_avg_ops_last_10,
    h.avg_babip_last_10 AS hitter_avg_babip_last_10,
    h.avg_batting_avg_last_10 AS hitter_avg_batting_avg_last_10,
    h.avg_hbp_last_10 AS hitter_avg_hbp_last_10,
    h.avg_sf_last_10 AS hitter_avg_sf_last_10,
    h.avg_sbunts_last_10 AS hitter_avg_sbunts_last_10,
    h.avg_stolen_bases_last_10 AS hitter_avg_stolen_bases_last_10,
    h.avg_caught_stealing_last_10 AS hitter_avg_caught_stealing_last_10,
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
    h.weighted_babip_last_10 AS hitter_weighted_babip_last_10,

    -- hitter previous
    h.prev_k AS hitter_prev_k,
    h.prev_pa AS hitter_prev_pa,
    h.prev_ab AS hitter_prev_ab,
    h.prev_hits AS hitter_prev_hits,
    h.prev_hr AS hitter_prev_hr,
    h.prev_bb AS hitter_prev_bb,
    h.prev_pitches AS hitter_prev_pitches,
    h.prev_ops AS hitter_prev_ops,
    h.prev_k_rate AS hitter_prev_k_rate,

    -- hitter statcast simple 3
    h.avg_sc_pitches_seen_last_3 AS hitter_avg_sc_pitches_seen_last_3,
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
    h.avg_max_exit_velocity_last_3 AS hitter_avg_max_exit_velocity_last_3,
    h.avg_launch_angle_last_3 AS hitter_avg_launch_angle_last_3,
    h.avg_hit_distance_last_3 AS hitter_avg_hit_distance_last_3,
    h.avg_xba_last_3 AS hitter_avg_xba_last_3,
    h.avg_xwoba_last_3 AS hitter_avg_xwoba_last_3,
    h.avg_woba_value_last_3 AS hitter_avg_woba_value_last_3,
    h.avg_babip_value_last_3 AS hitter_avg_babip_value_last_3,
    h.avg_iso_value_last_3 AS hitter_avg_iso_value_last_3,
    h.avg_bat_speed_last_3 AS hitter_avg_bat_speed_last_3,
    h.avg_swing_length_last_3 AS hitter_avg_swing_length_last_3,
    h.avg_pitch_velocity_seen_last_3 AS hitter_avg_pitch_velocity_seen_last_3,
    h.avg_pitch_spin_seen_last_3 AS hitter_avg_pitch_spin_seen_last_3,
    h.avg_pitch_extension_seen_last_3 AS hitter_avg_pitch_extension_seen_last_3,
    h.avg_horz_movement_seen_last_3 AS hitter_avg_horz_movement_seen_last_3,
    h.avg_vert_movement_seen_last_3 AS hitter_avg_vert_movement_seen_last_3,
    h.avg_plate_x_seen_last_3 AS hitter_avg_plate_x_seen_last_3,
    h.avg_plate_z_seen_last_3 AS hitter_avg_plate_z_seen_last_3,
    h.avg_ff_seen_pct_last_3 AS hitter_avg_ff_seen_pct_last_3,
    h.avg_si_seen_pct_last_3 AS hitter_avg_si_seen_pct_last_3,
    h.avg_fc_seen_pct_last_3 AS hitter_avg_fc_seen_pct_last_3,
    h.avg_sl_seen_pct_last_3 AS hitter_avg_sl_seen_pct_last_3,
    h.avg_cu_seen_pct_last_3 AS hitter_avg_cu_seen_pct_last_3,
    h.avg_ch_seen_pct_last_3 AS hitter_avg_ch_seen_pct_last_3,
    h.avg_fs_seen_pct_last_3 AS hitter_avg_fs_seen_pct_last_3,
    h.avg_whiff_rate_vs_rhp_last_3 AS hitter_avg_whiff_rate_vs_rhp_last_3,
    h.avg_whiff_rate_vs_lhp_last_3 AS hitter_avg_whiff_rate_vs_lhp_last_3,

    -- hitter statcast weighted 3
    h.weighted_whiff_rate_last_3 AS hitter_weighted_whiff_rate_last_3,
    h.weighted_contact_rate_last_3 AS hitter_weighted_contact_rate_last_3,
    h.weighted_swing_rate_last_3 AS hitter_weighted_swing_rate_last_3,
    h.weighted_chase_rate_last_3 AS hitter_weighted_chase_rate_last_3,
    h.weighted_zone_swing_rate_last_3 AS hitter_weighted_zone_swing_rate_last_3,
    h.weighted_zone_rate_last_3 AS hitter_weighted_zone_rate_last_3,
    h.weighted_called_strike_rate_last_3 AS hitter_weighted_called_strike_rate_last_3,
    h.weighted_csw_against_rate_last_3 AS hitter_weighted_csw_against_rate_last_3,
    h.weighted_two_strike_whiff_rate_last_3 AS hitter_weighted_two_strike_whiff_rate_last_3,
    h.weighted_whiff_rate_0_2_last_3 AS hitter_weighted_whiff_rate_0_2_last_3,
    h.weighted_whiff_rate_1_2_last_3 AS hitter_weighted_whiff_rate_1_2_last_3,
    h.weighted_whiff_rate_2_2_last_3 AS hitter_weighted_whiff_rate_2_2_last_3,
    h.weighted_ff_seen_pct_last_3 AS hitter_weighted_ff_seen_pct_last_3,
    h.weighted_si_seen_pct_last_3 AS hitter_weighted_si_seen_pct_last_3,
    h.weighted_fc_seen_pct_last_3 AS hitter_weighted_fc_seen_pct_last_3,
    h.weighted_sl_seen_pct_last_3 AS hitter_weighted_sl_seen_pct_last_3,
    h.weighted_cu_seen_pct_last_3 AS hitter_weighted_cu_seen_pct_last_3,
    h.weighted_ch_seen_pct_last_3 AS hitter_weighted_ch_seen_pct_last_3,
    h.weighted_fs_seen_pct_last_3 AS hitter_weighted_fs_seen_pct_last_3,
    h.weighted_whiff_rate_vs_rhp_last_3 AS hitter_weighted_whiff_rate_vs_rhp_last_3,
    h.weighted_whiff_rate_vs_lhp_last_3 AS hitter_weighted_whiff_rate_vs_lhp_last_3,

    -- hitter statcast simple 5
    h.avg_sc_pitches_seen_last_5 AS hitter_avg_sc_pitches_seen_last_5,
    h.avg_whiff_rate_last_5 AS hitter_avg_whiff_rate_last_5,
    h.avg_contact_rate_last_5 AS hitter_avg_contact_rate_last_5,
    h.avg_swing_rate_last_5 AS hitter_avg_swing_rate_last_5,
    h.avg_chase_rate_last_5 AS hitter_avg_chase_rate_last_5,
    h.avg_zone_swing_rate_last_5 AS hitter_avg_zone_swing_rate_last_5,
    h.avg_zone_rate_last_5 AS hitter_avg_zone_rate_last_5,
    h.avg_called_strike_rate_last_5 AS hitter_avg_called_strike_rate_last_5,
    h.avg_csw_against_rate_last_5 AS hitter_avg_csw_against_rate_last_5,
    h.avg_two_strike_whiff_rate_last_5 AS hitter_avg_two_strike_whiff_rate_last_5,
    h.avg_exit_velocity_last_5 AS hitter_avg_exit_velocity_last_5,
    h.avg_xwoba_last_5 AS hitter_avg_xwoba_last_5,
    h.avg_bat_speed_last_5 AS hitter_avg_bat_speed_last_5,
    h.avg_whiff_rate_vs_rhp_last_5 AS hitter_avg_whiff_rate_vs_rhp_last_5,
    h.avg_whiff_rate_vs_lhp_last_5 AS hitter_avg_whiff_rate_vs_lhp_last_5,

    -- hitter statcast weighted 5
    h.weighted_whiff_rate_last_5 AS hitter_weighted_whiff_rate_last_5,
    h.weighted_contact_rate_last_5 AS hitter_weighted_contact_rate_last_5,
    h.weighted_swing_rate_last_5 AS hitter_weighted_swing_rate_last_5,
    h.weighted_chase_rate_last_5 AS hitter_weighted_chase_rate_last_5,
    h.weighted_zone_swing_rate_last_5 AS hitter_weighted_zone_swing_rate_last_5,
    h.weighted_zone_rate_last_5 AS hitter_weighted_zone_rate_last_5,
    h.weighted_called_strike_rate_last_5 AS hitter_weighted_called_strike_rate_last_5,
    h.weighted_csw_against_rate_last_5 AS hitter_weighted_csw_against_rate_last_5,
    h.weighted_two_strike_whiff_rate_last_5 AS hitter_weighted_two_strike_whiff_rate_last_5,
    h.weighted_whiff_rate_0_2_last_5 AS hitter_weighted_whiff_rate_0_2_last_5,
    h.weighted_whiff_rate_1_2_last_5 AS hitter_weighted_whiff_rate_1_2_last_5,
    h.weighted_whiff_rate_2_2_last_5 AS hitter_weighted_whiff_rate_2_2_last_5,
    h.weighted_ff_seen_pct_last_5 AS hitter_weighted_ff_seen_pct_last_5,
    h.weighted_si_seen_pct_last_5 AS hitter_weighted_si_seen_pct_last_5,
    h.weighted_fc_seen_pct_last_5 AS hitter_weighted_fc_seen_pct_last_5,
    h.weighted_sl_seen_pct_last_5 AS hitter_weighted_sl_seen_pct_last_5,
    h.weighted_cu_seen_pct_last_5 AS hitter_weighted_cu_seen_pct_last_5,
    h.weighted_ch_seen_pct_last_5 AS hitter_weighted_ch_seen_pct_last_5,
    h.weighted_fs_seen_pct_last_5 AS hitter_weighted_fs_seen_pct_last_5,
    h.weighted_whiff_rate_vs_rhp_last_5 AS hitter_weighted_whiff_rate_vs_rhp_last_5,
    h.weighted_whiff_rate_vs_lhp_last_5 AS hitter_weighted_whiff_rate_vs_lhp_last_5,

    -- hitter statcast simple 10
    h.avg_sc_pitches_seen_last_10 AS hitter_avg_sc_pitches_seen_last_10,
    h.avg_whiff_rate_last_10 AS hitter_avg_whiff_rate_last_10,
    h.avg_contact_rate_last_10 AS hitter_avg_contact_rate_last_10,
    h.avg_swing_rate_last_10 AS hitter_avg_swing_rate_last_10,
    h.avg_chase_rate_last_10 AS hitter_avg_chase_rate_last_10,
    h.avg_zone_swing_rate_last_10 AS hitter_avg_zone_swing_rate_last_10,
    h.avg_zone_rate_last_10 AS hitter_avg_zone_rate_last_10,
    h.avg_called_strike_rate_last_10 AS hitter_avg_called_strike_rate_last_10,
    h.avg_csw_against_rate_last_10 AS hitter_avg_csw_against_rate_last_10,
    h.avg_two_strike_whiff_rate_last_10 AS hitter_avg_two_strike_whiff_rate_last_10,
    h.avg_exit_velocity_last_10 AS hitter_avg_exit_velocity_last_10,
    h.avg_xwoba_last_10 AS hitter_avg_xwoba_last_10,
    h.avg_bat_speed_last_10 AS hitter_avg_bat_speed_last_10,
    h.avg_whiff_rate_vs_rhp_last_10 AS hitter_avg_whiff_rate_vs_rhp_last_10,
    h.avg_whiff_rate_vs_lhp_last_10 AS hitter_avg_whiff_rate_vs_lhp_last_10,

    -- hitter statcast weighted 10
    h.weighted_whiff_rate_last_10 AS hitter_weighted_whiff_rate_last_10,
    h.weighted_contact_rate_last_10 AS hitter_weighted_contact_rate_last_10,
    h.weighted_swing_rate_last_10 AS hitter_weighted_swing_rate_last_10,
    h.weighted_chase_rate_last_10 AS hitter_weighted_chase_rate_last_10,
    h.weighted_zone_swing_rate_last_10 AS hitter_weighted_zone_swing_rate_last_10,
    h.weighted_zone_rate_last_10 AS hitter_weighted_zone_rate_last_10,
    h.weighted_called_strike_rate_last_10 AS hitter_weighted_called_strike_rate_last_10,
    h.weighted_csw_against_rate_last_10 AS hitter_weighted_csw_against_rate_last_10,
    h.weighted_two_strike_whiff_rate_last_10 AS hitter_weighted_two_strike_whiff_rate_last_10,
    h.weighted_whiff_rate_0_2_last_10 AS hitter_weighted_whiff_rate_0_2_last_10,
    h.weighted_whiff_rate_1_2_last_10 AS hitter_weighted_whiff_rate_1_2_last_10,
    h.weighted_whiff_rate_2_2_last_10 AS hitter_weighted_whiff_rate_2_2_last_10,
    h.weighted_ff_seen_pct_last_10 AS hitter_weighted_ff_seen_pct_last_10,
    h.weighted_si_seen_pct_last_10 AS hitter_weighted_si_seen_pct_last_10,
    h.weighted_fc_seen_pct_last_10 AS hitter_weighted_fc_seen_pct_last_10,
    h.weighted_sl_seen_pct_last_10 AS hitter_weighted_sl_seen_pct_last_10,
    h.weighted_cu_seen_pct_last_10 AS hitter_weighted_cu_seen_pct_last_10,
    h.weighted_ch_seen_pct_last_10 AS hitter_weighted_ch_seen_pct_last_10,
    h.weighted_fs_seen_pct_last_10 AS hitter_weighted_fs_seen_pct_last_10,
    h.weighted_whiff_rate_vs_rhp_last_10 AS hitter_weighted_whiff_rate_vs_rhp_last_10,
    h.weighted_whiff_rate_vs_lhp_last_10 AS hitter_weighted_whiff_rate_vs_lhp_last_10,

    h.prev_whiff_rate AS hitter_prev_whiff_rate,
    h.prev_contact_rate AS hitter_prev_contact_rate,
    h.prev_chase_rate AS hitter_prev_chase_rate,
    h.prev_exit_velocity AS hitter_prev_exit_velocity,
    h.prev_xwoba AS hitter_prev_xwoba,
    h.prev_bat_speed AS hitter_prev_bat_speed,

    /* =========================
       LINEUP FEATURES
       ========================= */
    hl.hitter_batting_order,
    hl.hitter_lineup_position,
    hl.hitter_lineup_position_name,

    /* =========================
       HITTER GAME FEATURES
       ========================= */
    hpg.hitter_game_plate_appearances,
    hpg.hitter_game_strikeouts,
    hpg.hitter_game_avg_pa_last_3,
    hpg.hitter_game_avg_pa_last_5,
    hpg.hitter_game_avg_pa_last_10,
    hpg.hitter_game_avg_strikeouts_last_3,
    hpg.hitter_game_avg_strikeouts_last_5,
    hpg.hitter_game_avg_strikeouts_last_10,
    hpg.hitter_game_weighted_pa_last_3,
    hpg.hitter_game_weighted_pa_last_5,
    hpg.hitter_game_weighted_pa_last_10,
    hpg.hitter_game_weighted_strikeouts_last_3,
    hpg.hitter_game_weighted_strikeouts_last_5,
    hpg.hitter_game_weighted_strikeouts_last_10,

    /* =========================
       PITCHER FEATURES
       ========================= */
    p.gamesStarted AS pitcher_gamesStarted,
    p.days_since_last_appearance AS pitcher_days_since_last_appearance,

    -- pitcher simple 3
    p.avg_games_started_last_3 AS pitcher_avg_games_started_last_3,
    p.avg_ip_last_3 AS pitcher_avg_ip_last_3,
    p.avg_bf_last_3 AS pitcher_avg_bf_last_3,
    p.avg_pitches_last_3 AS pitcher_avg_pitches_last_3,
    p.avg_outs_last_3 AS pitcher_avg_outs_last_3,
    p.avg_k_last_3 AS pitcher_avg_k_last_3,
    p.avg_k9_last_3 AS pitcher_avg_k9_last_3,
    p.avg_era_last_3 AS pitcher_avg_era_last_3,
    p.avg_whip_last_3 AS pitcher_avg_whip_last_3,
    p.avg_bb_last_3 AS pitcher_avg_bb_last_3,
    p.avg_hr_last_3 AS pitcher_avg_hr_last_3,
    p.avg_hits_last_3 AS pitcher_avg_hits_last_3,
    p.avg_strike_pct_last_3 AS pitcher_avg_strike_pct_last_3,
    p.avg_kbb_last_3 AS pitcher_avg_kbb_last_3,
    p.sum_pitches_last_3 AS pitcher_sum_pitches_last_3,
    p.sum_bf_last_3 AS pitcher_sum_bf_last_3,
    p.pct_5plus_ip_last_3 AS pitcher_pct_5plus_ip_last_3,
    p.pct_6plus_ip_last_3 AS pitcher_pct_6plus_ip_last_3,

    -- pitcher simple 5
    p.avg_games_started_last_5 AS pitcher_avg_games_started_last_5,
    p.avg_ip_last_5 AS pitcher_avg_ip_last_5,
    p.avg_bf_last_5 AS pitcher_avg_bf_last_5,
    p.avg_pitches_last_5 AS pitcher_avg_pitches_last_5,
    p.avg_outs_last_5 AS pitcher_avg_outs_last_5,
    p.avg_k_last_5 AS pitcher_avg_k_last_5,
    p.avg_k9_last_5 AS pitcher_avg_k9_last_5,
    p.avg_era_last_5 AS pitcher_avg_era_last_5,
    p.avg_whip_last_5 AS pitcher_avg_whip_last_5,
    p.avg_bb_last_5 AS pitcher_avg_bb_last_5,
    p.avg_hr_last_5 AS pitcher_avg_hr_last_5,
    p.avg_hits_last_5 AS pitcher_avg_hits_last_5,
    p.avg_strike_pct_last_5 AS pitcher_avg_strike_pct_last_5,
    p.avg_kbb_last_5 AS pitcher_avg_kbb_last_5,
    p.sum_pitches_last_5 AS pitcher_sum_pitches_last_5,
    p.sum_bf_last_5 AS pitcher_sum_bf_last_5,
    p.pct_5plus_ip_last_5 AS pitcher_pct_5plus_ip_last_5,
    p.pct_6plus_ip_last_5 AS pitcher_pct_6plus_ip_last_5,

    -- pitcher simple 10
    p.avg_games_started_last_10 AS pitcher_avg_games_started_last_10,
    p.avg_ip_last_10 AS pitcher_avg_ip_last_10,
    p.avg_bf_last_10 AS pitcher_avg_bf_last_10,
    p.avg_pitches_last_10 AS pitcher_avg_pitches_last_10,
    p.avg_outs_last_10 AS pitcher_avg_outs_last_10,
    p.avg_k_last_10 AS pitcher_avg_k_last_10,
    p.avg_k9_last_10 AS pitcher_avg_k9_last_10,
    p.avg_era_last_10 AS pitcher_avg_era_last_10,
    p.avg_whip_last_10 AS pitcher_avg_whip_last_10,
    p.avg_bb_last_10 AS pitcher_avg_bb_last_10,
    p.avg_hr_last_10 AS pitcher_avg_hr_last_10,
    p.avg_hits_last_10 AS pitcher_avg_hits_last_10,
    p.avg_strike_pct_last_10 AS pitcher_avg_strike_pct_last_10,
    p.avg_kbb_last_10 AS pitcher_avg_kbb_last_10,
    p.sum_pitches_last_10 AS pitcher_sum_pitches_last_10,
    p.sum_bf_last_10 AS pitcher_sum_bf_last_10,
    p.pct_5plus_ip_last_10 AS pitcher_pct_5plus_ip_last_10,
    p.pct_6plus_ip_last_10 AS pitcher_pct_6plus_ip_last_10,

    -- pitcher weighted
    p.weighted_k_per_bf_last_3 AS pitcher_weighted_k_per_bf_last_3,
    p.weighted_bb_per_bf_last_3 AS pitcher_weighted_bb_per_bf_last_3,
    p.weighted_baa_last_3 AS pitcher_weighted_baa_last_3,
    p.weighted_hr_per_bf_last_3 AS pitcher_weighted_hr_per_bf_last_3,
    p.weighted_strike_pct_last_3 AS pitcher_weighted_strike_pct_last_3,
    p.weighted_pitches_per_inning_last_3 AS pitcher_weighted_pitches_per_inning_last_3,
    p.weighted_k9_last_3 AS pitcher_weighted_k9_last_3,
    p.weighted_bb9_last_3 AS pitcher_weighted_bb9_last_3,
    p.weighted_whip_last_3 AS pitcher_weighted_whip_last_3,
    p.weighted_kbb_last_3 AS pitcher_weighted_kbb_last_3,
    p.weighted_inherited_runner_score_pct_last_3 AS pitcher_weighted_inherited_runner_score_pct_last_3,

    p.weighted_k_per_bf_last_5 AS pitcher_weighted_k_per_bf_last_5,
    p.weighted_bb_per_bf_last_5 AS pitcher_weighted_bb_per_bf_last_5,
    p.weighted_baa_last_5 AS pitcher_weighted_baa_last_5,
    p.weighted_hr_per_bf_last_5 AS pitcher_weighted_hr_per_bf_last_5,
    p.weighted_strike_pct_last_5 AS pitcher_weighted_strike_pct_last_5,
    p.weighted_pitches_per_inning_last_5 AS pitcher_weighted_pitches_per_inning_last_5,
    p.weighted_k9_last_5 AS pitcher_weighted_k9_last_5,
    p.weighted_bb9_last_5 AS pitcher_weighted_bb9_last_5,
    p.weighted_whip_last_5 AS pitcher_weighted_whip_last_5,
    p.weighted_kbb_last_5 AS pitcher_weighted_kbb_last_5,
    p.weighted_inherited_runner_score_pct_last_5 AS pitcher_weighted_inherited_runner_score_pct_last_5,

    p.weighted_k_per_bf_last_10 AS pitcher_weighted_k_per_bf_last_10,
    p.weighted_bb_per_bf_last_10 AS pitcher_weighted_bb_per_bf_last_10,
    p.weighted_baa_last_10 AS pitcher_weighted_baa_last_10,
    p.weighted_hr_per_bf_last_10 AS pitcher_weighted_hr_per_bf_last_10,
    p.weighted_strike_pct_last_10 AS pitcher_weighted_strike_pct_last_10,
    p.weighted_pitches_per_inning_last_10 AS pitcher_weighted_pitches_per_inning_last_10,
    p.weighted_k9_last_10 AS pitcher_weighted_k9_last_10,
    p.weighted_bb9_last_10 AS pitcher_weighted_bb9_last_10,
    p.weighted_whip_last_10 AS pitcher_weighted_whip_last_10,
    p.weighted_kbb_last_10 AS pitcher_weighted_kbb_last_10,
    p.weighted_inherited_runner_score_pct_last_10 AS pitcher_weighted_inherited_runner_score_pct_last_10,

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
    p.avg_plate_x_last_3 AS pitcher_avg_plate_x_last_3,
    p.avg_plate_z_last_3 AS pitcher_avg_plate_z_last_3,
    p.avg_ff_pct_last_3 AS pitcher_avg_ff_pct_last_3,
    p.avg_si_pct_last_3 AS pitcher_avg_si_pct_last_3,
    p.avg_fc_pct_last_3 AS pitcher_avg_fc_pct_last_3,
    p.avg_sl_pct_last_3 AS pitcher_avg_sl_pct_last_3,
    p.avg_cu_pct_last_3 AS pitcher_avg_cu_pct_last_3,
    p.avg_ch_pct_last_3 AS pitcher_avg_ch_pct_last_3,
    p.avg_fs_pct_last_3 AS pitcher_avg_fs_pct_last_3,
    p.avg_sl_whiff_rate_last_3 AS pitcher_avg_sl_whiff_rate_last_3,
    p.avg_ff_whiff_rate_last_3 AS pitcher_avg_ff_whiff_rate_last_3,

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

    -- pitcher statcast weighted
    p.weighted_whiff_rate_last_3 AS pitcher_weighted_whiff_rate_last_3,
    p.weighted_csw_rate_last_3 AS pitcher_weighted_csw_rate_last_3,
    p.weighted_sc_strike_rate_last_3 AS pitcher_weighted_sc_strike_rate_last_3,
    p.weighted_chase_rate_last_3 AS pitcher_weighted_chase_rate_last_3,
    p.weighted_putaway_rate_last_3 AS pitcher_weighted_putaway_rate_last_3,

    p.weighted_whiff_rate_last_5 AS pitcher_weighted_whiff_rate_last_5,
    p.weighted_csw_rate_last_5 AS pitcher_weighted_csw_rate_last_5,
    p.weighted_sc_strike_rate_last_5 AS pitcher_weighted_sc_strike_rate_last_5,
    p.weighted_chase_rate_last_5 AS pitcher_weighted_chase_rate_last_5,
    p.weighted_putaway_rate_last_5 AS pitcher_weighted_putaway_rate_last_5,

    p.weighted_whiff_rate_last_10 AS pitcher_weighted_whiff_rate_last_10,
    p.weighted_csw_rate_last_10 AS pitcher_weighted_csw_rate_last_10,
    p.weighted_sc_strike_rate_last_10 AS pitcher_weighted_sc_strike_rate_last_10,
    p.weighted_chase_rate_last_10 AS pitcher_weighted_chase_rate_last_10,
    p.weighted_putaway_rate_last_10 AS pitcher_weighted_putaway_rate_last_10,

    p.prev_whiff_rate AS pitcher_prev_whiff_rate,
    p.prev_csw_rate AS pitcher_prev_csw_rate,
    p.prev_chase_rate AS pitcher_prev_chase_rate,

    pg.strikeOuts AS pitcher_strikeOuts,

    /* =========================
       LINEUP AGG FEATURES
       ========================= */
    la.lineup_spots,
    la.matched_hitter_feature_rows,

    la.lineup_avg_k_last_3,
    la.lineup_avg_k_last_5,
    la.lineup_avg_k_last_10,

    la.lineup_avg_ops_last_3,
    la.lineup_avg_ops_last_5,
    la.lineup_avg_ops_last_10,

    la.lineup_avg_whiff_rate_last_3,
    la.lineup_avg_whiff_rate_last_5,
    la.lineup_avg_whiff_rate_last_10,

    la.lineup_avg_csw_against_rate_last_3,
    la.lineup_avg_csw_against_rate_last_5,
    la.lineup_avg_csw_against_rate_last_10,

    la.lineup_wavg_k_last_3,
    la.lineup_wavg_k_last_5,
    la.lineup_wavg_k_last_10,

    la.lineup_wavg_ops_last_3,
    la.lineup_wavg_ops_last_5,
    la.lineup_wavg_ops_last_10,

    la.lineup_wavg_whiff_rate_last_3,
    la.lineup_wavg_whiff_rate_last_5,
    la.lineup_wavg_whiff_rate_last_10,

    la.lineup_wavg_csw_against_rate_last_3,
    la.lineup_wavg_csw_against_rate_last_5,
    la.lineup_wavg_csw_against_rate_last_10,

    la.lineup_weighted_k_rate_last_3,
    la.lineup_weighted_k_rate_last_5,
    la.lineup_weighted_k_rate_last_10,

    la.lineup_weighted_walk_rate_last_3,
    la.lineup_weighted_walk_rate_last_5,
    la.lineup_weighted_walk_rate_last_10,

    la.lineup_weighted_hit_rate_last_3,
    la.lineup_weighted_hit_rate_last_5,
    la.lineup_weighted_hit_rate_last_10,

    la.lineup_weighted_tb_rate_last_3,
    la.lineup_weighted_tb_rate_last_5,
    la.lineup_weighted_tb_rate_last_10,

    la.lineup_weighted_hr_rate_last_3,
    la.lineup_weighted_hr_rate_last_5,
    la.lineup_weighted_hr_rate_last_10,

    la.lineup_weighted_obp_last_3,
    la.lineup_weighted_obp_last_5,
    la.lineup_weighted_obp_last_10,

    la.lineup_weighted_slg_last_3,
    la.lineup_weighted_slg_last_5,
    la.lineup_weighted_slg_last_10,

    la.lineup_weighted_ops_last_3,
    la.lineup_weighted_ops_last_5,
    la.lineup_weighted_ops_last_10,

    la.lineup_weighted_whiff_rate_last_3,
    la.lineup_weighted_whiff_rate_last_5,
    la.lineup_weighted_whiff_rate_last_10,

    la.lineup_weighted_csw_against_rate_last_3,
    la.lineup_weighted_csw_against_rate_last_5,
    la.lineup_weighted_csw_against_rate_last_10,

    la.lineup_weighted_whiff_rate_vs_rhp_last_3,
    la.lineup_weighted_whiff_rate_vs_rhp_last_5,
    la.lineup_weighted_whiff_rate_vs_rhp_last_10,

    la.lineup_weighted_whiff_rate_vs_lhp_last_3,
    la.lineup_weighted_whiff_rate_vs_lhp_last_5,
    la.lineup_weighted_whiff_rate_vs_lhp_last_10,

    la.lineup_num_high_k_hitters,
    la.lineup_num_power_hitters,
    la.lineup_num_high_whiff_hitters,
    la.lineup_num_strong_bats

INTO mlb.dbo.fact_hitter_pitcher_matchup_model_features
FROM pa_base pa
INNER JOIN mlb.dbo.fact_hitter_model_features h
    ON pa.gamePk = h.gamePk
   AND pa.hitter_id = h.player_id
   AND pa.season = h.season
INNER JOIN matchup_features m
    ON pa.gamePk = m.gamePk
   AND pa.hitter_id = m.hitter_id
   AND pa.pitcher_id = m.pitcher_id
   AND pa.season = m.season
INNER JOIN mlb.dbo.fact_pitcher_model_features p
    ON pa.gamePk = p.gamePk
   AND pa.pitcher_id = p.player_id
   AND pa.season = p.season
LEFT JOIN mlb.dbo.fact_player_pitching_gamelogs pg
    ON pa.gamePk = pg.gamePk
   AND pa.pitcher_id = pg.player_id
LEFT JOIN hitter_pa_game_features hpg
    ON pa.gamePk = hpg.gamePk
   AND pa.hitter_id = hpg.hitter_id
   AND pa.season = hpg.season
LEFT JOIN hitter_lineup hl
    ON pa.gamePk = hl.gamePk
   AND pa.hitter_id = hl.hitter_id
LEFT JOIN mlb.dbo.fact_lineup_agg_features la
    ON pa.gamePk = la.gamePk
   AND pa.season = la.season
   AND pa.hitter_team_id = la.team_id;
    """

    execute_sql(sql)
    logger.info("Finished building mlb.dbo.fact_hitter_pitcher_matchup_model_features")

if __name__ == "__main__":
    build_hitter_pitcher_matchup_model_features()