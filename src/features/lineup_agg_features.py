import logging
from sql.sql_loader import execute_sql

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


def build_fact_lineup_agg_features() -> None:
    logger.info("Building mlb.dbo.fact_lineup_agg_features")

    sql = """
IF OBJECT_ID('mlb.dbo.fact_lineup_agg_features', 'U') IS NOT NULL
    DROP TABLE mlb.dbo.fact_lineup_agg_features;

WITH lineup_base AS (
    SELECT
        TRY_CAST(l.gamePk AS bigint) AS gamePk,
        CAST(l.game_date AS date) AS game_date,
        YEAR(CAST(l.game_date AS date)) AS season,
        l.side,
        TRY_CAST(l.team_id AS int) AS team_id,
        l.team_name,
        TRY_CAST(l.batting_order AS int) AS batting_order,
        TRY_CAST(l.player_id AS int) AS player_id,
        l.player_name,
        l.position_abbreviation,
        l.position_name,

        CASE
            WHEN TRY_CAST(l.batting_order AS int) = 1 THEN 1.30
            WHEN TRY_CAST(l.batting_order AS int) = 2 THEN 1.20
            WHEN TRY_CAST(l.batting_order AS int) = 3 THEN 1.15
            WHEN TRY_CAST(l.batting_order AS int) = 4 THEN 1.10
            WHEN TRY_CAST(l.batting_order AS int) = 5 THEN 1.00
            WHEN TRY_CAST(l.batting_order AS int) = 6 THEN 0.95
            WHEN TRY_CAST(l.batting_order AS int) = 7 THEN 0.90
            WHEN TRY_CAST(l.batting_order AS int) = 8 THEN 0.85
            WHEN TRY_CAST(l.batting_order AS int) = 9 THEN 0.80
            ELSE 0.00
        END AS lineup_weight

    FROM mlb.dbo.fact_hitter_lineup l
    WHERE TRY_CAST(l.batting_order AS int) BETWEEN 1 AND 9
),

joined AS (
    SELECT
        lb.gamePk,
        lb.game_date,
        lb.season,
        lb.side,
        lb.team_id,
        lb.team_name,
        lb.batting_order,
        lb.player_id,
        lb.player_name,
        lb.position_abbreviation,
        lb.position_name,
        lb.lineup_weight,

        /* -------------------- CORE HITTER ROLLING -------------------- */
        TRY_CAST(h.avg_k_last_3 AS float) AS avg_k_last_3,
        TRY_CAST(h.avg_k_last_5 AS float) AS avg_k_last_5,
        TRY_CAST(h.avg_k_last_10 AS float) AS avg_k_last_10,

        TRY_CAST(h.avg_pa_last_3 AS float) AS avg_pa_last_3,
        TRY_CAST(h.avg_pa_last_5 AS float) AS avg_pa_last_5,
        TRY_CAST(h.avg_pa_last_10 AS float) AS avg_pa_last_10,

        TRY_CAST(h.avg_ab_last_3 AS float) AS avg_ab_last_3,
        TRY_CAST(h.avg_ab_last_5 AS float) AS avg_ab_last_5,
        TRY_CAST(h.avg_ab_last_10 AS float) AS avg_ab_last_10,

        TRY_CAST(h.avg_hits_last_3 AS float) AS avg_hits_last_3,
        TRY_CAST(h.avg_hits_last_5 AS float) AS avg_hits_last_5,
        TRY_CAST(h.avg_hits_last_10 AS float) AS avg_hits_last_10,

        TRY_CAST(h.avg_hr_last_3 AS float) AS avg_hr_last_3,
        TRY_CAST(h.avg_hr_last_5 AS float) AS avg_hr_last_5,
        TRY_CAST(h.avg_hr_last_10 AS float) AS avg_hr_last_10,

        TRY_CAST(h.avg_bb_last_3 AS float) AS avg_bb_last_3,
        TRY_CAST(h.avg_bb_last_5 AS float) AS avg_bb_last_5,
        TRY_CAST(h.avg_bb_last_10 AS float) AS avg_bb_last_10,

        TRY_CAST(h.avg_pitches_last_3 AS float) AS avg_pitches_last_3,
        TRY_CAST(h.avg_pitches_last_5 AS float) AS avg_pitches_last_5,
        TRY_CAST(h.avg_pitches_last_10 AS float) AS avg_pitches_last_10,

        TRY_CAST(h.avg_tb_last_3 AS float) AS avg_tb_last_3,
        TRY_CAST(h.avg_tb_last_5 AS float) AS avg_tb_last_5,
        TRY_CAST(h.avg_tb_last_10 AS float) AS avg_tb_last_10,

        TRY_CAST(h.avg_rbi_last_3 AS float) AS avg_rbi_last_3,
        TRY_CAST(h.avg_rbi_last_5 AS float) AS avg_rbi_last_5,
        TRY_CAST(h.avg_rbi_last_10 AS float) AS avg_rbi_last_10,

        TRY_CAST(h.avg_obp_last_3 AS float) AS avg_obp_last_3,
        TRY_CAST(h.avg_obp_last_5 AS float) AS avg_obp_last_5,
        TRY_CAST(h.avg_obp_last_10 AS float) AS avg_obp_last_10,

        TRY_CAST(h.avg_slg_last_3 AS float) AS avg_slg_last_3,
        TRY_CAST(h.avg_slg_last_5 AS float) AS avg_slg_last_5,
        TRY_CAST(h.avg_slg_last_10 AS float) AS avg_slg_last_10,

        TRY_CAST(h.avg_ops_last_3 AS float) AS avg_ops_last_3,
        TRY_CAST(h.avg_ops_last_5 AS float) AS avg_ops_last_5,
        TRY_CAST(h.avg_ops_last_10 AS float) AS avg_ops_last_10,

        TRY_CAST(h.avg_batting_avg_last_3 AS float) AS avg_batting_avg_last_3,
        TRY_CAST(h.avg_batting_avg_last_5 AS float) AS avg_batting_avg_last_5,
        TRY_CAST(h.avg_batting_avg_last_10 AS float) AS avg_batting_avg_last_10,

        TRY_CAST(h.avg_k_rate_last_3 AS float) AS avg_k_rate_last_3,
        TRY_CAST(h.avg_k_rate_last_5 AS float) AS avg_k_rate_last_5,
        TRY_CAST(h.avg_k_rate_last_10 AS float) AS avg_k_rate_last_10,

        TRY_CAST(h.avg_walk_rate_last_3 AS float) AS avg_walk_rate_last_3,
        TRY_CAST(h.avg_walk_rate_last_5 AS float) AS avg_walk_rate_last_5,
        TRY_CAST(h.avg_walk_rate_last_10 AS float) AS avg_walk_rate_last_10,

        TRY_CAST(h.avg_hit_rate_last_3 AS float) AS avg_hit_rate_last_3,
        TRY_CAST(h.avg_hit_rate_last_5 AS float) AS avg_hit_rate_last_5,
        TRY_CAST(h.avg_hit_rate_last_10 AS float) AS avg_hit_rate_last_10,

        TRY_CAST(h.avg_tb_rate_last_3 AS float) AS avg_tb_rate_last_3,
        TRY_CAST(h.avg_tb_rate_last_5 AS float) AS avg_tb_rate_last_5,
        TRY_CAST(h.avg_tb_rate_last_10 AS float) AS avg_tb_rate_last_10,

        TRY_CAST(h.avg_hr_rate_last_3 AS float) AS avg_hr_rate_last_3,
        TRY_CAST(h.avg_hr_rate_last_5 AS float) AS avg_hr_rate_last_5,
        TRY_CAST(h.avg_hr_rate_last_10 AS float) AS avg_hr_rate_last_10,

        TRY_CAST(h.pct_1plus_k_last_3 AS float) AS pct_1plus_k_last_3,
        TRY_CAST(h.pct_1plus_k_last_5 AS float) AS pct_1plus_k_last_5,
        TRY_CAST(h.pct_1plus_k_last_10 AS float) AS pct_1plus_k_last_10,

        TRY_CAST(h.pct_2plus_k_last_3 AS float) AS pct_2plus_k_last_3,
        TRY_CAST(h.pct_2plus_k_last_5 AS float) AS pct_2plus_k_last_5,
        TRY_CAST(h.pct_2plus_k_last_10 AS float) AS pct_2plus_k_last_10,

        /* -------------------- NEW VOLUME-WEIGHTED HITTER ROLLING -------------------- */
        TRY_CAST(h.weighted_k_rate_last_3 AS float) AS weighted_k_rate_last_3,
        TRY_CAST(h.weighted_k_rate_last_5 AS float) AS weighted_k_rate_last_5,
        TRY_CAST(h.weighted_k_rate_last_10 AS float) AS weighted_k_rate_last_10,

        TRY_CAST(h.weighted_walk_rate_last_3 AS float) AS weighted_walk_rate_last_3,
        TRY_CAST(h.weighted_walk_rate_last_5 AS float) AS weighted_walk_rate_last_5,
        TRY_CAST(h.weighted_walk_rate_last_10 AS float) AS weighted_walk_rate_last_10,

        TRY_CAST(h.weighted_hit_rate_last_3 AS float) AS weighted_hit_rate_last_3,
        TRY_CAST(h.weighted_hit_rate_last_5 AS float) AS weighted_hit_rate_last_5,
        TRY_CAST(h.weighted_hit_rate_last_10 AS float) AS weighted_hit_rate_last_10,

        TRY_CAST(h.weighted_tb_rate_last_3 AS float) AS weighted_tb_rate_last_3,
        TRY_CAST(h.weighted_tb_rate_last_5 AS float) AS weighted_tb_rate_last_5,
        TRY_CAST(h.weighted_tb_rate_last_10 AS float) AS weighted_tb_rate_last_10,

        TRY_CAST(h.weighted_hr_rate_last_3 AS float) AS weighted_hr_rate_last_3,
        TRY_CAST(h.weighted_hr_rate_last_5 AS float) AS weighted_hr_rate_last_5,
        TRY_CAST(h.weighted_hr_rate_last_10 AS float) AS weighted_hr_rate_last_10,

        TRY_CAST(h.weighted_batting_avg_last_3 AS float) AS weighted_batting_avg_last_3,
        TRY_CAST(h.weighted_batting_avg_last_5 AS float) AS weighted_batting_avg_last_5,
        TRY_CAST(h.weighted_batting_avg_last_10 AS float) AS weighted_batting_avg_last_10,

        TRY_CAST(h.weighted_pitches_per_pa_last_3 AS float) AS weighted_pitches_per_pa_last_3,
        TRY_CAST(h.weighted_pitches_per_pa_last_5 AS float) AS weighted_pitches_per_pa_last_5,
        TRY_CAST(h.weighted_pitches_per_pa_last_10 AS float) AS weighted_pitches_per_pa_last_10,

        TRY_CAST(h.weighted_obp_last_3 AS float) AS weighted_obp_last_3,
        TRY_CAST(h.weighted_obp_last_5 AS float) AS weighted_obp_last_5,
        TRY_CAST(h.weighted_obp_last_10 AS float) AS weighted_obp_last_10,

        TRY_CAST(h.weighted_slg_last_3 AS float) AS weighted_slg_last_3,
        TRY_CAST(h.weighted_slg_last_5 AS float) AS weighted_slg_last_5,
        TRY_CAST(h.weighted_slg_last_10 AS float) AS weighted_slg_last_10,

        TRY_CAST(h.weighted_ops_last_3 AS float) AS weighted_ops_last_3,
        TRY_CAST(h.weighted_ops_last_5 AS float) AS weighted_ops_last_5,
        TRY_CAST(h.weighted_ops_last_10 AS float) AS weighted_ops_last_10,

        TRY_CAST(h.weighted_babip_last_3 AS float) AS weighted_babip_last_3,
        TRY_CAST(h.weighted_babip_last_5 AS float) AS weighted_babip_last_5,
        TRY_CAST(h.weighted_babip_last_10 AS float) AS weighted_babip_last_10,

        /* -------------------- STATCAST / SWING-MISS -------------------- */
        TRY_CAST(h.avg_sc_pitches_seen_last_3 AS float) AS avg_sc_pitches_seen_last_3,
        TRY_CAST(h.avg_sc_pitches_seen_last_5 AS float) AS avg_sc_pitches_seen_last_5,
        TRY_CAST(h.avg_sc_pitches_seen_last_10 AS float) AS avg_sc_pitches_seen_last_10,

        TRY_CAST(h.avg_whiff_rate_last_3 AS float) AS avg_whiff_rate_last_3,
        TRY_CAST(h.avg_whiff_rate_last_5 AS float) AS avg_whiff_rate_last_5,
        TRY_CAST(h.avg_whiff_rate_last_10 AS float) AS avg_whiff_rate_last_10,

        TRY_CAST(h.avg_contact_rate_last_3 AS float) AS avg_contact_rate_last_3,
        TRY_CAST(h.avg_contact_rate_last_5 AS float) AS avg_contact_rate_last_5,
        TRY_CAST(h.avg_contact_rate_last_10 AS float) AS avg_contact_rate_last_10,

        TRY_CAST(h.avg_swing_rate_last_3 AS float) AS avg_swing_rate_last_3,
        TRY_CAST(h.avg_swing_rate_last_5 AS float) AS avg_swing_rate_last_5,
        TRY_CAST(h.avg_swing_rate_last_10 AS float) AS avg_swing_rate_last_10,

        TRY_CAST(h.avg_chase_rate_last_3 AS float) AS avg_chase_rate_last_3,
        TRY_CAST(h.avg_chase_rate_last_5 AS float) AS avg_chase_rate_last_5,
        TRY_CAST(h.avg_chase_rate_last_10 AS float) AS avg_chase_rate_last_10,

        TRY_CAST(h.avg_zone_swing_rate_last_3 AS float) AS avg_zone_swing_rate_last_3,
        TRY_CAST(h.avg_zone_swing_rate_last_5 AS float) AS avg_zone_swing_rate_last_5,
        TRY_CAST(h.avg_zone_swing_rate_last_10 AS float) AS avg_zone_swing_rate_last_10,

        TRY_CAST(h.avg_zone_rate_last_3 AS float) AS avg_zone_rate_last_3,
        TRY_CAST(h.avg_zone_rate_last_5 AS float) AS avg_zone_rate_last_5,
        TRY_CAST(h.avg_zone_rate_last_10 AS float) AS avg_zone_rate_last_10,

        TRY_CAST(h.avg_called_strike_rate_last_3 AS float) AS avg_called_strike_rate_last_3,
        TRY_CAST(h.avg_called_strike_rate_last_5 AS float) AS avg_called_strike_rate_last_5,
        TRY_CAST(h.avg_called_strike_rate_last_10 AS float) AS avg_called_strike_rate_last_10,

        TRY_CAST(h.avg_csw_against_rate_last_3 AS float) AS avg_csw_against_rate_last_3,
        TRY_CAST(h.avg_csw_against_rate_last_5 AS float) AS avg_csw_against_rate_last_5,
        TRY_CAST(h.avg_csw_against_rate_last_10 AS float) AS avg_csw_against_rate_last_10,

        TRY_CAST(h.avg_two_strike_whiff_rate_last_3 AS float) AS avg_two_strike_whiff_rate_last_3,
        TRY_CAST(h.avg_two_strike_whiff_rate_last_5 AS float) AS avg_two_strike_whiff_rate_last_5,
        TRY_CAST(h.avg_two_strike_whiff_rate_last_10 AS float) AS avg_two_strike_whiff_rate_last_10,

        TRY_CAST(h.avg_exit_velocity_last_3 AS float) AS avg_exit_velocity_last_3,
        TRY_CAST(h.avg_exit_velocity_last_5 AS float) AS avg_exit_velocity_last_5,
        TRY_CAST(h.avg_exit_velocity_last_10 AS float) AS avg_exit_velocity_last_10,

        TRY_CAST(h.avg_xwoba_last_3 AS float) AS avg_xwoba_last_3,
        TRY_CAST(h.avg_xwoba_last_5 AS float) AS avg_xwoba_last_5,
        TRY_CAST(h.avg_xwoba_last_10 AS float) AS avg_xwoba_last_10,

        TRY_CAST(h.avg_bat_speed_last_3 AS float) AS avg_bat_speed_last_3,
        TRY_CAST(h.avg_bat_speed_last_5 AS float) AS avg_bat_speed_last_5,
        TRY_CAST(h.avg_bat_speed_last_10 AS float) AS avg_bat_speed_last_10,

        TRY_CAST(h.avg_whiff_rate_vs_rhp_last_3 AS float) AS avg_whiff_rate_vs_rhp_last_3,
        TRY_CAST(h.avg_whiff_rate_vs_rhp_last_5 AS float) AS avg_whiff_rate_vs_rhp_last_5,
        TRY_CAST(h.avg_whiff_rate_vs_rhp_last_10 AS float) AS avg_whiff_rate_vs_rhp_last_10,

        TRY_CAST(h.avg_whiff_rate_vs_lhp_last_3 AS float) AS avg_whiff_rate_vs_lhp_last_3,
        TRY_CAST(h.avg_whiff_rate_vs_lhp_last_5 AS float) AS avg_whiff_rate_vs_lhp_last_5,
        TRY_CAST(h.avg_whiff_rate_vs_lhp_last_10 AS float) AS avg_whiff_rate_vs_lhp_last_10,

        /* -------------------- NEW VOLUME-WEIGHTED STATCAST -------------------- */
        TRY_CAST(h.weighted_whiff_rate_last_3 AS float) AS weighted_whiff_rate_last_3,
        TRY_CAST(h.weighted_whiff_rate_last_5 AS float) AS weighted_whiff_rate_last_5,
        TRY_CAST(h.weighted_whiff_rate_last_10 AS float) AS weighted_whiff_rate_last_10,

        TRY_CAST(h.weighted_contact_rate_last_3 AS float) AS weighted_contact_rate_last_3,
        TRY_CAST(h.weighted_contact_rate_last_5 AS float) AS weighted_contact_rate_last_5,
        TRY_CAST(h.weighted_contact_rate_last_10 AS float) AS weighted_contact_rate_last_10,

        TRY_CAST(h.weighted_swing_rate_last_3 AS float) AS weighted_swing_rate_last_3,
        TRY_CAST(h.weighted_swing_rate_last_5 AS float) AS weighted_swing_rate_last_5,
        TRY_CAST(h.weighted_swing_rate_last_10 AS float) AS weighted_swing_rate_last_10,

        TRY_CAST(h.weighted_chase_rate_last_3 AS float) AS weighted_chase_rate_last_3,
        TRY_CAST(h.weighted_chase_rate_last_5 AS float) AS weighted_chase_rate_last_5,
        TRY_CAST(h.weighted_chase_rate_last_10 AS float) AS weighted_chase_rate_last_10,

        TRY_CAST(h.weighted_zone_swing_rate_last_3 AS float) AS weighted_zone_swing_rate_last_3,
        TRY_CAST(h.weighted_zone_swing_rate_last_5 AS float) AS weighted_zone_swing_rate_last_5,
        TRY_CAST(h.weighted_zone_swing_rate_last_10 AS float) AS weighted_zone_swing_rate_last_10,

        TRY_CAST(h.weighted_zone_rate_last_3 AS float) AS weighted_zone_rate_last_3,
        TRY_CAST(h.weighted_zone_rate_last_5 AS float) AS weighted_zone_rate_last_5,
        TRY_CAST(h.weighted_zone_rate_last_10 AS float) AS weighted_zone_rate_last_10,

        TRY_CAST(h.weighted_called_strike_rate_last_3 AS float) AS weighted_called_strike_rate_last_3,
        TRY_CAST(h.weighted_called_strike_rate_last_5 AS float) AS weighted_called_strike_rate_last_5,
        TRY_CAST(h.weighted_called_strike_rate_last_10 AS float) AS weighted_called_strike_rate_last_10,

        TRY_CAST(h.weighted_csw_against_rate_last_3 AS float) AS weighted_csw_against_rate_last_3,
        TRY_CAST(h.weighted_csw_against_rate_last_5 AS float) AS weighted_csw_against_rate_last_5,
        TRY_CAST(h.weighted_csw_against_rate_last_10 AS float) AS weighted_csw_against_rate_last_10,

        TRY_CAST(h.weighted_two_strike_whiff_rate_last_3 AS float) AS weighted_two_strike_whiff_rate_last_3,
        TRY_CAST(h.weighted_two_strike_whiff_rate_last_5 AS float) AS weighted_two_strike_whiff_rate_last_5,
        TRY_CAST(h.weighted_two_strike_whiff_rate_last_10 AS float) AS weighted_two_strike_whiff_rate_last_10,

        TRY_CAST(h.weighted_whiff_rate_vs_rhp_last_3 AS float) AS weighted_whiff_rate_vs_rhp_last_3,
        TRY_CAST(h.weighted_whiff_rate_vs_rhp_last_5 AS float) AS weighted_whiff_rate_vs_rhp_last_5,
        TRY_CAST(h.weighted_whiff_rate_vs_rhp_last_10 AS float) AS weighted_whiff_rate_vs_rhp_last_10,

        TRY_CAST(h.weighted_whiff_rate_vs_lhp_last_3 AS float) AS weighted_whiff_rate_vs_lhp_last_3,
        TRY_CAST(h.weighted_whiff_rate_vs_lhp_last_5 AS float) AS weighted_whiff_rate_vs_lhp_last_5,
        TRY_CAST(h.weighted_whiff_rate_vs_lhp_last_10 AS float) AS weighted_whiff_rate_vs_lhp_last_10

    FROM lineup_base lb
    LEFT JOIN mlb.dbo.fact_hitter_model_features h
        ON lb.gamePk = h.gamePk
       AND lb.player_id = h.player_id
       AND lb.season = h.season
),

agg AS (
    SELECT
        gamePk,
        game_date,
        season,
        side,
        team_id,
        team_name,

        COUNT(*) AS lineup_spots,
        COUNT(CASE WHEN avg_k_last_3 IS NOT NULL THEN 1 END) AS matched_hitter_feature_rows,

        /* -------------------- SIMPLE AVERAGES: CORE -------------------- */
        AVG(avg_k_last_3) AS lineup_avg_k_last_3,
        AVG(avg_k_last_5) AS lineup_avg_k_last_5,
        AVG(avg_k_last_10) AS lineup_avg_k_last_10,

        AVG(avg_pa_last_3) AS lineup_avg_pa_last_3,
        AVG(avg_pa_last_5) AS lineup_avg_pa_last_5,
        AVG(avg_pa_last_10) AS lineup_avg_pa_last_10,

        AVG(avg_ab_last_3) AS lineup_avg_ab_last_3,
        AVG(avg_ab_last_5) AS lineup_avg_ab_last_5,
        AVG(avg_ab_last_10) AS lineup_avg_ab_last_10,

        AVG(avg_hits_last_3) AS lineup_avg_hits_last_3,
        AVG(avg_hits_last_5) AS lineup_avg_hits_last_5,
        AVG(avg_hits_last_10) AS lineup_avg_hits_last_10,

        AVG(avg_hr_last_3) AS lineup_avg_hr_last_3,
        AVG(avg_hr_last_5) AS lineup_avg_hr_last_5,
        AVG(avg_hr_last_10) AS lineup_avg_hr_last_10,

        AVG(avg_bb_last_3) AS lineup_avg_bb_last_3,
        AVG(avg_bb_last_5) AS lineup_avg_bb_last_5,
        AVG(avg_bb_last_10) AS lineup_avg_bb_last_10,

        AVG(avg_pitches_last_3) AS lineup_avg_pitches_last_3,
        AVG(avg_pitches_last_5) AS lineup_avg_pitches_last_5,
        AVG(avg_pitches_last_10) AS lineup_avg_pitches_last_10,

        AVG(avg_tb_last_3) AS lineup_avg_tb_last_3,
        AVG(avg_tb_last_5) AS lineup_avg_tb_last_5,
        AVG(avg_tb_last_10) AS lineup_avg_tb_last_10,

        AVG(avg_rbi_last_3) AS lineup_avg_rbi_last_3,
        AVG(avg_rbi_last_5) AS lineup_avg_rbi_last_5,
        AVG(avg_rbi_last_10) AS lineup_avg_rbi_last_10,

        AVG(avg_obp_last_3) AS lineup_avg_obp_last_3,
        AVG(avg_obp_last_5) AS lineup_avg_obp_last_5,
        AVG(avg_obp_last_10) AS lineup_avg_obp_last_10,

        AVG(avg_slg_last_3) AS lineup_avg_slg_last_3,
        AVG(avg_slg_last_5) AS lineup_avg_slg_last_5,
        AVG(avg_slg_last_10) AS lineup_avg_slg_last_10,

        AVG(avg_ops_last_3) AS lineup_avg_ops_last_3,
        AVG(avg_ops_last_5) AS lineup_avg_ops_last_5,
        AVG(avg_ops_last_10) AS lineup_avg_ops_last_10,

        AVG(avg_batting_avg_last_3) AS lineup_avg_batting_avg_last_3,
        AVG(avg_batting_avg_last_5) AS lineup_avg_batting_avg_last_5,
        AVG(avg_batting_avg_last_10) AS lineup_avg_batting_avg_last_10,

        AVG(avg_k_rate_last_3) AS lineup_avg_k_rate_last_3,
        AVG(avg_k_rate_last_5) AS lineup_avg_k_rate_last_5,
        AVG(avg_k_rate_last_10) AS lineup_avg_k_rate_last_10,

        AVG(avg_walk_rate_last_3) AS lineup_avg_walk_rate_last_3,
        AVG(avg_walk_rate_last_5) AS lineup_avg_walk_rate_last_5,
        AVG(avg_walk_rate_last_10) AS lineup_avg_walk_rate_last_10,

        AVG(avg_hit_rate_last_3) AS lineup_avg_hit_rate_last_3,
        AVG(avg_hit_rate_last_5) AS lineup_avg_hit_rate_last_5,
        AVG(avg_hit_rate_last_10) AS lineup_avg_hit_rate_last_10,

        AVG(avg_tb_rate_last_3) AS lineup_avg_tb_rate_last_3,
        AVG(avg_tb_rate_last_5) AS lineup_avg_tb_rate_last_5,
        AVG(avg_tb_rate_last_10) AS lineup_avg_tb_rate_last_10,

        AVG(avg_hr_rate_last_3) AS lineup_avg_hr_rate_last_3,
        AVG(avg_hr_rate_last_5) AS lineup_avg_hr_rate_last_5,
        AVG(avg_hr_rate_last_10) AS lineup_avg_hr_rate_last_10,

        AVG(pct_1plus_k_last_3) AS lineup_avg_pct_1plus_k_last_3,
        AVG(pct_1plus_k_last_5) AS lineup_avg_pct_1plus_k_last_5,
        AVG(pct_1plus_k_last_10) AS lineup_avg_pct_1plus_k_last_10,

        AVG(pct_2plus_k_last_3) AS lineup_avg_pct_2plus_k_last_3,
        AVG(pct_2plus_k_last_5) AS lineup_avg_pct_2plus_k_last_5,
        AVG(pct_2plus_k_last_10) AS lineup_avg_pct_2plus_k_last_10,

        /* -------------------- SIMPLE AVERAGES: STATCAST -------------------- */
        AVG(avg_sc_pitches_seen_last_3) AS lineup_avg_sc_pitches_seen_last_3,
        AVG(avg_sc_pitches_seen_last_5) AS lineup_avg_sc_pitches_seen_last_5,
        AVG(avg_sc_pitches_seen_last_10) AS lineup_avg_sc_pitches_seen_last_10,

        AVG(avg_whiff_rate_last_3) AS lineup_avg_whiff_rate_last_3,
        AVG(avg_whiff_rate_last_5) AS lineup_avg_whiff_rate_last_5,
        AVG(avg_whiff_rate_last_10) AS lineup_avg_whiff_rate_last_10,

        AVG(avg_contact_rate_last_3) AS lineup_avg_contact_rate_last_3,
        AVG(avg_contact_rate_last_5) AS lineup_avg_contact_rate_last_5,
        AVG(avg_contact_rate_last_10) AS lineup_avg_contact_rate_last_10,

        AVG(avg_swing_rate_last_3) AS lineup_avg_swing_rate_last_3,
        AVG(avg_swing_rate_last_5) AS lineup_avg_swing_rate_last_5,
        AVG(avg_swing_rate_last_10) AS lineup_avg_swing_rate_last_10,

        AVG(avg_chase_rate_last_3) AS lineup_avg_chase_rate_last_3,
        AVG(avg_chase_rate_last_5) AS lineup_avg_chase_rate_last_5,
        AVG(avg_chase_rate_last_10) AS lineup_avg_chase_rate_last_10,

        AVG(avg_zone_swing_rate_last_3) AS lineup_avg_zone_swing_rate_last_3,
        AVG(avg_zone_swing_rate_last_5) AS lineup_avg_zone_swing_rate_last_5,
        AVG(avg_zone_swing_rate_last_10) AS lineup_avg_zone_swing_rate_last_10,

        AVG(avg_zone_rate_last_3) AS lineup_avg_zone_rate_last_3,
        AVG(avg_zone_rate_last_5) AS lineup_avg_zone_rate_last_5,
        AVG(avg_zone_rate_last_10) AS lineup_avg_zone_rate_last_10,

        AVG(avg_called_strike_rate_last_3) AS lineup_avg_called_strike_rate_last_3,
        AVG(avg_called_strike_rate_last_5) AS lineup_avg_called_strike_rate_last_5,
        AVG(avg_called_strike_rate_last_10) AS lineup_avg_called_strike_rate_last_10,

        AVG(avg_csw_against_rate_last_3) AS lineup_avg_csw_against_rate_last_3,
        AVG(avg_csw_against_rate_last_5) AS lineup_avg_csw_against_rate_last_5,
        AVG(avg_csw_against_rate_last_10) AS lineup_avg_csw_against_rate_last_10,

        AVG(avg_two_strike_whiff_rate_last_3) AS lineup_avg_two_strike_whiff_rate_last_3,
        AVG(avg_two_strike_whiff_rate_last_5) AS lineup_avg_two_strike_whiff_rate_last_5,
        AVG(avg_two_strike_whiff_rate_last_10) AS lineup_avg_two_strike_whiff_rate_last_10,

        AVG(avg_exit_velocity_last_3) AS lineup_avg_exit_velocity_last_3,
        AVG(avg_exit_velocity_last_5) AS lineup_avg_exit_velocity_last_5,
        AVG(avg_exit_velocity_last_10) AS lineup_avg_exit_velocity_last_10,

        AVG(avg_xwoba_last_3) AS lineup_avg_xwoba_last_3,
        AVG(avg_xwoba_last_5) AS lineup_avg_xwoba_last_5,
        AVG(avg_xwoba_last_10) AS lineup_avg_xwoba_last_10,

        AVG(avg_bat_speed_last_3) AS lineup_avg_bat_speed_last_3,
        AVG(avg_bat_speed_last_5) AS lineup_avg_bat_speed_last_5,
        AVG(avg_bat_speed_last_10) AS lineup_avg_bat_speed_last_10,

        AVG(avg_whiff_rate_vs_rhp_last_3) AS lineup_avg_whiff_rate_vs_rhp_last_3,
        AVG(avg_whiff_rate_vs_rhp_last_5) AS lineup_avg_whiff_rate_vs_rhp_last_5,
        AVG(avg_whiff_rate_vs_rhp_last_10) AS lineup_avg_whiff_rate_vs_rhp_last_10,

        AVG(avg_whiff_rate_vs_lhp_last_3) AS lineup_avg_whiff_rate_vs_lhp_last_3,
        AVG(avg_whiff_rate_vs_lhp_last_5) AS lineup_avg_whiff_rate_vs_lhp_last_5,
        AVG(avg_whiff_rate_vs_lhp_last_10) AS lineup_avg_whiff_rate_vs_lhp_last_10,

        /* -------------------- LINEUP-ORDER WEIGHTED SIMPLE FEATURES -------------------- */
        SUM(avg_k_last_3 * lineup_weight) / NULLIF(SUM(CASE WHEN avg_k_last_3 IS NOT NULL THEN lineup_weight END), 0) AS lineup_wavg_k_last_3,
        SUM(avg_k_last_5 * lineup_weight) / NULLIF(SUM(CASE WHEN avg_k_last_5 IS NOT NULL THEN lineup_weight END), 0) AS lineup_wavg_k_last_5,
        SUM(avg_k_last_10 * lineup_weight) / NULLIF(SUM(CASE WHEN avg_k_last_10 IS NOT NULL THEN lineup_weight END), 0) AS lineup_wavg_k_last_10,

        SUM(avg_pa_last_3 * lineup_weight) / NULLIF(SUM(CASE WHEN avg_pa_last_3 IS NOT NULL THEN lineup_weight END), 0) AS lineup_wavg_pa_last_3,
        SUM(avg_pa_last_5 * lineup_weight) / NULLIF(SUM(CASE WHEN avg_pa_last_5 IS NOT NULL THEN lineup_weight END), 0) AS lineup_wavg_pa_last_5,
        SUM(avg_pa_last_10 * lineup_weight) / NULLIF(SUM(CASE WHEN avg_pa_last_10 IS NOT NULL THEN lineup_weight END), 0) AS lineup_wavg_pa_last_10,

        SUM(avg_ops_last_3 * lineup_weight) / NULLIF(SUM(CASE WHEN avg_ops_last_3 IS NOT NULL THEN lineup_weight END), 0) AS lineup_wavg_ops_last_3,
        SUM(avg_ops_last_5 * lineup_weight) / NULLIF(SUM(CASE WHEN avg_ops_last_5 IS NOT NULL THEN lineup_weight END), 0) AS lineup_wavg_ops_last_5,
        SUM(avg_ops_last_10 * lineup_weight) / NULLIF(SUM(CASE WHEN avg_ops_last_10 IS NOT NULL THEN lineup_weight END), 0) AS lineup_wavg_ops_last_10,

        SUM(avg_whiff_rate_last_3 * lineup_weight) / NULLIF(SUM(CASE WHEN avg_whiff_rate_last_3 IS NOT NULL THEN lineup_weight END), 0) AS lineup_wavg_whiff_rate_last_3,
        SUM(avg_whiff_rate_last_5 * lineup_weight) / NULLIF(SUM(CASE WHEN avg_whiff_rate_last_5 IS NOT NULL THEN lineup_weight END), 0) AS lineup_wavg_whiff_rate_last_5,
        SUM(avg_whiff_rate_last_10 * lineup_weight) / NULLIF(SUM(CASE WHEN avg_whiff_rate_last_10 IS NOT NULL THEN lineup_weight END), 0) AS lineup_wavg_whiff_rate_last_10,

        SUM(avg_csw_against_rate_last_3 * lineup_weight) / NULLIF(SUM(CASE WHEN avg_csw_against_rate_last_3 IS NOT NULL THEN lineup_weight END), 0) AS lineup_wavg_csw_against_rate_last_3,
        SUM(avg_csw_against_rate_last_5 * lineup_weight) / NULLIF(SUM(CASE WHEN avg_csw_against_rate_last_5 IS NOT NULL THEN lineup_weight END), 0) AS lineup_wavg_csw_against_rate_last_5,
        SUM(avg_csw_against_rate_last_10 * lineup_weight) / NULLIF(SUM(CASE WHEN avg_csw_against_rate_last_10 IS NOT NULL THEN lineup_weight END), 0) AS lineup_wavg_csw_against_rate_last_10,

        SUM(avg_xwoba_last_3 * lineup_weight) / NULLIF(SUM(CASE WHEN avg_xwoba_last_3 IS NOT NULL THEN lineup_weight END), 0) AS lineup_wavg_xwoba_last_3,
        SUM(avg_xwoba_last_5 * lineup_weight) / NULLIF(SUM(CASE WHEN avg_xwoba_last_5 IS NOT NULL THEN lineup_weight END), 0) AS lineup_wavg_xwoba_last_5,
        SUM(avg_xwoba_last_10 * lineup_weight) / NULLIF(SUM(CASE WHEN avg_xwoba_last_10 IS NOT NULL THEN lineup_weight END), 0) AS lineup_wavg_xwoba_last_10,

        /* -------------------- LINEUP-ORDER WEIGHTED NEW VOLUME FEATURES -------------------- */
        SUM(weighted_k_rate_last_3 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_k_rate_last_3 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_k_rate_last_3,
        SUM(weighted_k_rate_last_5 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_k_rate_last_5 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_k_rate_last_5,
        SUM(weighted_k_rate_last_10 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_k_rate_last_10 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_k_rate_last_10,

        SUM(weighted_walk_rate_last_3 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_walk_rate_last_3 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_walk_rate_last_3,
        SUM(weighted_walk_rate_last_5 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_walk_rate_last_5 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_walk_rate_last_5,
        SUM(weighted_walk_rate_last_10 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_walk_rate_last_10 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_walk_rate_last_10,

        SUM(weighted_hit_rate_last_3 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_hit_rate_last_3 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_hit_rate_last_3,
        SUM(weighted_hit_rate_last_5 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_hit_rate_last_5 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_hit_rate_last_5,
        SUM(weighted_hit_rate_last_10 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_hit_rate_last_10 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_hit_rate_last_10,

        SUM(weighted_tb_rate_last_3 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_tb_rate_last_3 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_tb_rate_last_3,
        SUM(weighted_tb_rate_last_5 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_tb_rate_last_5 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_tb_rate_last_5,
        SUM(weighted_tb_rate_last_10 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_tb_rate_last_10 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_tb_rate_last_10,

        SUM(weighted_hr_rate_last_3 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_hr_rate_last_3 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_hr_rate_last_3,
        SUM(weighted_hr_rate_last_5 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_hr_rate_last_5 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_hr_rate_last_5,
        SUM(weighted_hr_rate_last_10 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_hr_rate_last_10 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_hr_rate_last_10,

        SUM(weighted_obp_last_3 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_obp_last_3 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_obp_last_3,
        SUM(weighted_obp_last_5 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_obp_last_5 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_obp_last_5,
        SUM(weighted_obp_last_10 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_obp_last_10 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_obp_last_10,

        SUM(weighted_slg_last_3 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_slg_last_3 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_slg_last_3,
        SUM(weighted_slg_last_5 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_slg_last_5 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_slg_last_5,
        SUM(weighted_slg_last_10 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_slg_last_10 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_slg_last_10,

        SUM(weighted_ops_last_3 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_ops_last_3 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_ops_last_3,
        SUM(weighted_ops_last_5 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_ops_last_5 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_ops_last_5,
        SUM(weighted_ops_last_10 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_ops_last_10 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_ops_last_10,

        SUM(weighted_whiff_rate_last_3 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_whiff_rate_last_3 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_whiff_rate_last_3,
        SUM(weighted_whiff_rate_last_5 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_whiff_rate_last_5 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_whiff_rate_last_5,
        SUM(weighted_whiff_rate_last_10 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_whiff_rate_last_10 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_whiff_rate_last_10,

        SUM(weighted_csw_against_rate_last_3 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_csw_against_rate_last_3 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_csw_against_rate_last_3,
        SUM(weighted_csw_against_rate_last_5 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_csw_against_rate_last_5 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_csw_against_rate_last_5,
        SUM(weighted_csw_against_rate_last_10 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_csw_against_rate_last_10 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_csw_against_rate_last_10,

        SUM(weighted_whiff_rate_vs_rhp_last_3 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_whiff_rate_vs_rhp_last_3 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_whiff_rate_vs_rhp_last_3,
        SUM(weighted_whiff_rate_vs_rhp_last_5 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_whiff_rate_vs_rhp_last_5 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_whiff_rate_vs_rhp_last_5,
        SUM(weighted_whiff_rate_vs_rhp_last_10 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_whiff_rate_vs_rhp_last_10 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_whiff_rate_vs_rhp_last_10,

        SUM(weighted_whiff_rate_vs_lhp_last_3 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_whiff_rate_vs_lhp_last_3 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_whiff_rate_vs_lhp_last_3,
        SUM(weighted_whiff_rate_vs_lhp_last_5 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_whiff_rate_vs_lhp_last_5 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_whiff_rate_vs_lhp_last_5,
        SUM(weighted_whiff_rate_vs_lhp_last_10 * lineup_weight) / NULLIF(SUM(CASE WHEN weighted_whiff_rate_vs_lhp_last_10 IS NOT NULL THEN lineup_weight END), 0) AS lineup_weighted_whiff_rate_vs_lhp_last_10,

        /* -------------------- COUNTS -------------------- */
        SUM(CASE WHEN avg_k_last_5 >= 1.0 THEN 1 ELSE 0 END) AS lineup_num_high_k_hitters,
        SUM(CASE WHEN avg_hr_last_5 >= 0.4 THEN 1 ELSE 0 END) AS lineup_num_power_hitters,
        SUM(CASE WHEN avg_whiff_rate_last_5 >= 0.30 THEN 1 ELSE 0 END) AS lineup_num_high_whiff_hitters,
        SUM(CASE WHEN avg_ops_last_5 >= 0.800 THEN 1 ELSE 0 END) AS lineup_num_strong_bats

    FROM joined
    GROUP BY
        gamePk,
        game_date,
        season,
        side,
        team_id,
        team_name
)

SELECT *
INTO mlb.dbo.fact_lineup_agg_features
FROM agg;
    """

    execute_sql(sql)
    logger.info("Finished building mlb.dbo.fact_lineup_agg_features")


def run_all_lineup_agg_features() -> None:
    logger.info("Starting lineup aggregate feature pipeline")
    build_fact_lineup_agg_features()
    logger.info("Finished lineup aggregate feature pipeline")


if __name__ == "__main__":
    run_all_lineup_agg_features()