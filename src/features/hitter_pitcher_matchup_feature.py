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

ranked_matchups AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY gamePk, hitter_id
            ORDER BY pitches_seen_vs_pitcher DESC, pitcher_id
        ) AS rn
    FROM matchup_counts
),

primary_matchup AS (
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
    FROM ranked_matchups
    WHERE rn = 1
)

SELECT
    /* =========================
       IDENTIFIERS
       ========================= */
    h.gamePk,
    h.game_date,
    h.season,

    h.player_id AS hitter_id,
    h.player_name AS hitter_name,
    h.position AS hitter_position,
    h.team_id AS hitter_team_id,
    h.team_name AS hitter_team_name,

    m.pitcher_id AS pitcher_id,
    p.player_name AS pitcher_name,
    p.team_id AS pitcher_team_id,
    p.team_name AS pitcher_team_name,

    m.pitcher_throws,
    m.hitter_stand,

    /* =========================
       TARGET
       ========================= */
    h.strikeOuts AS hitter_strikeOuts,

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
       HITTER FEATURES
       ========================= */
    h.days_since_last_game AS hitter_days_since_last_game,

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

    h.wavg_k_last_3 AS hitter_wavg_k_last_3,
    h.wavg_pa_last_3 AS hitter_wavg_pa_last_3,
    h.wavg_ab_last_3 AS hitter_wavg_ab_last_3,
    h.wavg_hits_last_3 AS hitter_wavg_hits_last_3,
    h.wavg_hr_last_3 AS hitter_wavg_hr_last_3,
    h.wavg_bb_last_3 AS hitter_wavg_bb_last_3,
    h.wavg_pitches_last_3 AS hitter_wavg_pitches_last_3,
    h.wavg_tb_last_3 AS hitter_wavg_tb_last_3,
    h.wavg_rbi_last_3 AS hitter_wavg_rbi_last_3,
    h.wavg_lob_last_3 AS hitter_wavg_lob_last_3,
    h.wavg_obp_last_3 AS hitter_wavg_obp_last_3,
    h.wavg_slg_last_3 AS hitter_wavg_slg_last_3,
    h.wavg_ops_last_3 AS hitter_wavg_ops_last_3,
    h.wavg_babip_last_3 AS hitter_wavg_babip_last_3,
    h.wavg_batting_avg_last_3 AS hitter_wavg_batting_avg_last_3,
    h.wavg_hbp_last_3 AS hitter_wavg_hbp_last_3,
    h.wavg_sf_last_3 AS hitter_wavg_sf_last_3,
    h.wavg_sbunts_last_3 AS hitter_wavg_sbunts_last_3,
    h.wavg_stolen_bases_last_3 AS hitter_wavg_stolen_bases_last_3,
    h.wavg_caught_stealing_last_3 AS hitter_wavg_caught_stealing_last_3,
    h.wavg_k_rate_last_3 AS hitter_wavg_k_rate_last_3,
    h.wavg_walk_rate_last_3 AS hitter_wavg_walk_rate_last_3,
    h.wavg_hit_rate_last_3 AS hitter_wavg_hit_rate_last_3,
    h.wavg_tb_rate_last_3 AS hitter_wavg_tb_rate_last_3,
    h.wavg_hr_rate_last_3 AS hitter_wavg_hr_rate_last_3,

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

    h.wavg_k_last_5 AS hitter_wavg_k_last_5,
    h.wavg_pa_last_5 AS hitter_wavg_pa_last_5,
    h.wavg_ab_last_5 AS hitter_wavg_ab_last_5,
    h.wavg_hits_last_5 AS hitter_wavg_hits_last_5,
    h.wavg_hr_last_5 AS hitter_wavg_hr_last_5,
    h.wavg_bb_last_5 AS hitter_wavg_bb_last_5,
    h.wavg_pitches_last_5 AS hitter_wavg_pitches_last_5,
    h.wavg_tb_last_5 AS hitter_wavg_tb_last_5,
    h.wavg_rbi_last_5 AS hitter_wavg_rbi_last_5,
    h.wavg_lob_last_5 AS hitter_wavg_lob_last_5,
    h.wavg_obp_last_5 AS hitter_wavg_obp_last_5,
    h.wavg_slg_last_5 AS hitter_wavg_slg_last_5,
    h.wavg_ops_last_5 AS hitter_wavg_ops_last_5,
    h.wavg_babip_last_5 AS hitter_wavg_babip_last_5,
    h.wavg_batting_avg_last_5 AS hitter_wavg_batting_avg_last_5,
    h.wavg_hbp_last_5 AS hitter_wavg_hbp_last_5,
    h.wavg_sf_last_5 AS hitter_wavg_sf_last_5,
    h.wavg_sbunts_last_5 AS hitter_wavg_sbunts_last_5,
    h.wavg_stolen_bases_last_5 AS hitter_wavg_stolen_bases_last_5,
    h.wavg_caught_stealing_last_5 AS hitter_wavg_caught_stealing_last_5,
    h.wavg_k_rate_last_5 AS hitter_wavg_k_rate_last_5,
    h.wavg_walk_rate_last_5 AS hitter_wavg_walk_rate_last_5,
    h.wavg_hit_rate_last_5 AS hitter_wavg_hit_rate_last_5,
    h.wavg_tb_rate_last_5 AS hitter_wavg_tb_rate_last_5,
    h.wavg_hr_rate_last_5 AS hitter_wavg_hr_rate_last_5,

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

    h.wavg_k_last_10 AS hitter_wavg_k_last_10,
    h.wavg_pa_last_10 AS hitter_wavg_pa_last_10,
    h.wavg_ab_last_10 AS hitter_wavg_ab_last_10,
    h.wavg_hits_last_10 AS hitter_wavg_hits_last_10,
    h.wavg_hr_last_10 AS hitter_wavg_hr_last_10,
    h.wavg_bb_last_10 AS hitter_wavg_bb_last_10,
    h.wavg_pitches_last_10 AS hitter_wavg_pitches_last_10,
    h.wavg_tb_last_10 AS hitter_wavg_tb_last_10,
    h.wavg_rbi_last_10 AS hitter_wavg_rbi_last_10,
    h.wavg_lob_last_10 AS hitter_wavg_lob_last_10,
    h.wavg_obp_last_10 AS hitter_wavg_obp_last_10,
    h.wavg_slg_last_10 AS hitter_wavg_slg_last_10,
    h.wavg_ops_last_10 AS hitter_wavg_ops_last_10,
    h.wavg_babip_last_10 AS hitter_wavg_babip_last_10,
    h.wavg_batting_avg_last_10 AS hitter_wavg_batting_avg_last_10,
    h.wavg_hbp_last_10 AS hitter_wavg_hbp_last_10,
    h.wavg_sf_last_10 AS hitter_wavg_sf_last_10,
    h.wavg_sbunts_last_10 AS hitter_wavg_sbunts_last_10,
    h.wavg_stolen_bases_last_10 AS hitter_wavg_stolen_bases_last_10,
    h.wavg_caught_stealing_last_10 AS hitter_wavg_caught_stealing_last_10,
    h.wavg_k_rate_last_10 AS hitter_wavg_k_rate_last_10,
    h.wavg_walk_rate_last_10 AS hitter_wavg_walk_rate_last_10,
    h.wavg_hit_rate_last_10 AS hitter_wavg_hit_rate_last_10,
    h.wavg_tb_rate_last_10 AS hitter_wavg_tb_rate_last_10,
    h.wavg_hr_rate_last_10 AS hitter_wavg_hr_rate_last_10,

    h.prev_k AS hitter_prev_k,
    h.prev_pa AS hitter_prev_pa,
    h.prev_ab AS hitter_prev_ab,
    h.prev_hits AS hitter_prev_hits,
    h.prev_hr AS hitter_prev_hr,
    h.prev_bb AS hitter_prev_bb,
    h.prev_pitches AS hitter_prev_pitches,
    h.prev_ops AS hitter_prev_ops,
    h.prev_k_rate AS hitter_prev_k_rate,

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

    h.wavg_sc_pitches_seen_last_3 AS hitter_wavg_sc_pitches_seen_last_3,
    h.wavg_whiff_rate_last_3 AS hitter_wavg_whiff_rate_last_3,
    h.wavg_contact_rate_last_3 AS hitter_wavg_contact_rate_last_3,
    h.wavg_swing_rate_last_3 AS hitter_wavg_swing_rate_last_3,
    h.wavg_chase_rate_last_3 AS hitter_wavg_chase_rate_last_3,
    h.wavg_zone_swing_rate_last_3 AS hitter_wavg_zone_swing_rate_last_3,
    h.wavg_zone_rate_last_3 AS hitter_wavg_zone_rate_last_3,
    h.wavg_called_strike_rate_last_3 AS hitter_wavg_called_strike_rate_last_3,
    h.wavg_csw_against_rate_last_3 AS hitter_wavg_csw_against_rate_last_3,
    h.wavg_two_strike_whiff_rate_last_3 AS hitter_wavg_two_strike_whiff_rate_last_3,
    h.wavg_whiff_rate_0_2_last_3 AS hitter_wavg_whiff_rate_0_2_last_3,
    h.wavg_whiff_rate_1_2_last_3 AS hitter_wavg_whiff_rate_1_2_last_3,
    h.wavg_whiff_rate_2_2_last_3 AS hitter_wavg_whiff_rate_2_2_last_3,
    h.wavg_exit_velocity_last_3 AS hitter_wavg_exit_velocity_last_3,
    h.wavg_max_exit_velocity_last_3 AS hitter_wavg_max_exit_velocity_last_3,
    h.wavg_launch_angle_last_3 AS hitter_wavg_launch_angle_last_3,
    h.wavg_hit_distance_last_3 AS hitter_wavg_hit_distance_last_3,
    h.wavg_xba_last_3 AS hitter_wavg_xba_last_3,
    h.wavg_xwoba_last_3 AS hitter_wavg_xwoba_last_3,
    h.wavg_woba_value_last_3 AS hitter_wavg_woba_value_last_3,
    h.wavg_babip_value_last_3 AS hitter_wavg_babip_value_last_3,
    h.wavg_iso_value_last_3 AS hitter_wavg_iso_value_last_3,
    h.wavg_bat_speed_last_3 AS hitter_wavg_bat_speed_last_3,
    h.wavg_swing_length_last_3 AS hitter_wavg_swing_length_last_3,
    h.wavg_pitch_velocity_seen_last_3 AS hitter_wavg_pitch_velocity_seen_last_3,
    h.wavg_pitch_spin_seen_last_3 AS hitter_wavg_pitch_spin_seen_last_3,
    h.wavg_pitch_extension_seen_last_3 AS hitter_wavg_pitch_extension_seen_last_3,
    h.wavg_horz_movement_seen_last_3 AS hitter_wavg_horz_movement_seen_last_3,
    h.wavg_vert_movement_seen_last_3 AS hitter_wavg_vert_movement_seen_last_3,
    h.wavg_plate_x_seen_last_3 AS hitter_wavg_plate_x_seen_last_3,
    h.wavg_plate_z_seen_last_3 AS hitter_wavg_plate_z_seen_last_3,
    h.wavg_ff_seen_pct_last_3 AS hitter_wavg_ff_seen_pct_last_3,
    h.wavg_si_seen_pct_last_3 AS hitter_wavg_si_seen_pct_last_3,
    h.wavg_fc_seen_pct_last_3 AS hitter_wavg_fc_seen_pct_last_3,
    h.wavg_sl_seen_pct_last_3 AS hitter_wavg_sl_seen_pct_last_3,
    h.wavg_cu_seen_pct_last_3 AS hitter_wavg_cu_seen_pct_last_3,
    h.wavg_ch_seen_pct_last_3 AS hitter_wavg_ch_seen_pct_last_3,
    h.wavg_fs_seen_pct_last_3 AS hitter_wavg_fs_seen_pct_last_3,
    h.wavg_whiff_rate_vs_rhp_last_3 AS hitter_wavg_whiff_rate_vs_rhp_last_3,
    h.wavg_whiff_rate_vs_lhp_last_3 AS hitter_wavg_whiff_rate_vs_lhp_last_3,

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

    h.wavg_sc_pitches_seen_last_5 AS hitter_wavg_sc_pitches_seen_last_5,
    h.wavg_whiff_rate_last_5 AS hitter_wavg_whiff_rate_last_5,
    h.wavg_contact_rate_last_5 AS hitter_wavg_contact_rate_last_5,
    h.wavg_swing_rate_last_5 AS hitter_wavg_swing_rate_last_5,
    h.wavg_chase_rate_last_5 AS hitter_wavg_chase_rate_last_5,
    h.wavg_zone_swing_rate_last_5 AS hitter_wavg_zone_swing_rate_last_5,
    h.wavg_zone_rate_last_5 AS hitter_wavg_zone_rate_last_5,
    h.wavg_called_strike_rate_last_5 AS hitter_wavg_called_strike_rate_last_5,
    h.wavg_csw_against_rate_last_5 AS hitter_wavg_csw_against_rate_last_5,
    h.wavg_two_strike_whiff_rate_last_5 AS hitter_wavg_two_strike_whiff_rate_last_5,
    h.wavg_exit_velocity_last_5 AS hitter_wavg_exit_velocity_last_5,
    h.wavg_xwoba_last_5 AS hitter_wavg_xwoba_last_5,
    h.wavg_bat_speed_last_5 AS hitter_wavg_bat_speed_last_5,
    h.wavg_whiff_rate_vs_rhp_last_5 AS hitter_wavg_whiff_rate_vs_rhp_last_5,
    h.wavg_whiff_rate_vs_lhp_last_5 AS hitter_wavg_whiff_rate_vs_lhp_last_5,

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

    h.wavg_sc_pitches_seen_last_10 AS hitter_wavg_sc_pitches_seen_last_10,
    h.wavg_whiff_rate_last_10 AS hitter_wavg_whiff_rate_last_10,
    h.wavg_contact_rate_last_10 AS hitter_wavg_contact_rate_last_10,
    h.wavg_swing_rate_last_10 AS hitter_wavg_swing_rate_last_10,
    h.wavg_chase_rate_last_10 AS hitter_wavg_chase_rate_last_10,
    h.wavg_zone_swing_rate_last_10 AS hitter_wavg_zone_swing_rate_last_10,
    h.wavg_zone_rate_last_10 AS hitter_wavg_zone_rate_last_10,
    h.wavg_called_strike_rate_last_10 AS hitter_wavg_called_strike_rate_last_10,
    h.wavg_csw_against_rate_last_10 AS hitter_wavg_csw_against_rate_last_10,
    h.wavg_two_strike_whiff_rate_last_10 AS hitter_wavg_two_strike_whiff_rate_last_10,
    h.wavg_exit_velocity_last_10 AS hitter_wavg_exit_velocity_last_10,
    h.wavg_xwoba_last_10 AS hitter_wavg_xwoba_last_10,
    h.wavg_bat_speed_last_10 AS hitter_wavg_bat_speed_last_10,
    h.wavg_whiff_rate_vs_rhp_last_10 AS hitter_wavg_whiff_rate_vs_rhp_last_10,
    h.wavg_whiff_rate_vs_lhp_last_10 AS hitter_wavg_whiff_rate_vs_lhp_last_10,

    h.prev_whiff_rate AS hitter_prev_whiff_rate,
    h.prev_contact_rate AS hitter_prev_contact_rate,
    h.prev_chase_rate AS hitter_prev_chase_rate,
    h.prev_exit_velocity AS hitter_prev_exit_velocity,
    h.prev_xwoba AS hitter_prev_xwoba,
    h.prev_bat_speed AS hitter_prev_bat_speed,

    /* =========================
       PITCHER FEATURES
       ========================= */
    p.gamesStarted AS pitcher_gamesStarted,
    p.days_since_last_appearance AS pitcher_days_since_last_appearance,
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

    p.prev_whiff_rate AS pitcher_prev_whiff_rate,
    p.prev_csw_rate AS pitcher_prev_csw_rate,
    p.prev_chase_rate AS pitcher_prev_chase_rate,

    pg.strikeOuts AS pitcher_strikeOuts

INTO mlb.dbo.fact_hitter_pitcher_matchup_model_features
FROM mlb.dbo.fact_hitter_model_features h
INNER JOIN primary_matchup m
    ON h.gamePk = m.gamePk
   AND h.player_id = m.hitter_id
   AND h.season = m.season
INNER JOIN mlb.dbo.fact_pitcher_model_features p
    ON m.gamePk = p.gamePk
   AND m.pitcher_id = p.player_id
   AND m.season = p.season
LEFT JOIN mlb.dbo.fact_player_pitching_gamelogs pg
    ON p.gamePk = pg.gamePk
   AND p.player_id = pg.player_id;
    """

    execute_sql(sql)
    logger.info("Finished building mlb.dbo.fact_hitter_pitcher_matchup_model_features")

if __name__ == "__main__":
    build_hitter_pitcher_matchup_model_features()