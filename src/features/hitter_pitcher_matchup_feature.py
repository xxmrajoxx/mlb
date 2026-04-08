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
    -- identifiers
    h.gamePk,
    h.game_date,
    h.season,

    h.player_id AS hitter_id,
    h.player_name AS hitter_name,
    h.position AS hitter_position,
    h.team_id AS hitter_team_id,
    h.team_name AS hitter_team_name,

    p.player_id AS pitcher_id,
    p.player_name AS pitcher_name,
    p.team_id AS pitcher_team_id,
    p.team_name AS pitcher_team_name,

    m.pitcher_throws,
    m.hitter_stand,

    -- target
    h.strikeOuts AS hitter_strikeOuts,

    -- direct matchup bridge
    m.pitches_seen_vs_pitcher,
    m.swings_vs_pitcher,
    m.whiffs_vs_pitcher,
    m.called_strikes_vs_pitcher,
    m.matchup_whiff_rate,
    m.matchup_called_strike_rate,
    m.matchup_csw_rate,

    -- =====================================
    -- HITTER FEATURES
    -- =====================================
    h.days_since_last_game AS hitter_days_since_last_game,

    -- hitter last 3
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
    h.avg_babip_last_3 AS hitter_avg_babip_last_3,
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

    -- hitter last 5
    h.avg_k_last_5 AS hitter_avg_k_last_5,
    h.avg_pa_last_5 AS hitter_avg_pa_last_5,
    h.avg_ab_last_5 AS hitter_avg_ab_last_5,
    h.avg_hits_last_5 AS hitter_avg_hits_last_5,
    h.avg_hr_last_5 AS hitter_avg_hr_last_5,
    h.avg_bb_last_5 AS hitter_avg_bb_last_5,
    h.avg_pitches_last_5 AS hitter_avg_pitches_last_5,
    h.avg_tb_last_5 AS hitter_avg_tb_last_5,
    h.avg_rbi_last_5 AS hitter_avg_rbi_last_5,
    h.avg_obp_last_5 AS hitter_avg_obp_last_5,
    h.avg_slg_last_5 AS hitter_avg_slg_last_5,
    h.avg_ops_last_5 AS hitter_avg_ops_last_5,
    h.avg_babip_last_5 AS hitter_avg_babip_last_5,
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

    -- hitter previous game
    h.prev_k AS hitter_prev_k,
    h.prev_pa AS hitter_prev_pa,
    h.prev_ab AS hitter_prev_ab,
    h.prev_hits AS hitter_prev_hits,
    h.prev_hr AS hitter_prev_hr,
    h.prev_bb AS hitter_prev_bb,
    h.prev_pitches AS hitter_prev_pitches,
    h.prev_ops AS hitter_prev_ops,
    h.prev_k_rate AS hitter_prev_k_rate,

    -- hitter statcast rolling
    h.avg_whiff_rate_last_3 AS hitter_avg_whiff_rate_last_3,
    h.avg_contact_rate_last_3 AS hitter_avg_contact_rate_last_3,
    h.avg_swing_rate_last_3 AS hitter_avg_swing_rate_last_3,
    h.avg_chase_rate_last_3 AS hitter_avg_chase_rate_last_3,
    h.avg_zone_swing_rate_last_3 AS hitter_avg_zone_swing_rate_last_3,
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
    h.avg_horz_movement_seen_last_3 AS hitter_avg_horz_movement_seen_last_3,
    h.avg_vert_movement_seen_last_3 AS hitter_avg_vert_movement_seen_last_3,
    h.avg_ff_seen_pct_last_3 AS hitter_avg_ff_seen_pct_last_3,
    h.avg_sl_seen_pct_last_3 AS hitter_avg_sl_seen_pct_last_3,
    h.avg_cu_seen_pct_last_3 AS hitter_avg_cu_seen_pct_last_3,
    h.avg_ch_seen_pct_last_3 AS hitter_avg_ch_seen_pct_last_3,
    h.avg_whiff_rate_vs_rhp_last_3 AS hitter_avg_whiff_rate_vs_rhp_last_3,
    h.avg_whiff_rate_vs_lhp_last_3 AS hitter_avg_whiff_rate_vs_lhp_last_3,

    h.avg_sc_pitches_seen_last_5 AS hitter_avg_sc_pitches_seen_last_5,
    h.avg_whiff_rate_last_5 AS hitter_avg_whiff_rate_last_5,
    h.avg_contact_rate_last_5 AS hitter_avg_contact_rate_last_5,
    h.avg_chase_rate_last_5 AS hitter_avg_chase_rate_last_5,
    h.avg_zone_swing_rate_last_5 AS hitter_avg_zone_swing_rate_last_5,
    h.avg_csw_against_rate_last_5 AS hitter_avg_csw_against_rate_last_5,
    h.avg_exit_velocity_last_5 AS hitter_avg_exit_velocity_last_5,
    h.avg_xwoba_last_5 AS hitter_avg_xwoba_last_5,
    h.avg_bat_speed_last_5 AS hitter_avg_bat_speed_last_5,
    h.avg_whiff_rate_vs_rhp_last_5 AS hitter_avg_whiff_rate_vs_rhp_last_5,
    h.avg_whiff_rate_vs_lhp_last_5 AS hitter_avg_whiff_rate_vs_lhp_last_5,

    h.prev_whiff_rate AS hitter_prev_whiff_rate,
    h.prev_contact_rate AS hitter_prev_contact_rate,
    h.prev_chase_rate AS hitter_prev_chase_rate,
    h.prev_exit_velocity AS hitter_prev_exit_velocity,
    h.prev_xwoba AS hitter_prev_xwoba,
    h.prev_bat_speed AS hitter_prev_bat_speed,

    -- =====================================
    -- PITCHER FEATURES
    -- =====================================
    p.gamesStarted AS pitcher_gamesStarted,
    p.days_since_last_appearance AS pitcher_days_since_last_appearance,

    -- pitcher rolling
    p.avg_games_started_last_3 AS pitcher_avg_games_started_last_3,
    p.avg_ip_last_3 AS pitcher_avg_ip_last_3,
    p.avg_bf_last_3 AS pitcher_avg_bf_last_3,
    p.avg_pitches_last_3 AS pitcher_avg_pitches_last_3,
    p.avg_outs_last_3 AS pitcher_avg_outs_last_3,
    p.avg_k9_last_3 AS pitcher_avg_k9_last_3,
    p.avg_strike_pct_last_3 AS pitcher_avg_strike_pct_last_3,
    p.avg_whip_last_3 AS pitcher_avg_whip_last_3,
    p.avg_bb_last_3 AS pitcher_avg_bb_last_3,
    p.avg_kbb_last_3 AS pitcher_avg_kbb_last_3,
    p.sum_pitches_last_3 AS pitcher_sum_pitches_last_3,
    p.pct_5plus_ip_last_3 AS pitcher_pct_5plus_ip_last_3,
    p.pct_6plus_ip_last_3 AS pitcher_pct_6plus_ip_last_3,

    p.avg_ip_last_5 AS pitcher_avg_ip_last_5,
    p.avg_bf_last_5 AS pitcher_avg_bf_last_5,
    p.avg_pitches_last_5 AS pitcher_avg_pitches_last_5,
    p.avg_k9_last_5 AS pitcher_avg_k9_last_5,

    p.prev_k AS pitcher_prev_k,
    p.prev_ip AS pitcher_prev_ip,
    p.prev_bf AS pitcher_prev_bf,
    p.prev_pitches AS pitcher_prev_pitches,
    p.prev_k9 AS pitcher_prev_k9,

    -- pitcher statcast
    p.avg_whiff_rate_last_3 AS pitcher_avg_whiff_rate_last_3,
    p.avg_csw_rate_last_3 AS pitcher_avg_csw_rate_last_3,
    p.avg_putaway_rate_last_3 AS pitcher_avg_putaway_rate_last_3,
    p.avg_swing_rate_last_3 AS pitcher_avg_swing_rate_last_3,
    p.avg_chase_rate_last_3 AS pitcher_avg_chase_rate_last_3,
    p.avg_zone_rate_last_3 AS pitcher_avg_zone_rate_last_3,
    p.avg_whiff_rate_1_2_last_3 AS pitcher_avg_whiff_rate_1_2_last_3,
    p.avg_whiff_rate_2_2_last_3 AS pitcher_avg_whiff_rate_2_2_last_3,
    p.avg_velocity_last_3 AS pitcher_avg_velocity_last_3,
    p.avg_spin_rate_last_3 AS pitcher_avg_spin_rate_last_3,
    p.avg_sl_whiff_rate_last_3 AS pitcher_avg_sl_whiff_rate_last_3,
    p.avg_ff_whiff_rate_last_3 AS pitcher_avg_ff_whiff_rate_last_3,
    p.prev_whiff_rate AS pitcher_prev_whiff_rate,
    p.prev_csw_rate AS pitcher_prev_csw_rate,
    p.prev_chase_rate AS pitcher_prev_chase_rate

INTO mlb.dbo.fact_hitter_pitcher_matchup_model_features
FROM mlb.dbo.fact_hitter_model_features h
INNER JOIN primary_matchup m
    ON h.gamePk = m.gamePk
   AND h.player_id = m.hitter_id
   AND h.season = m.season
LEFT JOIN mlb.dbo.fact_pitcher_model_features p
    ON m.gamePk = p.gamePk
   AND m.pitcher_id = p.player_id
   AND m.season = p.season;
    """

    execute_sql(sql)
    logger.info("Finished building mlb.dbo.fact_hitter_pitcher_matchup_model_features")

if __name__ == "__main__":
    build_hitter_pitcher_matchup_model_features()