import logging
from sql.sql_loader import execute_sql

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


def build_fact_pitcher_vs_lineup_features() -> None:
    logger.info("Building mlb.dbo.fact_pitcher_vs_lineup_features")

    sql = """
IF OBJECT_ID('mlb.dbo.fact_pitcher_vs_lineup_features', 'U') IS NOT NULL
    DROP TABLE mlb.dbo.fact_pitcher_vs_lineup_features;

WITH pitcher_base AS (
    SELECT
        TRY_CAST(p.gamePk AS bigint) AS gamePk,
        CAST(p.game_date AS date) AS game_date,
        TRY_CAST(p.season AS int) AS season,
        TRY_CAST(p.player_id AS int) AS pitcher_id,
        p.player_name AS pitcher_name,
        TRY_CAST(p.team_id AS int) AS pitcher_team_id,
        p.team_name AS pitcher_team_name,
        TRY_CAST(p.gamesStarted AS float) AS gamesStarted,
        TRY_CAST(p.strikeOuts AS float) AS pitcher_strikeOuts,
        TRY_CAST(p.days_since_last_appearance AS float) AS days_since_last_appearance,

        /* -------------------- CORE PITCHER FEATURES -------------------- */
        TRY_CAST(p.avg_k_last_3 AS float) AS avg_k_last_3,
        TRY_CAST(p.avg_k_last_5 AS float) AS avg_k_last_5,
        TRY_CAST(p.avg_k_last_10 AS float) AS avg_k_last_10,

        TRY_CAST(p.avg_ip_last_3 AS float) AS avg_ip_last_3,
        TRY_CAST(p.avg_ip_last_5 AS float) AS avg_ip_last_5,
        TRY_CAST(p.avg_ip_last_10 AS float) AS avg_ip_last_10,

        TRY_CAST(p.avg_bf_last_3 AS float) AS avg_bf_last_3,
        TRY_CAST(p.avg_bf_last_5 AS float) AS avg_bf_last_5,
        TRY_CAST(p.avg_bf_last_10 AS float) AS avg_bf_last_10,

        TRY_CAST(p.avg_pitches_last_3 AS float) AS avg_pitches_last_3,
        TRY_CAST(p.avg_pitches_last_5 AS float) AS avg_pitches_last_5,
        TRY_CAST(p.avg_pitches_last_10 AS float) AS avg_pitches_last_10,

        TRY_CAST(p.avg_strike_pct_last_3 AS float) AS avg_strike_pct_last_3,
        TRY_CAST(p.avg_strike_pct_last_5 AS float) AS avg_strike_pct_last_5,
        TRY_CAST(p.avg_strike_pct_last_10 AS float) AS avg_strike_pct_last_10,

        TRY_CAST(p.avg_strikes_last_3 AS float) AS avg_strikes_last_3,
        TRY_CAST(p.avg_strikes_last_5 AS float) AS avg_strikes_last_5,
        TRY_CAST(p.avg_strikes_last_10 AS float) AS avg_strikes_last_10,

        TRY_CAST(p.avg_whip_last_3 AS float) AS avg_whip_last_3,
        TRY_CAST(p.avg_whip_last_5 AS float) AS avg_whip_last_5,
        TRY_CAST(p.avg_whip_last_10 AS float) AS avg_whip_last_10,

        TRY_CAST(p.avg_bb_last_3 AS float) AS avg_bb_last_3,
        TRY_CAST(p.avg_bb_last_5 AS float) AS avg_bb_last_5,
        TRY_CAST(p.avg_bb_last_10 AS float) AS avg_bb_last_10,

        TRY_CAST(p.avg_era_last_3 AS float) AS avg_era_last_3,
        TRY_CAST(p.avg_era_last_5 AS float) AS avg_era_last_5,
        TRY_CAST(p.avg_era_last_10 AS float) AS avg_era_last_10,

        TRY_CAST(p.avg_outs_last_3 AS float) AS avg_outs_last_3,
        TRY_CAST(p.avg_outs_last_5 AS float) AS avg_outs_last_5,
        TRY_CAST(p.avg_outs_last_10 AS float) AS avg_outs_last_10,

        TRY_CAST(p.avg_k9_last_3 AS float) AS avg_k9_last_3,
        TRY_CAST(p.avg_k9_last_5 AS float) AS avg_k9_last_5,
        TRY_CAST(p.avg_k9_last_10 AS float) AS avg_k9_last_10,

        TRY_CAST(p.avg_pitches_per_inning_last_3 AS float) AS avg_pitches_per_inning_last_3,
        TRY_CAST(p.avg_pitches_per_inning_last_5 AS float) AS avg_pitches_per_inning_last_5,
        TRY_CAST(p.avg_pitches_per_inning_last_10 AS float) AS avg_pitches_per_inning_last_10,

        TRY_CAST(p.avg_kbb_last_3 AS float) AS avg_kbb_last_3,
        TRY_CAST(p.avg_kbb_last_5 AS float) AS avg_kbb_last_5,
        TRY_CAST(p.avg_kbb_last_10 AS float) AS avg_kbb_last_10,

        TRY_CAST(p.avg_bb9_last_3 AS float) AS avg_bb9_last_3,
        TRY_CAST(p.avg_bb9_last_5 AS float) AS avg_bb9_last_5,
        TRY_CAST(p.avg_bb9_last_10 AS float) AS avg_bb9_last_10,

        TRY_CAST(p.avg_hits_last_3 AS float) AS avg_hits_last_3,
        TRY_CAST(p.avg_hits_last_5 AS float) AS avg_hits_last_5,
        TRY_CAST(p.avg_hits_last_10 AS float) AS avg_hits_last_10,

        TRY_CAST(p.avg_hr_last_3 AS float) AS avg_hr_last_3,
        TRY_CAST(p.avg_hr_last_5 AS float) AS avg_hr_last_5,
        TRY_CAST(p.avg_hr_last_10 AS float) AS avg_hr_last_10,

        TRY_CAST(p.avg_baa_last_3 AS float) AS avg_baa_last_3,
        TRY_CAST(p.avg_baa_last_5 AS float) AS avg_baa_last_5,
        TRY_CAST(p.avg_baa_last_10 AS float) AS avg_baa_last_10,

        TRY_CAST(p.avg_baserunners_last_3 AS float) AS avg_baserunners_last_3,
        TRY_CAST(p.avg_baserunners_last_5 AS float) AS avg_baserunners_last_5,
        TRY_CAST(p.avg_baserunners_last_10 AS float) AS avg_baserunners_last_10,

        TRY_CAST(p.avg_is_starter_last_3 AS float) AS avg_is_starter_last_3,
        TRY_CAST(p.avg_is_starter_last_5 AS float) AS avg_is_starter_last_5,
        TRY_CAST(p.avg_is_starter_last_10 AS float) AS avg_is_starter_last_10,

        TRY_CAST(p.pct_5plus_ip_last_3 AS float) AS pct_5plus_ip_last_3,
        TRY_CAST(p.pct_5plus_ip_last_5 AS float) AS pct_5plus_ip_last_5,
        TRY_CAST(p.pct_5plus_ip_last_10 AS float) AS pct_5plus_ip_last_10,

        TRY_CAST(p.pct_6plus_ip_last_3 AS float) AS pct_6plus_ip_last_3,
        TRY_CAST(p.pct_6plus_ip_last_5 AS float) AS pct_6plus_ip_last_5,
        TRY_CAST(p.pct_6plus_ip_last_10 AS float) AS pct_6plus_ip_last_10,

        TRY_CAST(p.pct_5plus_k_last_3 AS float) AS pct_5plus_k_last_3,
        TRY_CAST(p.pct_5plus_k_last_5 AS float) AS pct_5plus_k_last_5,
        TRY_CAST(p.pct_5plus_k_last_10 AS float) AS pct_5plus_k_last_10,

        TRY_CAST(p.pct_7plus_k_last_3 AS float) AS pct_7plus_k_last_3,
        TRY_CAST(p.pct_7plus_k_last_5 AS float) AS pct_7plus_k_last_5,
        TRY_CAST(p.pct_7plus_k_last_10 AS float) AS pct_7plus_k_last_10,

        /* -------------------- WEIGHTED CORE -------------------- */
        TRY_CAST(p.weighted_k_last_3 AS float) AS weighted_k_last_3,
        TRY_CAST(p.weighted_k_last_5 AS float) AS weighted_k_last_5,
        TRY_CAST(p.weighted_k_last_10 AS float) AS weighted_k_last_10,

        TRY_CAST(p.weighted_ip_last_3 AS float) AS weighted_ip_last_3,
        TRY_CAST(p.weighted_ip_last_5 AS float) AS weighted_ip_last_5,
        TRY_CAST(p.weighted_ip_last_10 AS float) AS weighted_ip_last_10,

        TRY_CAST(p.weighted_bf_last_3 AS float) AS weighted_bf_last_3,
        TRY_CAST(p.weighted_bf_last_5 AS float) AS weighted_bf_last_5,
        TRY_CAST(p.weighted_bf_last_10 AS float) AS weighted_bf_last_10,

        TRY_CAST(p.weighted_pitches_last_3 AS float) AS weighted_pitches_last_3,
        TRY_CAST(p.weighted_pitches_last_5 AS float) AS weighted_pitches_last_5,
        TRY_CAST(p.weighted_pitches_last_10 AS float) AS weighted_pitches_last_10,

        TRY_CAST(p.weighted_strike_pct_last_3 AS float) AS weighted_strike_pct_last_3,
        TRY_CAST(p.weighted_strike_pct_last_5 AS float) AS weighted_strike_pct_last_5,
        TRY_CAST(p.weighted_strike_pct_last_10 AS float) AS weighted_strike_pct_last_10,

        TRY_CAST(p.weighted_k9_last_3 AS float) AS weighted_k9_last_3,
        TRY_CAST(p.weighted_k9_last_5 AS float) AS weighted_k9_last_5,
        TRY_CAST(p.weighted_k9_last_10 AS float) AS weighted_k9_last_10,

        TRY_CAST(p.weighted_bb_last_3 AS float) AS weighted_bb_last_3,
        TRY_CAST(p.weighted_bb_last_5 AS float) AS weighted_bb_last_5,
        TRY_CAST(p.weighted_bb_last_10 AS float) AS weighted_bb_last_10,

        TRY_CAST(p.weighted_whip_last_3 AS float) AS weighted_whip_last_3,
        TRY_CAST(p.weighted_whip_last_5 AS float) AS weighted_whip_last_5,
        TRY_CAST(p.weighted_whip_last_10 AS float) AS weighted_whip_last_10,

        TRY_CAST(p.weighted_outs_last_3 AS float) AS weighted_outs_last_3,
        TRY_CAST(p.weighted_outs_last_5 AS float) AS weighted_outs_last_5,
        TRY_CAST(p.weighted_outs_last_10 AS float) AS weighted_outs_last_10,

        /* -------------------- STATCAST -------------------- */
        TRY_CAST(p.avg_sc_pitches_last_3 AS float) AS avg_sc_pitches_last_3,
        TRY_CAST(p.avg_sc_pitches_last_5 AS float) AS avg_sc_pitches_last_5,
        TRY_CAST(p.avg_sc_pitches_last_10 AS float) AS avg_sc_pitches_last_10,

        TRY_CAST(p.avg_whiff_rate_last_3 AS float) AS avg_whiff_rate_last_3,
        TRY_CAST(p.avg_whiff_rate_last_5 AS float) AS avg_whiff_rate_last_5,
        TRY_CAST(p.avg_whiff_rate_last_10 AS float) AS avg_whiff_rate_last_10,

        TRY_CAST(p.avg_called_strike_rate_last_3 AS float) AS avg_called_strike_rate_last_3,
        TRY_CAST(p.avg_called_strike_rate_last_5 AS float) AS avg_called_strike_rate_last_5,
        TRY_CAST(p.avg_called_strike_rate_last_10 AS float) AS avg_called_strike_rate_last_10,

        TRY_CAST(p.avg_csw_rate_last_3 AS float) AS avg_csw_rate_last_3,
        TRY_CAST(p.avg_csw_rate_last_5 AS float) AS avg_csw_rate_last_5,
        TRY_CAST(p.avg_csw_rate_last_10 AS float) AS avg_csw_rate_last_10,

        TRY_CAST(p.avg_sc_strike_rate_last_3 AS float) AS avg_sc_strike_rate_last_3,
        TRY_CAST(p.avg_sc_strike_rate_last_5 AS float) AS avg_sc_strike_rate_last_5,
        TRY_CAST(p.avg_sc_strike_rate_last_10 AS float) AS avg_sc_strike_rate_last_10,

        TRY_CAST(p.avg_fps_rate_last_3 AS float) AS avg_fps_rate_last_3,
        TRY_CAST(p.avg_fps_rate_last_5 AS float) AS avg_fps_rate_last_5,
        TRY_CAST(p.avg_fps_rate_last_10 AS float) AS avg_fps_rate_last_10,

        TRY_CAST(p.avg_putaway_rate_last_3 AS float) AS avg_putaway_rate_last_3,
        TRY_CAST(p.avg_putaway_rate_last_5 AS float) AS avg_putaway_rate_last_5,
        TRY_CAST(p.avg_putaway_rate_last_10 AS float) AS avg_putaway_rate_last_10,

        TRY_CAST(p.avg_swing_rate_last_3 AS float) AS avg_swing_rate_last_3,
        TRY_CAST(p.avg_swing_rate_last_5 AS float) AS avg_swing_rate_last_5,
        TRY_CAST(p.avg_swing_rate_last_10 AS float) AS avg_swing_rate_last_10,

        TRY_CAST(p.avg_chase_rate_last_3 AS float) AS avg_chase_rate_last_3,
        TRY_CAST(p.avg_chase_rate_last_5 AS float) AS avg_chase_rate_last_5,
        TRY_CAST(p.avg_chase_rate_last_10 AS float) AS avg_chase_rate_last_10,

        TRY_CAST(p.avg_zone_rate_last_3 AS float) AS avg_zone_rate_last_3,
        TRY_CAST(p.avg_zone_rate_last_5 AS float) AS avg_zone_rate_last_5,
        TRY_CAST(p.avg_zone_rate_last_10 AS float) AS avg_zone_rate_last_10,

        TRY_CAST(p.avg_velocity_last_3 AS float) AS avg_velocity_last_3,
        TRY_CAST(p.avg_velocity_last_5 AS float) AS avg_velocity_last_5,
        TRY_CAST(p.avg_velocity_last_10 AS float) AS avg_velocity_last_10,

        TRY_CAST(p.avg_max_velocity_last_3 AS float) AS avg_max_velocity_last_3,
        TRY_CAST(p.avg_max_velocity_last_5 AS float) AS avg_max_velocity_last_5,
        TRY_CAST(p.avg_max_velocity_last_10 AS float) AS avg_max_velocity_last_10,

        TRY_CAST(p.avg_spin_rate_last_3 AS float) AS avg_spin_rate_last_3,
        TRY_CAST(p.avg_spin_rate_last_5 AS float) AS avg_spin_rate_last_5,
        TRY_CAST(p.avg_spin_rate_last_10 AS float) AS avg_spin_rate_last_10,

        TRY_CAST(p.avg_extension_last_3 AS float) AS avg_extension_last_3,
        TRY_CAST(p.avg_extension_last_5 AS float) AS avg_extension_last_5,
        TRY_CAST(p.avg_extension_last_10 AS float) AS avg_extension_last_10,

        TRY_CAST(p.avg_ev_allowed_last_3 AS float) AS avg_ev_allowed_last_3,
        TRY_CAST(p.avg_ev_allowed_last_5 AS float) AS avg_ev_allowed_last_5,
        TRY_CAST(p.avg_ev_allowed_last_10 AS float) AS avg_ev_allowed_last_10,

        TRY_CAST(p.avg_whiff_vs_rhb_last_3 AS float) AS avg_whiff_vs_rhb_last_3,
        TRY_CAST(p.avg_whiff_vs_rhb_last_5 AS float) AS avg_whiff_vs_rhb_last_5,
        TRY_CAST(p.avg_whiff_vs_rhb_last_10 AS float) AS avg_whiff_vs_rhb_last_10,

        TRY_CAST(p.avg_whiff_vs_lhb_last_3 AS float) AS avg_whiff_vs_lhb_last_3,
        TRY_CAST(p.avg_whiff_vs_lhb_last_5 AS float) AS avg_whiff_vs_lhb_last_5,
        TRY_CAST(p.avg_whiff_vs_lhb_last_10 AS float) AS avg_whiff_vs_lhb_last_10,

        TRY_CAST(p.weighted_sc_pitches_last_3 AS float) AS weighted_sc_pitches_last_3,
        TRY_CAST(p.weighted_sc_pitches_last_5 AS float) AS weighted_sc_pitches_last_5,
        TRY_CAST(p.weighted_sc_pitches_last_10 AS float) AS weighted_sc_pitches_last_10,

        TRY_CAST(p.weighted_whiff_rate_last_3 AS float) AS weighted_whiff_rate_last_3,
        TRY_CAST(p.weighted_whiff_rate_last_5 AS float) AS weighted_whiff_rate_last_5,
        TRY_CAST(p.weighted_whiff_rate_last_10 AS float) AS weighted_whiff_rate_last_10,

        TRY_CAST(p.weighted_csw_rate_last_3 AS float) AS weighted_csw_rate_last_3,
        TRY_CAST(p.weighted_csw_rate_last_5 AS float) AS weighted_csw_rate_last_5,
        TRY_CAST(p.weighted_csw_rate_last_10 AS float) AS weighted_csw_rate_last_10,

        TRY_CAST(p.weighted_sc_strike_rate_last_3 AS float) AS weighted_sc_strike_rate_last_3,
        TRY_CAST(p.weighted_sc_strike_rate_last_5 AS float) AS weighted_sc_strike_rate_last_5,
        TRY_CAST(p.weighted_sc_strike_rate_last_10 AS float) AS weighted_sc_strike_rate_last_10,

        TRY_CAST(p.weighted_velocity_last_3 AS float) AS weighted_velocity_last_3,
        TRY_CAST(p.weighted_velocity_last_5 AS float) AS weighted_velocity_last_5,
        TRY_CAST(p.weighted_velocity_last_10 AS float) AS weighted_velocity_last_10,

        TRY_CAST(p.weighted_spin_rate_last_3 AS float) AS weighted_spin_rate_last_3,
        TRY_CAST(p.weighted_spin_rate_last_5 AS float) AS weighted_spin_rate_last_5,
        TRY_CAST(p.weighted_spin_rate_last_10 AS float) AS weighted_spin_rate_last_10,

        TRY_CAST(p.weighted_chase_rate_last_3 AS float) AS weighted_chase_rate_last_3,
        TRY_CAST(p.weighted_chase_rate_last_5 AS float) AS weighted_chase_rate_last_5,
        TRY_CAST(p.weighted_chase_rate_last_10 AS float) AS weighted_chase_rate_last_10,

        TRY_CAST(p.weighted_putaway_rate_last_3 AS float) AS weighted_putaway_rate_last_3,
        TRY_CAST(p.weighted_putaway_rate_last_5 AS float) AS weighted_putaway_rate_last_5,
        TRY_CAST(p.weighted_putaway_rate_last_10 AS float) AS weighted_putaway_rate_last_10,

        /* -------------------- PREV GAME -------------------- */
        TRY_CAST(p.prev_k AS float) AS prev_k,
        TRY_CAST(p.prev_ip AS float) AS prev_ip,
        TRY_CAST(p.prev_bf AS float) AS prev_bf,
        TRY_CAST(p.prev_pitches AS float) AS prev_pitches,
        TRY_CAST(p.prev_k9 AS float) AS prev_k9,
        TRY_CAST(p.prev_outs AS float) AS prev_outs,
        TRY_CAST(p.prev_strike_pct AS float) AS prev_strike_pct,
        TRY_CAST(p.prev_bb AS float) AS prev_bb,
        TRY_CAST(p.prev_whip AS float) AS prev_whip,
        TRY_CAST(p.prev_hits AS float) AS prev_hits,
        TRY_CAST(p.prev_hr AS float) AS prev_hr,
        TRY_CAST(p.prev_games_started AS float) AS prev_games_started,
        TRY_CAST(p.prev_whiff_rate AS float) AS prev_whiff_rate,
        TRY_CAST(p.prev_csw_rate AS float) AS prev_csw_rate,
        TRY_CAST(p.prev_velocity AS float) AS prev_velocity,
        TRY_CAST(p.prev_spin_rate AS float) AS prev_spin_rate,
        TRY_CAST(p.prev_chase_rate AS float) AS prev_chase_rate,
        TRY_CAST(p.prev_zone_rate AS float) AS prev_zone_rate,
        TRY_CAST(p.prev_sl_whiff_rate AS float) AS prev_sl_whiff_rate,
        TRY_CAST(p.prev_ff_whiff_rate AS float) AS prev_ff_whiff_rate

    FROM mlb.dbo.fact_pitcher_model_features p
),

joined AS (
    SELECT
        p.*,

        l.side AS opp_lineup_side,
        TRY_CAST(l.team_id AS int) AS opp_team_id,
        l.team_name AS opp_team_name,

        TRY_CAST(l.lineup_spots AS float) AS opp_lineup_spots,
        TRY_CAST(l.matched_hitter_feature_rows AS float) AS opp_matched_hitter_feature_rows,

        TRY_CAST(l.lineup_avg_k_last_3 AS float) AS opp_lineup_avg_k_last_3,
        TRY_CAST(l.lineup_avg_k_last_5 AS float) AS opp_lineup_avg_k_last_5,
        TRY_CAST(l.lineup_avg_k_last_10 AS float) AS opp_lineup_avg_k_last_10,

        TRY_CAST(l.lineup_wavg_k_last_3 AS float) AS opp_lineup_wavg_k_last_3,
        TRY_CAST(l.lineup_wavg_k_last_5 AS float) AS opp_lineup_wavg_k_last_5,
        TRY_CAST(l.lineup_wavg_k_last_10 AS float) AS opp_lineup_wavg_k_last_10,

        TRY_CAST(l.lineup_avg_pa_last_3 AS float) AS opp_lineup_avg_pa_last_3,
        TRY_CAST(l.lineup_avg_pa_last_5 AS float) AS opp_lineup_avg_pa_last_5,
        TRY_CAST(l.lineup_avg_pa_last_10 AS float) AS opp_lineup_avg_pa_last_10,

        TRY_CAST(l.lineup_avg_hits_last_3 AS float) AS opp_lineup_avg_hits_last_3,
        TRY_CAST(l.lineup_avg_hits_last_5 AS float) AS opp_lineup_avg_hits_last_5,
        TRY_CAST(l.lineup_avg_hits_last_10 AS float) AS opp_lineup_avg_hits_last_10,

        TRY_CAST(l.lineup_avg_hr_last_3 AS float) AS opp_lineup_avg_hr_last_3,
        TRY_CAST(l.lineup_avg_hr_last_5 AS float) AS opp_lineup_avg_hr_last_5,
        TRY_CAST(l.lineup_avg_hr_last_10 AS float) AS opp_lineup_avg_hr_last_10,

        TRY_CAST(l.lineup_avg_obp_last_3 AS float) AS opp_lineup_avg_obp_last_3,
        TRY_CAST(l.lineup_avg_obp_last_5 AS float) AS opp_lineup_avg_obp_last_5,
        TRY_CAST(l.lineup_avg_obp_last_10 AS float) AS opp_lineup_avg_obp_last_10,

        TRY_CAST(l.lineup_avg_slg_last_3 AS float) AS opp_lineup_avg_slg_last_3,
        TRY_CAST(l.lineup_avg_slg_last_5 AS float) AS opp_lineup_avg_slg_last_5,
        TRY_CAST(l.lineup_avg_slg_last_10 AS float) AS opp_lineup_avg_slg_last_10,

        TRY_CAST(l.lineup_avg_ops_last_3 AS float) AS opp_lineup_avg_ops_last_3,
        TRY_CAST(l.lineup_avg_ops_last_5 AS float) AS opp_lineup_avg_ops_last_5,
        TRY_CAST(l.lineup_avg_ops_last_10 AS float) AS opp_lineup_avg_ops_last_10,

        TRY_CAST(l.lineup_wavg_ops_last_3 AS float) AS opp_lineup_wavg_ops_last_3,
        TRY_CAST(l.lineup_wavg_ops_last_5 AS float) AS opp_lineup_wavg_ops_last_5,
        TRY_CAST(l.lineup_wavg_ops_last_10 AS float) AS opp_lineup_wavg_ops_last_10,

        TRY_CAST(l.lineup_avg_whiff_rate_last_3 AS float) AS opp_lineup_avg_whiff_rate_last_3,
        TRY_CAST(l.lineup_avg_whiff_rate_last_5 AS float) AS opp_lineup_avg_whiff_rate_last_5,
        TRY_CAST(l.lineup_avg_whiff_rate_last_10 AS float) AS opp_lineup_avg_whiff_rate_last_10,

        TRY_CAST(l.lineup_wavg_whiff_rate_last_3 AS float) AS opp_lineup_wavg_whiff_rate_last_3,
        TRY_CAST(l.lineup_wavg_whiff_rate_last_5 AS float) AS opp_lineup_wavg_whiff_rate_last_5,
        TRY_CAST(l.lineup_wavg_whiff_rate_last_10 AS float) AS opp_lineup_wavg_whiff_rate_last_10,

        TRY_CAST(l.lineup_avg_contact_rate_last_3 AS float) AS opp_lineup_avg_contact_rate_last_3,
        TRY_CAST(l.lineup_avg_contact_rate_last_5 AS float) AS opp_lineup_avg_contact_rate_last_5,
        TRY_CAST(l.lineup_avg_contact_rate_last_10 AS float) AS opp_lineup_avg_contact_rate_last_10,

        TRY_CAST(l.lineup_avg_chase_rate_last_3 AS float) AS opp_lineup_avg_chase_rate_last_3,
        TRY_CAST(l.lineup_avg_chase_rate_last_5 AS float) AS opp_lineup_avg_chase_rate_last_5,
        TRY_CAST(l.lineup_avg_chase_rate_last_10 AS float) AS opp_lineup_avg_chase_rate_last_10,

        TRY_CAST(l.lineup_avg_csw_against_rate_last_3 AS float) AS opp_lineup_avg_csw_against_rate_last_3,
        TRY_CAST(l.lineup_avg_csw_against_rate_last_5 AS float) AS opp_lineup_avg_csw_against_rate_last_5,
        TRY_CAST(l.lineup_avg_csw_against_rate_last_10 AS float) AS opp_lineup_avg_csw_against_rate_last_10,

        TRY_CAST(l.lineup_wavg_csw_against_rate_last_3 AS float) AS opp_lineup_wavg_csw_against_rate_last_3,
        TRY_CAST(l.lineup_wavg_csw_against_rate_last_5 AS float) AS opp_lineup_wavg_csw_against_rate_last_5,
        TRY_CAST(l.lineup_wavg_csw_against_rate_last_10 AS float) AS opp_lineup_wavg_csw_against_rate_last_10,

        TRY_CAST(l.lineup_avg_xwoba_last_3 AS float) AS opp_lineup_avg_xwoba_last_3,
        TRY_CAST(l.lineup_avg_xwoba_last_5 AS float) AS opp_lineup_avg_xwoba_last_5,
        TRY_CAST(l.lineup_avg_xwoba_last_10 AS float) AS opp_lineup_avg_xwoba_last_10,

        TRY_CAST(l.lineup_wavg_xwoba_last_3 AS float) AS opp_lineup_wavg_xwoba_last_3,
        TRY_CAST(l.lineup_wavg_xwoba_last_5 AS float) AS opp_lineup_wavg_xwoba_last_5,
        TRY_CAST(l.lineup_wavg_xwoba_last_10 AS float) AS opp_lineup_wavg_xwoba_last_10,

        TRY_CAST(l.lineup_num_high_k_hitters AS float) AS opp_lineup_num_high_k_hitters,
        TRY_CAST(l.lineup_num_power_hitters AS float) AS opp_lineup_num_power_hitters,
        TRY_CAST(l.lineup_num_high_whiff_hitters AS float) AS opp_lineup_num_high_whiff_hitters,
        TRY_CAST(l.lineup_num_strong_bats AS float) AS opp_lineup_num_strong_bats

    FROM pitcher_base p
    LEFT JOIN mlb.dbo.fact_lineup_agg_features l
        ON p.gamePk = l.gamePk
       AND p.season = l.season
       AND p.pitcher_team_id <> l.team_id
)

SELECT *
INTO mlb.dbo.fact_pitcher_vs_lineup_features
FROM joined;
    """

    execute_sql(sql)
    logger.info("Finished building mlb.dbo.fact_pitcher_vs_lineup_features")


def run_all_pitcher_vs_lineup_features() -> None:
    logger.info("Starting pitcher vs lineup feature pipeline")
    build_fact_pitcher_vs_lineup_features()
    logger.info("Finished pitcher vs lineup feature pipeline")


if __name__ == "__main__":
    run_all_pitcher_vs_lineup_features()