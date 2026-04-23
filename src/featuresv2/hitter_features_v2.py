"""
Hitter feature pipeline for MLB strikeout prediction.

Updates applied vs. prior version:
  1. ROW_NUMBER partitioning changed from (player_id, season) -> (player_id).
     Rolling windows now carry across seasons, which:
       - Eliminates NULLs for the first ~10 games of each player's season.
       - Makes `days_since_last_game` meaningful across offseasons.
       - Preserves late-prior-season form as a prior for early-current-season
         games. The rolling 10-game window naturally decays stale info within
         ~3 weeks.
  2. Platoon-split weighted features (weighted_whiff_rate_vs_rhp_last_N and
     weighted_whiff_rate_vs_lhp_last_N) are now weighted by pitches_vs_rhp /
     pitches_vs_lhp respectively, not by total_pitches_seen.
  3. fact_hitter_statcast_game_agg now passes through pitches_vs_rhp /
     pitches_vs_lhp so downstream rolling features can use them for weights.

Pipeline order (unchanged):
  fact_hitter_statcast_game_agg
    -> fact_hitter_statcast_rolling_features
  fact_player_hitting_gamelogs (already loaded from MLB Stats API)
    -> fact_hitter_rolling_features
  fact_hitter_rolling_features + fact_hitter_statcast_rolling_features
    -> fact_hitter_model_features
"""

import logging
from sql.sql_loader import execute_sql

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


def build_hitter_statcast_game_agg() -> None:
    logger.info("Building mlb.dbo.fact_hitter_statcast_game_aggv2")

    sql = """
IF OBJECT_ID('mlb.dbo.fact_hitter_statcast_game_aggv2', 'U') IS NOT NULL
    DROP TABLE mlb.dbo.fact_hitter_statcast_game_aggv2;

WITH statcast_base AS (
    SELECT
        game_pk,
        CAST(game_date AS date) AS game_date,
        TRY_CAST(game_year AS int) AS season,
        TRY_CAST(player_id AS int) AS player_id,
        TRY_CAST(batter AS int) AS batter_id,
        player_name,
        TRY_CAST(team_id AS int) AS team_id,

        stand,
        p_throws,
        pitch_type,
        pitch_name,
        events,
        description,
        type,
        bb_type,
        balls,
        strikes,
        at_bat_number,
        pitch_number,
        zone,

        TRY_CAST(release_speed AS float) AS release_speed,
        TRY_CAST(release_spin_rate AS float) AS release_spin_rate,
        TRY_CAST(release_extension AS float) AS release_extension,

        TRY_CAST(pfx_x AS float) AS pfx_x,
        TRY_CAST(pfx_z AS float) AS pfx_z,
        TRY_CAST(plate_x AS float) AS plate_x,
        TRY_CAST(plate_z AS float) AS plate_z,

        TRY_CAST(launch_speed AS float) AS launch_speed,
        TRY_CAST(launch_angle AS float) AS launch_angle,
        TRY_CAST(hit_distance_sc AS float) AS hit_distance_sc,

        TRY_CAST(estimated_ba_using_speedangle AS float) AS estimated_ba_using_speedangle,
        TRY_CAST(estimated_woba_using_speedangle AS float) AS estimated_woba_using_speedangle,
        TRY_CAST(woba_value AS float) AS woba_value,
        TRY_CAST(babip_value AS float) AS babip_value,
        TRY_CAST(iso_value AS float) AS iso_value,

        TRY_CAST(bat_speed AS float) AS bat_speed,
        TRY_CAST(swing_length AS float) AS swing_length

    FROM mlb.dbo.fact_player_hit_statcast
    WHERE player_id IS NOT NULL
      AND game_pk IS NOT NULL
),

agg AS (
    SELECT
        game_pk,
        game_date,
        season,
        player_id,
        MAX(player_name) AS player_name,
        MAX(team_id) AS team_id,
        MAX(stand) AS stand,

        COUNT(*) AS total_pitches_seen,

        -- swings
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
            END) AS swings,

        -- whiffs
        SUM(CASE
                WHEN description IN ('swinging_strike', 'swinging_strike_blocked')
                THEN 1 ELSE 0
            END) AS whiffs,

        -- called strikes
        SUM(CASE
                WHEN description = 'called_strike'
                THEN 1 ELSE 0
            END) AS called_strikes,

        -- balls in play
        SUM(CASE
                WHEN description IN ('hit_into_play', 'hit_into_play_no_out', 'hit_into_play_score')
                THEN 1 ELSE 0
            END) AS balls_in_play,

        -- chase swings (swings outside rough zone)
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
                 AND (
                     plate_x NOT BETWEEN -0.83 AND 0.83
                     OR plate_z NOT BETWEEN 1.5 AND 3.5
                 )
                THEN 1 ELSE 0
            END) AS chase_swings,

        -- pitches in zone
        SUM(CASE
                WHEN plate_x BETWEEN -0.83 AND 0.83
                 AND plate_z BETWEEN 1.5 AND 3.5
                THEN 1 ELSE 0
            END) AS pitches_in_zone,

        -- swings in zone
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
                 AND plate_x BETWEEN -0.83 AND 0.83
                 AND plate_z BETWEEN 1.5 AND 3.5
                THEN 1 ELSE 0
            END) AS swings_in_zone,

        -- contact
        SUM(CASE
                WHEN description IN (
                    'foul',
                    'foul_tip',
                    'hit_into_play',
                    'hit_into_play_no_out',
                    'hit_into_play_score'
                )
                THEN 1 ELSE 0
            END) AS contacts,

        -- two-strike context
        SUM(CASE
                WHEN strikes = 2 THEN 1 ELSE 0
            END) AS two_strike_pitches_seen,

        SUM(CASE
                WHEN strikes = 2
                 AND description IN ('swinging_strike', 'swinging_strike_blocked')
                THEN 1 ELSE 0
            END) AS two_strike_whiffs,

        -- count context
        SUM(CASE WHEN balls = 0 AND strikes = 2 THEN 1 ELSE 0 END) AS pitches_seen_0_2,
        SUM(CASE WHEN balls = 1 AND strikes = 2 THEN 1 ELSE 0 END) AS pitches_seen_1_2,
        SUM(CASE WHEN balls = 2 AND strikes = 2 THEN 1 ELSE 0 END) AS pitches_seen_2_2,

        SUM(CASE
                WHEN balls = 0 AND strikes = 2
                 AND description IN ('swinging_strike', 'swinging_strike_blocked')
                THEN 1 ELSE 0
            END) AS whiffs_0_2,

        SUM(CASE
                WHEN balls = 1 AND strikes = 2
                 AND description IN ('swinging_strike', 'swinging_strike_blocked')
                THEN 1 ELSE 0
            END) AS whiffs_1_2,

        SUM(CASE
                WHEN balls = 2 AND strikes = 2
                 AND description IN ('swinging_strike', 'swinging_strike_blocked')
                THEN 1 ELSE 0
            END) AS whiffs_2_2,

        -- event results
        SUM(CASE WHEN events = 'strikeout' THEN 1 ELSE 0 END) AS strikeouts,
        SUM(CASE WHEN events = 'walk' THEN 1 ELSE 0 END) AS walks,
        SUM(CASE WHEN events = 'single' THEN 1 ELSE 0 END) AS singles,
        SUM(CASE WHEN events = 'double' THEN 1 ELSE 0 END) AS doubles,
        SUM(CASE WHEN events = 'triple' THEN 1 ELSE 0 END) AS triples,
        SUM(CASE WHEN events = 'home_run' THEN 1 ELSE 0 END) AS home_runs,

        -- quality of contact
        AVG(launch_speed) AS avg_exit_velocity,
        MAX(launch_speed) AS max_exit_velocity,
        AVG(launch_angle) AS avg_launch_angle,
        AVG(hit_distance_sc) AS avg_hit_distance,

        AVG(estimated_ba_using_speedangle) AS avg_xba,
        AVG(estimated_woba_using_speedangle) AS avg_xwoba,
        AVG(woba_value) AS avg_woba_value,
        AVG(babip_value) AS avg_babip_value,
        AVG(iso_value) AS avg_iso_value,

        AVG(bat_speed) AS avg_bat_speed,
        AVG(swing_length) AS avg_swing_length,

        -- pitch characteristics seen
        AVG(release_speed) AS avg_pitch_velocity_seen,
        AVG(release_spin_rate) AS avg_pitch_spin_seen,
        AVG(release_extension) AS avg_pitch_extension_seen,
        AVG(pfx_x) AS avg_horz_movement_seen,
        AVG(pfx_z) AS avg_vert_movement_seen,
        AVG(plate_x) AS avg_plate_x_seen,
        AVG(plate_z) AS avg_plate_z_seen,

        -- pitch mix seen
        SUM(CASE WHEN pitch_type = 'FF' THEN 1 ELSE 0 END) AS ff_seen,
        SUM(CASE WHEN pitch_type = 'SI' THEN 1 ELSE 0 END) AS si_seen,
        SUM(CASE WHEN pitch_type = 'FC' THEN 1 ELSE 0 END) AS fc_seen,
        SUM(CASE WHEN pitch_type = 'SL' THEN 1 ELSE 0 END) AS sl_seen,
        SUM(CASE WHEN pitch_type = 'CU' THEN 1 ELSE 0 END) AS cu_seen,
        SUM(CASE WHEN pitch_type = 'CH' THEN 1 ELSE 0 END) AS ch_seen,
        SUM(CASE WHEN pitch_type = 'FS' THEN 1 ELSE 0 END) AS fs_seen,

        -- split by pitcher handedness
        SUM(CASE WHEN p_throws = 'R' THEN 1 ELSE 0 END) AS pitches_vs_rhp,
        SUM(CASE WHEN p_throws = 'L' THEN 1 ELSE 0 END) AS pitches_vs_lhp,

        SUM(CASE
                WHEN p_throws = 'R'
                 AND description IN ('swinging_strike', 'swinging_strike_blocked')
                THEN 1 ELSE 0
            END) AS whiffs_vs_rhp,

        SUM(CASE
                WHEN p_throws = 'L'
                 AND description IN ('swinging_strike', 'swinging_strike_blocked')
                THEN 1 ELSE 0
            END) AS whiffs_vs_lhp

    FROM statcast_base
    GROUP BY
        game_pk,
        game_date,
        season,
        player_id
)

SELECT
    game_pk,
    game_date,
    season,
    player_id,
    player_name,
    team_id,
    stand,

    total_pitches_seen,
    swings,
    whiffs,
    called_strikes,
    balls_in_play,
    contacts,
    chase_swings,
    pitches_in_zone,
    swings_in_zone,
    two_strike_pitches_seen,
    two_strike_whiffs,

    strikeouts,
    walks,
    singles,
    doubles,
    triples,
    home_runs,

    whiffs * 1.0 / NULLIF(swings, 0) AS whiff_rate,
    contacts * 1.0 / NULLIF(swings, 0) AS contact_rate,
    swings * 1.0 / NULLIF(total_pitches_seen, 0) AS swing_rate,
    chase_swings * 1.0 / NULLIF(swings, 0) AS chase_rate,
    swings_in_zone * 1.0 / NULLIF(pitches_in_zone, 0) AS zone_swing_rate,
    pitches_in_zone * 1.0 / NULLIF(total_pitches_seen, 0) AS zone_rate,
    called_strikes * 1.0 / NULLIF(total_pitches_seen, 0) AS called_strike_rate,
    (whiffs + called_strikes) * 1.0 / NULLIF(total_pitches_seen, 0) AS csw_against_rate,
    two_strike_whiffs * 1.0 / NULLIF(two_strike_pitches_seen, 0) AS two_strike_whiff_rate,

    pitches_seen_0_2,
    pitches_seen_1_2,
    pitches_seen_2_2,
    whiffs_0_2,
    whiffs_1_2,
    whiffs_2_2,
    whiffs_0_2 * 1.0 / NULLIF(pitches_seen_0_2, 0) AS whiff_rate_0_2,
    whiffs_1_2 * 1.0 / NULLIF(pitches_seen_1_2, 0) AS whiff_rate_1_2,
    whiffs_2_2 * 1.0 / NULLIF(pitches_seen_2_2, 0) AS whiff_rate_2_2,

    avg_exit_velocity,
    max_exit_velocity,
    avg_launch_angle,
    avg_hit_distance,
    avg_xba,
    avg_xwoba,
    avg_woba_value,
    avg_babip_value,
    avg_iso_value,
    avg_bat_speed,
    avg_swing_length,

    avg_pitch_velocity_seen,
    avg_pitch_spin_seen,
    avg_pitch_extension_seen,
    avg_horz_movement_seen,
    avg_vert_movement_seen,
    avg_plate_x_seen,
    avg_plate_z_seen,

    ff_seen,
    si_seen,
    fc_seen,
    sl_seen,
    cu_seen,
    ch_seen,
    fs_seen,

    ff_seen * 1.0 / NULLIF(total_pitches_seen, 0) AS ff_seen_pct,
    si_seen * 1.0 / NULLIF(total_pitches_seen, 0) AS si_seen_pct,
    fc_seen * 1.0 / NULLIF(total_pitches_seen, 0) AS fc_seen_pct,
    sl_seen * 1.0 / NULLIF(total_pitches_seen, 0) AS sl_seen_pct,
    cu_seen * 1.0 / NULLIF(total_pitches_seen, 0) AS cu_seen_pct,
    ch_seen * 1.0 / NULLIF(total_pitches_seen, 0) AS ch_seen_pct,
    fs_seen * 1.0 / NULLIF(total_pitches_seen, 0) AS fs_seen_pct,

    pitches_vs_rhp,
    pitches_vs_lhp,
    whiffs_vs_rhp,
    whiffs_vs_lhp,
    whiffs_vs_rhp * 1.0 / NULLIF(pitches_vs_rhp, 0) AS whiff_rate_vs_rhp,
    whiffs_vs_lhp * 1.0 / NULLIF(pitches_vs_lhp, 0) AS whiff_rate_vs_lhp

INTO mlb.dbo.fact_hitter_statcast_game_aggv2
FROM agg;
    """
    execute_sql(sql)
    logger.info("Finished building mlb.dbo.fact_hitter_statcast_game_aggv2")


def build_hitter_statcast_rolling_features() -> None:
    logger.info("Building mlb.dbo.fact_hitter_statcast_rolling_featuresv2")

    sql = """
IF OBJECT_ID('mlb.dbo.fact_hitter_statcast_rolling_featuresv2', 'U') IS NOT NULL
    DROP TABLE mlb.dbo.fact_hitter_statcast_rolling_featuresv2;

WITH base AS (
    SELECT
        game_pk,
        CAST(game_date AS date) AS game_date,
        TRY_CAST(season AS int) AS season,
        TRY_CAST(player_id AS int) AS player_id,
        player_name,
        TRY_CAST(team_id AS int) AS team_id,

        TRY_CAST(total_pitches_seen AS float) AS total_pitches_seen,
        TRY_CAST(swings AS float) AS swings,
        TRY_CAST(whiffs AS float) AS whiffs,
        TRY_CAST(called_strikes AS float) AS called_strikes,
        TRY_CAST(balls_in_play AS float) AS balls_in_play,
        TRY_CAST(contacts AS float) AS contacts,

        TRY_CAST(strikeouts AS float) AS strikeouts,
        TRY_CAST(walks AS float) AS walks,
        TRY_CAST(singles AS float) AS singles,
        TRY_CAST(doubles AS float) AS doubles,
        TRY_CAST(triples AS float) AS triples,
        TRY_CAST(home_runs AS float) AS home_runs,

        TRY_CAST(whiff_rate AS float) AS whiff_rate,
        TRY_CAST(contact_rate AS float) AS contact_rate,
        TRY_CAST(swing_rate AS float) AS swing_rate,
        TRY_CAST(chase_rate AS float) AS chase_rate,
        TRY_CAST(zone_swing_rate AS float) AS zone_swing_rate,
        TRY_CAST(zone_rate AS float) AS zone_rate,
        TRY_CAST(called_strike_rate AS float) AS called_strike_rate,
        TRY_CAST(csw_against_rate AS float) AS csw_against_rate,
        TRY_CAST(two_strike_whiff_rate AS float) AS two_strike_whiff_rate,

        TRY_CAST(whiff_rate_0_2 AS float) AS whiff_rate_0_2,
        TRY_CAST(whiff_rate_1_2 AS float) AS whiff_rate_1_2,
        TRY_CAST(whiff_rate_2_2 AS float) AS whiff_rate_2_2,

        TRY_CAST(avg_exit_velocity AS float) AS avg_exit_velocity,
        TRY_CAST(max_exit_velocity AS float) AS max_exit_velocity,
        TRY_CAST(avg_launch_angle AS float) AS avg_launch_angle,
        TRY_CAST(avg_hit_distance AS float) AS avg_hit_distance,
        TRY_CAST(avg_xba AS float) AS avg_xba,
        TRY_CAST(avg_xwoba AS float) AS avg_xwoba,
        TRY_CAST(avg_woba_value AS float) AS avg_woba_value,
        TRY_CAST(avg_babip_value AS float) AS avg_babip_value,
        TRY_CAST(avg_iso_value AS float) AS avg_iso_value,
        TRY_CAST(avg_bat_speed AS float) AS avg_bat_speed,
        TRY_CAST(avg_swing_length AS float) AS avg_swing_length,

        TRY_CAST(avg_pitch_velocity_seen AS float) AS avg_pitch_velocity_seen,
        TRY_CAST(avg_pitch_spin_seen AS float) AS avg_pitch_spin_seen,
        TRY_CAST(avg_pitch_extension_seen AS float) AS avg_pitch_extension_seen,
        TRY_CAST(avg_horz_movement_seen AS float) AS avg_horz_movement_seen,
        TRY_CAST(avg_vert_movement_seen AS float) AS avg_vert_movement_seen,
        TRY_CAST(avg_plate_x_seen AS float) AS avg_plate_x_seen,
        TRY_CAST(avg_plate_z_seen AS float) AS avg_plate_z_seen,

        TRY_CAST(ff_seen_pct AS float) AS ff_seen_pct,
        TRY_CAST(si_seen_pct AS float) AS si_seen_pct,
        TRY_CAST(fc_seen_pct AS float) AS fc_seen_pct,
        TRY_CAST(sl_seen_pct AS float) AS sl_seen_pct,
        TRY_CAST(cu_seen_pct AS float) AS cu_seen_pct,
        TRY_CAST(ch_seen_pct AS float) AS ch_seen_pct,
        TRY_CAST(fs_seen_pct AS float) AS fs_seen_pct,

        TRY_CAST(pitches_vs_rhp AS float) AS pitches_vs_rhp,
        TRY_CAST(pitches_vs_lhp AS float) AS pitches_vs_lhp,
        TRY_CAST(whiff_rate_vs_rhp AS float) AS whiff_rate_vs_rhp,
        TRY_CAST(whiff_rate_vs_lhp AS float) AS whiff_rate_vs_lhp

    FROM mlb.dbo.fact_hitter_statcast_game_aggv2
),

prep AS (
    SELECT
        *,
        /* Partition by player_id only (NOT by season) so rolling windows
           carry across the offseason. The 10-game max window naturally
           decays stale info within ~3 weeks of a new season. */
        ROW_NUMBER() OVER (
            PARTITION BY player_id
            ORDER BY game_date, game_pk
        ) AS rn
    FROM base
),

rolling AS (
    SELECT
        p1.game_pk,
        p1.game_date,
        p1.season,
        p1.player_id,
        p1.player_name,
        p1.team_id,

        /* -------------------- LAST 3 SIMPLE AVG -------------------- */
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.total_pitches_seen END) AS avg_sc_pitches_seen_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.whiff_rate END) AS avg_whiff_rate_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.contact_rate END) AS avg_contact_rate_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.swing_rate END) AS avg_swing_rate_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.chase_rate END) AS avg_chase_rate_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.zone_swing_rate END) AS avg_zone_swing_rate_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.zone_rate END) AS avg_zone_rate_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.called_strike_rate END) AS avg_called_strike_rate_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.csw_against_rate END) AS avg_csw_against_rate_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.two_strike_whiff_rate END) AS avg_two_strike_whiff_rate_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.whiff_rate_0_2 END) AS avg_whiff_rate_0_2_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.whiff_rate_1_2 END) AS avg_whiff_rate_1_2_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.whiff_rate_2_2 END) AS avg_whiff_rate_2_2_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.avg_exit_velocity END) AS avg_exit_velocity_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.max_exit_velocity END) AS avg_max_exit_velocity_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.avg_launch_angle END) AS avg_launch_angle_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.avg_hit_distance END) AS avg_hit_distance_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.avg_xba END) AS avg_xba_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.avg_xwoba END) AS avg_xwoba_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.avg_woba_value END) AS avg_woba_value_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.avg_babip_value END) AS avg_babip_value_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.avg_iso_value END) AS avg_iso_value_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.avg_bat_speed END) AS avg_bat_speed_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.avg_swing_length END) AS avg_swing_length_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.avg_pitch_velocity_seen END) AS avg_pitch_velocity_seen_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.avg_pitch_spin_seen END) AS avg_pitch_spin_seen_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.avg_pitch_extension_seen END) AS avg_pitch_extension_seen_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.avg_horz_movement_seen END) AS avg_horz_movement_seen_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.avg_vert_movement_seen END) AS avg_vert_movement_seen_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.avg_plate_x_seen END) AS avg_plate_x_seen_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.avg_plate_z_seen END) AS avg_plate_z_seen_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.ff_seen_pct END) AS avg_ff_seen_pct_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.si_seen_pct END) AS avg_si_seen_pct_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.fc_seen_pct END) AS avg_fc_seen_pct_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.sl_seen_pct END) AS avg_sl_seen_pct_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.cu_seen_pct END) AS avg_cu_seen_pct_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.ch_seen_pct END) AS avg_ch_seen_pct_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.fs_seen_pct END) AS avg_fs_seen_pct_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.whiff_rate_vs_rhp END) AS avg_whiff_rate_vs_rhp_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.whiff_rate_vs_lhp END) AS avg_whiff_rate_vs_lhp_last_3,

        /* -------------------- LAST 3 VOLUME-WEIGHTED AVG -------------------- */
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.whiff_rate * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_whiff_rate_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.contact_rate * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_contact_rate_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.swing_rate * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_swing_rate_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.chase_rate * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_chase_rate_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.zone_swing_rate * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_zone_swing_rate_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.zone_rate * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_zone_rate_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.called_strike_rate * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_called_strike_rate_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.csw_against_rate * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_csw_against_rate_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.two_strike_whiff_rate * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_two_strike_whiff_rate_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.whiff_rate_0_2 * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_whiff_rate_0_2_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.whiff_rate_1_2 * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_whiff_rate_1_2_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.whiff_rate_2_2 * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_whiff_rate_2_2_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.ff_seen_pct * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_ff_seen_pct_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.si_seen_pct * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_si_seen_pct_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.fc_seen_pct * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_fc_seen_pct_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.sl_seen_pct * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_sl_seen_pct_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.cu_seen_pct * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_cu_seen_pct_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.ch_seen_pct * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_ch_seen_pct_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.fs_seen_pct * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_fs_seen_pct_last_3,

        /* Platoon-split weights use handedness-specific pitch counts, not total_pitches_seen */
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.whiff_rate_vs_rhp * p2.pitches_vs_rhp END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.pitches_vs_rhp END), 0) AS weighted_whiff_rate_vs_rhp_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.whiff_rate_vs_lhp * p2.pitches_vs_lhp END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.pitches_vs_lhp END), 0) AS weighted_whiff_rate_vs_lhp_last_3,

        /* -------------------- LAST 5 SIMPLE AVG -------------------- */
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.total_pitches_seen END) AS avg_sc_pitches_seen_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.whiff_rate END) AS avg_whiff_rate_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.contact_rate END) AS avg_contact_rate_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.swing_rate END) AS avg_swing_rate_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.chase_rate END) AS avg_chase_rate_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.zone_swing_rate END) AS avg_zone_swing_rate_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.zone_rate END) AS avg_zone_rate_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.called_strike_rate END) AS avg_called_strike_rate_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.csw_against_rate END) AS avg_csw_against_rate_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.two_strike_whiff_rate END) AS avg_two_strike_whiff_rate_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.whiff_rate_0_2 END) AS avg_whiff_rate_0_2_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.whiff_rate_1_2 END) AS avg_whiff_rate_1_2_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.whiff_rate_2_2 END) AS avg_whiff_rate_2_2_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.avg_exit_velocity END) AS avg_exit_velocity_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.max_exit_velocity END) AS avg_max_exit_velocity_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.avg_launch_angle END) AS avg_launch_angle_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.avg_hit_distance END) AS avg_hit_distance_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.avg_xba END) AS avg_xba_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.avg_xwoba END) AS avg_xwoba_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.avg_woba_value END) AS avg_woba_value_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.avg_babip_value END) AS avg_babip_value_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.avg_iso_value END) AS avg_iso_value_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.avg_bat_speed END) AS avg_bat_speed_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.avg_swing_length END) AS avg_swing_length_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.avg_pitch_velocity_seen END) AS avg_pitch_velocity_seen_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.avg_pitch_spin_seen END) AS avg_pitch_spin_seen_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.avg_pitch_extension_seen END) AS avg_pitch_extension_seen_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.avg_horz_movement_seen END) AS avg_horz_movement_seen_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.avg_vert_movement_seen END) AS avg_vert_movement_seen_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.avg_plate_x_seen END) AS avg_plate_x_seen_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.avg_plate_z_seen END) AS avg_plate_z_seen_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.ff_seen_pct END) AS avg_ff_seen_pct_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.si_seen_pct END) AS avg_si_seen_pct_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.fc_seen_pct END) AS avg_fc_seen_pct_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.sl_seen_pct END) AS avg_sl_seen_pct_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.cu_seen_pct END) AS avg_cu_seen_pct_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.ch_seen_pct END) AS avg_ch_seen_pct_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.fs_seen_pct END) AS avg_fs_seen_pct_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.whiff_rate_vs_rhp END) AS avg_whiff_rate_vs_rhp_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.whiff_rate_vs_lhp END) AS avg_whiff_rate_vs_lhp_last_5,

        /* -------------------- LAST 5 VOLUME-WEIGHTED AVG -------------------- */
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.whiff_rate * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_whiff_rate_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.contact_rate * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_contact_rate_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.swing_rate * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_swing_rate_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.chase_rate * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_chase_rate_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.zone_swing_rate * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_zone_swing_rate_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.zone_rate * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_zone_rate_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.called_strike_rate * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_called_strike_rate_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.csw_against_rate * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_csw_against_rate_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.two_strike_whiff_rate * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_two_strike_whiff_rate_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.whiff_rate_0_2 * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_whiff_rate_0_2_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.whiff_rate_1_2 * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_whiff_rate_1_2_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.whiff_rate_2_2 * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_whiff_rate_2_2_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.ff_seen_pct * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_ff_seen_pct_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.si_seen_pct * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_si_seen_pct_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.fc_seen_pct * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_fc_seen_pct_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.sl_seen_pct * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_sl_seen_pct_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.cu_seen_pct * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_cu_seen_pct_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.ch_seen_pct * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_ch_seen_pct_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.fs_seen_pct * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_fs_seen_pct_last_5,

        /* Platoon-split weights use handedness-specific pitch counts */
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.whiff_rate_vs_rhp * p2.pitches_vs_rhp END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.pitches_vs_rhp END), 0) AS weighted_whiff_rate_vs_rhp_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.whiff_rate_vs_lhp * p2.pitches_vs_lhp END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.pitches_vs_lhp END), 0) AS weighted_whiff_rate_vs_lhp_last_5,

        /* -------------------- LAST 10 SIMPLE AVG -------------------- */
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.total_pitches_seen END) AS avg_sc_pitches_seen_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.whiff_rate END) AS avg_whiff_rate_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.contact_rate END) AS avg_contact_rate_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.swing_rate END) AS avg_swing_rate_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.chase_rate END) AS avg_chase_rate_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.zone_swing_rate END) AS avg_zone_swing_rate_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.zone_rate END) AS avg_zone_rate_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.called_strike_rate END) AS avg_called_strike_rate_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.csw_against_rate END) AS avg_csw_against_rate_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.two_strike_whiff_rate END) AS avg_two_strike_whiff_rate_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.whiff_rate_0_2 END) AS avg_whiff_rate_0_2_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.whiff_rate_1_2 END) AS avg_whiff_rate_1_2_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.whiff_rate_2_2 END) AS avg_whiff_rate_2_2_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.avg_exit_velocity END) AS avg_exit_velocity_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.max_exit_velocity END) AS avg_max_exit_velocity_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.avg_launch_angle END) AS avg_launch_angle_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.avg_hit_distance END) AS avg_hit_distance_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.avg_xba END) AS avg_xba_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.avg_xwoba END) AS avg_xwoba_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.avg_woba_value END) AS avg_woba_value_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.avg_babip_value END) AS avg_babip_value_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.avg_iso_value END) AS avg_iso_value_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.avg_bat_speed END) AS avg_bat_speed_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.avg_swing_length END) AS avg_swing_length_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.avg_pitch_velocity_seen END) AS avg_pitch_velocity_seen_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.avg_pitch_spin_seen END) AS avg_pitch_spin_seen_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.avg_pitch_extension_seen END) AS avg_pitch_extension_seen_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.avg_horz_movement_seen END) AS avg_horz_movement_seen_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.avg_vert_movement_seen END) AS avg_vert_movement_seen_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.avg_plate_x_seen END) AS avg_plate_x_seen_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.avg_plate_z_seen END) AS avg_plate_z_seen_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.ff_seen_pct END) AS avg_ff_seen_pct_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.si_seen_pct END) AS avg_si_seen_pct_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.fc_seen_pct END) AS avg_fc_seen_pct_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.sl_seen_pct END) AS avg_sl_seen_pct_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.cu_seen_pct END) AS avg_cu_seen_pct_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.ch_seen_pct END) AS avg_ch_seen_pct_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.fs_seen_pct END) AS avg_fs_seen_pct_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.whiff_rate_vs_rhp END) AS avg_whiff_rate_vs_rhp_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.whiff_rate_vs_lhp END) AS avg_whiff_rate_vs_lhp_last_10,

        /* -------------------- LAST 10 VOLUME-WEIGHTED AVG -------------------- */
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.whiff_rate * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_whiff_rate_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.contact_rate * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_contact_rate_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.swing_rate * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_swing_rate_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.chase_rate * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_chase_rate_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.zone_swing_rate * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_zone_swing_rate_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.zone_rate * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_zone_rate_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.called_strike_rate * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_called_strike_rate_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.csw_against_rate * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_csw_against_rate_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.two_strike_whiff_rate * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_two_strike_whiff_rate_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.whiff_rate_0_2 * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_whiff_rate_0_2_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.whiff_rate_1_2 * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_whiff_rate_1_2_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.whiff_rate_2_2 * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_whiff_rate_2_2_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.ff_seen_pct * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_ff_seen_pct_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.si_seen_pct * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_si_seen_pct_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.fc_seen_pct * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_fc_seen_pct_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.sl_seen_pct * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_sl_seen_pct_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.cu_seen_pct * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_cu_seen_pct_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.ch_seen_pct * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_ch_seen_pct_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.fs_seen_pct * p2.total_pitches_seen END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.total_pitches_seen END), 0) AS weighted_fs_seen_pct_last_10,

        /* Platoon-split weights use handedness-specific pitch counts */
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.whiff_rate_vs_rhp * p2.pitches_vs_rhp END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.pitches_vs_rhp END), 0) AS weighted_whiff_rate_vs_rhp_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.whiff_rate_vs_lhp * p2.pitches_vs_lhp END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.pitches_vs_lhp END), 0) AS weighted_whiff_rate_vs_lhp_last_10,

        /* -------------------- PREVIOUS GAME -------------------- */
        MAX(CASE WHEN p2.rn = p1.rn - 1 THEN p2.whiff_rate END) AS prev_whiff_rate,
        MAX(CASE WHEN p2.rn = p1.rn - 1 THEN p2.contact_rate END) AS prev_contact_rate,
        MAX(CASE WHEN p2.rn = p1.rn - 1 THEN p2.chase_rate END) AS prev_chase_rate,
        MAX(CASE WHEN p2.rn = p1.rn - 1 THEN p2.avg_exit_velocity END) AS prev_exit_velocity,
        MAX(CASE WHEN p2.rn = p1.rn - 1 THEN p2.avg_xwoba END) AS prev_xwoba,
        MAX(CASE WHEN p2.rn = p1.rn - 1 THEN p2.avg_bat_speed END) AS prev_bat_speed

    FROM prep p1
    LEFT JOIN prep p2
        ON p1.player_id = p2.player_id
       AND p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1
    GROUP BY
        p1.game_pk,
        p1.game_date,
        p1.season,
        p1.player_id,
        p1.player_name,
        p1.team_id,
        p1.rn
)

SELECT *
INTO mlb.dbo.fact_hitter_statcast_rolling_featuresv2
FROM rolling;
    """
    execute_sql(sql)
    logger.info("Finished building mlb.dbo.fact_hitter_statcast_rolling_featuresv2")


def build_hitter_rolling_features() -> None:
    logger.info("Building mlb.dbo.fact_hitter_rolling_featuresv2")

    sql = """
IF OBJECT_ID('mlb.dbo.fact_hitter_rolling_featuresv2', 'U') IS NOT NULL
    DROP TABLE mlb.dbo.fact_hitter_rolling_featuresv2;

WITH base AS (
    SELECT
        gamePk,
        CAST(game_date AS date) AS game_date,
        TRY_CAST(season AS int) AS season,
        TRY_CAST(player_id AS int) AS player_id,
        player_name,
        position,
        TRY_CAST(team_id AS int) AS team_id,
        team_name,

        TRY_CAST(strikeOuts AS float) AS strikeOuts,
        TRY_CAST(baseOnBalls AS float) AS baseOnBalls,
        TRY_CAST(hits AS float) AS hits,
        TRY_CAST(homeRuns AS float) AS homeRuns,
        TRY_CAST(atBats AS float) AS atBats,
        TRY_CAST(plateAppearances AS float) AS plateAppearances,
        TRY_CAST(numberOfPitches AS float) AS numberOfPitches,
        TRY_CAST(totalBases AS float) AS totalBases,
        TRY_CAST(rbi AS float) AS rbi,
        TRY_CAST(leftOnBase AS float) AS leftOnBase,
        TRY_CAST(obp AS float) AS obp,
        TRY_CAST(slg AS float) AS slg,
        TRY_CAST(ops AS float) AS ops,
        TRY_CAST(babip AS float) AS babip,
        TRY_CAST(avg AS float) AS batting_avg,
        TRY_CAST(hitByPitch AS float) AS hitByPitch,
        TRY_CAST(sacFlies AS float) AS sacFlies,
        TRY_CAST(sacBunts AS float) AS sacBunts,
        TRY_CAST(stolenBases AS float) AS stolenBases,
        TRY_CAST(caughtStealing AS float) AS caughtStealing,

        TRY_CAST(strikeOuts AS float) / NULLIF(TRY_CAST(plateAppearances AS float), 0) AS k_rate,
        TRY_CAST(baseOnBalls AS float) / NULLIF(TRY_CAST(plateAppearances AS float), 0) AS walk_rate,
        TRY_CAST(hits AS float) / NULLIF(TRY_CAST(plateAppearances AS float), 0) AS hit_rate,
        TRY_CAST(totalBases AS float) / NULLIF(TRY_CAST(plateAppearances AS float), 0) AS tb_rate,
        TRY_CAST(homeRuns AS float) / NULLIF(TRY_CAST(plateAppearances AS float), 0) AS hr_rate

    FROM mlb.dbo.fact_player_hitting_gamelogs
    WHERE player_id IS NOT NULL
      AND gamePk IS NOT NULL
),

prep AS (
    SELECT
        *,
        /* Partition by player_id only (NOT by season) so rolling windows
           carry across the offseason. */
        ROW_NUMBER() OVER (
            PARTITION BY player_id
            ORDER BY game_date, gamePk
        ) AS rn
    FROM base
),

rolling AS (
    SELECT
        p1.gamePk,
        p1.game_date,
        p1.season,
        p1.player_id,
        p1.player_name,
        p1.position,
        p1.team_id,
        p1.team_name,
        p1.strikeOuts,

        DATEDIFF(
            DAY,
            MAX(CASE WHEN p2.rn = p1.rn - 1 THEN p2.game_date END),
            p1.game_date
        ) AS days_since_last_game,

        /* -------------------- LAST 3 SIMPLE AVG -------------------- */
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.strikeOuts END) AS avg_k_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.plateAppearances END) AS avg_pa_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.atBats END) AS avg_ab_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.hits END) AS avg_hits_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.homeRuns END) AS avg_hr_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.baseOnBalls END) AS avg_bb_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.numberOfPitches END) AS avg_pitches_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.totalBases END) AS avg_tb_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.rbi END) AS avg_rbi_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.leftOnBase END) AS avg_lob_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.obp END) AS avg_obp_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.slg END) AS avg_slg_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.ops END) AS avg_ops_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.babip END) AS avg_babip_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.batting_avg END) AS avg_batting_avg_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.hitByPitch END) AS avg_hbp_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.sacFlies END) AS avg_sf_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.sacBunts END) AS avg_sbunts_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.stolenBases END) AS avg_stolen_bases_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.caughtStealing END) AS avg_caught_stealing_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.k_rate END) AS avg_k_rate_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.walk_rate END) AS avg_walk_rate_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.hit_rate END) AS avg_hit_rate_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.tb_rate END) AS avg_tb_rate_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.hr_rate END) AS avg_hr_rate_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.plateAppearances END) AS sum_pa_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.atBats END) AS sum_ab_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 AND p2.strikeOuts >= 1 THEN 1.0
                 WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN 0.0 END) AS pct_1plus_k_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 AND p2.strikeOuts >= 2 THEN 1.0
                 WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN 0.0 END) AS pct_2plus_k_last_3,

        /* -------------------- LAST 3 VOLUME-WEIGHTED AVG (by PA/AB) -------------------- */
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.strikeOuts END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.plateAppearances END), 0) AS weighted_k_rate_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.baseOnBalls END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.plateAppearances END), 0) AS weighted_walk_rate_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.hits END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.plateAppearances END), 0) AS weighted_hit_rate_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.totalBases END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.plateAppearances END), 0) AS weighted_tb_rate_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.homeRuns END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.plateAppearances END), 0) AS weighted_hr_rate_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.hits END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.atBats END), 0) AS weighted_batting_avg_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.numberOfPitches END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.plateAppearances END), 0) AS weighted_pitches_per_pa_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.obp * p2.plateAppearances END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.plateAppearances END), 0) AS weighted_obp_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.slg * p2.atBats END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.atBats END), 0) AS weighted_slg_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.ops * p2.plateAppearances END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.plateAppearances END), 0) AS weighted_ops_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.babip * p2.atBats END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.atBats END), 0) AS weighted_babip_last_3,

        /* -------------------- LAST 5 SIMPLE AVG -------------------- */
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.strikeOuts END) AS avg_k_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.plateAppearances END) AS avg_pa_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.atBats END) AS avg_ab_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.hits END) AS avg_hits_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.homeRuns END) AS avg_hr_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.baseOnBalls END) AS avg_bb_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.numberOfPitches END) AS avg_pitches_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.totalBases END) AS avg_tb_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.rbi END) AS avg_rbi_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.leftOnBase END) AS avg_lob_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.obp END) AS avg_obp_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.slg END) AS avg_slg_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.ops END) AS avg_ops_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.babip END) AS avg_babip_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.batting_avg END) AS avg_batting_avg_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.hitByPitch END) AS avg_hbp_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.sacFlies END) AS avg_sf_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.sacBunts END) AS avg_sbunts_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.stolenBases END) AS avg_stolen_bases_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.caughtStealing END) AS avg_caught_stealing_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.k_rate END) AS avg_k_rate_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.walk_rate END) AS avg_walk_rate_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.hit_rate END) AS avg_hit_rate_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.tb_rate END) AS avg_tb_rate_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.hr_rate END) AS avg_hr_rate_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.plateAppearances END) AS sum_pa_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.atBats END) AS sum_ab_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 AND p2.strikeOuts >= 1 THEN 1.0
                 WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN 0.0 END) AS pct_1plus_k_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 AND p2.strikeOuts >= 2 THEN 1.0
                 WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN 0.0 END) AS pct_2plus_k_last_5,

        /* -------------------- LAST 5 VOLUME-WEIGHTED AVG -------------------- */
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.strikeOuts END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.plateAppearances END), 0) AS weighted_k_rate_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.baseOnBalls END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.plateAppearances END), 0) AS weighted_walk_rate_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.hits END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.plateAppearances END), 0) AS weighted_hit_rate_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.totalBases END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.plateAppearances END), 0) AS weighted_tb_rate_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.homeRuns END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.plateAppearances END), 0) AS weighted_hr_rate_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.hits END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.atBats END), 0) AS weighted_batting_avg_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.numberOfPitches END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.plateAppearances END), 0) AS weighted_pitches_per_pa_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.obp * p2.plateAppearances END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.plateAppearances END), 0) AS weighted_obp_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.slg * p2.atBats END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.atBats END), 0) AS weighted_slg_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.ops * p2.plateAppearances END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.plateAppearances END), 0) AS weighted_ops_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.babip * p2.atBats END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.atBats END), 0) AS weighted_babip_last_5,

        /* -------------------- LAST 10 SIMPLE AVG -------------------- */
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.strikeOuts END) AS avg_k_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.plateAppearances END) AS avg_pa_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.atBats END) AS avg_ab_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.hits END) AS avg_hits_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.homeRuns END) AS avg_hr_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.baseOnBalls END) AS avg_bb_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.numberOfPitches END) AS avg_pitches_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.totalBases END) AS avg_tb_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.rbi END) AS avg_rbi_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.leftOnBase END) AS avg_lob_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.obp END) AS avg_obp_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.slg END) AS avg_slg_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.ops END) AS avg_ops_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.babip END) AS avg_babip_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.batting_avg END) AS avg_batting_avg_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.hitByPitch END) AS avg_hbp_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.sacFlies END) AS avg_sf_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.sacBunts END) AS avg_sbunts_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.stolenBases END) AS avg_stolen_bases_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.caughtStealing END) AS avg_caught_stealing_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.k_rate END) AS avg_k_rate_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.walk_rate END) AS avg_walk_rate_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.hit_rate END) AS avg_hit_rate_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.tb_rate END) AS avg_tb_rate_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.hr_rate END) AS avg_hr_rate_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.plateAppearances END) AS sum_pa_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.atBats END) AS sum_ab_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 AND p2.strikeOuts >= 1 THEN 1.0
                 WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN 0.0 END) AS pct_1plus_k_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 AND p2.strikeOuts >= 2 THEN 1.0
                 WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN 0.0 END) AS pct_2plus_k_last_10,

        /* -------------------- LAST 10 VOLUME-WEIGHTED AVG -------------------- */
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.strikeOuts END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.plateAppearances END), 0) AS weighted_k_rate_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.baseOnBalls END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.plateAppearances END), 0) AS weighted_walk_rate_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.hits END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.plateAppearances END), 0) AS weighted_hit_rate_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.totalBases END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.plateAppearances END), 0) AS weighted_tb_rate_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.homeRuns END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.plateAppearances END), 0) AS weighted_hr_rate_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.hits END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.atBats END), 0) AS weighted_batting_avg_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.numberOfPitches END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.plateAppearances END), 0) AS weighted_pitches_per_pa_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.obp * p2.plateAppearances END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.plateAppearances END), 0) AS weighted_obp_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.slg * p2.atBats END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.atBats END), 0) AS weighted_slg_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.ops * p2.plateAppearances END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.plateAppearances END), 0) AS weighted_ops_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.babip * p2.atBats END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.atBats END), 0) AS weighted_babip_last_10,

        /* -------------------- PREVIOUS GAME -------------------- */
        MAX(CASE WHEN p2.rn = p1.rn - 1 THEN p2.strikeOuts END) AS prev_k,
        MAX(CASE WHEN p2.rn = p1.rn - 1 THEN p2.plateAppearances END) AS prev_pa,
        MAX(CASE WHEN p2.rn = p1.rn - 1 THEN p2.atBats END) AS prev_ab,
        MAX(CASE WHEN p2.rn = p1.rn - 1 THEN p2.hits END) AS prev_hits,
        MAX(CASE WHEN p2.rn = p1.rn - 1 THEN p2.homeRuns END) AS prev_hr,
        MAX(CASE WHEN p2.rn = p1.rn - 1 THEN p2.baseOnBalls END) AS prev_bb,
        MAX(CASE WHEN p2.rn = p1.rn - 1 THEN p2.numberOfPitches END) AS prev_pitches,
        MAX(CASE WHEN p2.rn = p1.rn - 1 THEN p2.ops END) AS prev_ops,
        MAX(CASE WHEN p2.rn = p1.rn - 1 THEN p2.k_rate END) AS prev_k_rate

    FROM prep p1
    LEFT JOIN prep p2
        ON p1.player_id = p2.player_id
       AND p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1
    GROUP BY
        p1.gamePk,
        p1.game_date,
        p1.season,
        p1.player_id,
        p1.player_name,
        p1.position,
        p1.team_id,
        p1.team_name,
        p1.strikeOuts,
        p1.rn
)

SELECT *
INTO mlb.dbo.fact_hitter_rolling_featuresv2
FROM rolling;
    """

    execute_sql(sql)
    logger.info("Finished building mlb.dbo.fact_hitter_rolling_featuresv2")


def build_hitter_model_features() -> None:
    """
    Join traditional rolling features with statcast rolling features.

    The join is on (player_id, gamePk, season). Because both rolling pipelines
    now partition ROW_NUMBER by player_id only, their windows align naturally
    across seasons.
    """
    logger.info("Building mlb.dbo.fact_hitter_model_featuresv2")

    sql = """
IF OBJECT_ID('mlb.dbo.fact_hitter_model_featuresv2', 'U') IS NOT NULL
    DROP TABLE mlb.dbo.fact_hitter_model_featuresv2;

SELECT
    h.gamePk,
    h.game_date,
    h.season,
    h.player_id,
    h.player_name,
    h.position,
    h.team_id,
    h.team_name,
    h.strikeOuts,

    /* -------------------- hitter rolling -------------------- */
    h.days_since_last_game,

    -- last 3 simple
    h.avg_k_last_3,
    h.avg_pa_last_3,
    h.avg_ab_last_3,
    h.avg_hits_last_3,
    h.avg_hr_last_3,
    h.avg_bb_last_3,
    h.avg_pitches_last_3,
    h.avg_tb_last_3,
    h.avg_rbi_last_3,
    h.avg_lob_last_3,
    h.avg_obp_last_3,
    h.avg_slg_last_3,
    h.avg_ops_last_3,
    h.avg_babip_last_3,
    h.avg_batting_avg_last_3,
    h.avg_hbp_last_3,
    h.avg_sf_last_3,
    h.avg_sbunts_last_3,
    h.avg_stolen_bases_last_3,
    h.avg_caught_stealing_last_3,
    h.avg_k_rate_last_3,
    h.avg_walk_rate_last_3,
    h.avg_hit_rate_last_3,
    h.avg_tb_rate_last_3,
    h.avg_hr_rate_last_3,
    h.sum_pa_last_3,
    h.sum_ab_last_3,
    h.pct_1plus_k_last_3,
    h.pct_2plus_k_last_3,

    -- last 3 weighted
    h.weighted_k_rate_last_3,
    h.weighted_walk_rate_last_3,
    h.weighted_hit_rate_last_3,
    h.weighted_tb_rate_last_3,
    h.weighted_hr_rate_last_3,
    h.weighted_batting_avg_last_3,
    h.weighted_pitches_per_pa_last_3,
    h.weighted_obp_last_3,
    h.weighted_slg_last_3,
    h.weighted_ops_last_3,
    h.weighted_babip_last_3,

    -- last 5 simple
    h.avg_k_last_5,
    h.avg_pa_last_5,
    h.avg_ab_last_5,
    h.avg_hits_last_5,
    h.avg_hr_last_5,
    h.avg_bb_last_5,
    h.avg_pitches_last_5,
    h.avg_tb_last_5,
    h.avg_rbi_last_5,
    h.avg_lob_last_5,
    h.avg_obp_last_5,
    h.avg_slg_last_5,
    h.avg_ops_last_5,
    h.avg_babip_last_5,
    h.avg_batting_avg_last_5,
    h.avg_hbp_last_5,
    h.avg_sf_last_5,
    h.avg_sbunts_last_5,
    h.avg_stolen_bases_last_5,
    h.avg_caught_stealing_last_5,
    h.avg_k_rate_last_5,
    h.avg_walk_rate_last_5,
    h.avg_hit_rate_last_5,
    h.avg_tb_rate_last_5,
    h.avg_hr_rate_last_5,
    h.sum_pa_last_5,
    h.sum_ab_last_5,
    h.pct_1plus_k_last_5,
    h.pct_2plus_k_last_5,

    -- last 5 weighted
    h.weighted_k_rate_last_5,
    h.weighted_walk_rate_last_5,
    h.weighted_hit_rate_last_5,
    h.weighted_tb_rate_last_5,
    h.weighted_hr_rate_last_5,
    h.weighted_batting_avg_last_5,
    h.weighted_pitches_per_pa_last_5,
    h.weighted_obp_last_5,
    h.weighted_slg_last_5,
    h.weighted_ops_last_5,
    h.weighted_babip_last_5,

    -- last 10 simple
    h.avg_k_last_10,
    h.avg_pa_last_10,
    h.avg_ab_last_10,
    h.avg_hits_last_10,
    h.avg_hr_last_10,
    h.avg_bb_last_10,
    h.avg_pitches_last_10,
    h.avg_tb_last_10,
    h.avg_rbi_last_10,
    h.avg_lob_last_10,
    h.avg_obp_last_10,
    h.avg_slg_last_10,
    h.avg_ops_last_10,
    h.avg_babip_last_10,
    h.avg_batting_avg_last_10,
    h.avg_hbp_last_10,
    h.avg_sf_last_10,
    h.avg_sbunts_last_10,
    h.avg_stolen_bases_last_10,
    h.avg_caught_stealing_last_10,
    h.avg_k_rate_last_10,
    h.avg_walk_rate_last_10,
    h.avg_hit_rate_last_10,
    h.avg_tb_rate_last_10,
    h.avg_hr_rate_last_10,
    h.sum_pa_last_10,
    h.sum_ab_last_10,
    h.pct_1plus_k_last_10,
    h.pct_2plus_k_last_10,

    -- last 10 weighted
    h.weighted_k_rate_last_10,
    h.weighted_walk_rate_last_10,
    h.weighted_hit_rate_last_10,
    h.weighted_tb_rate_last_10,
    h.weighted_hr_rate_last_10,
    h.weighted_batting_avg_last_10,
    h.weighted_pitches_per_pa_last_10,
    h.weighted_obp_last_10,
    h.weighted_slg_last_10,
    h.weighted_ops_last_10,
    h.weighted_babip_last_10,

    -- previous game
    h.prev_k,
    h.prev_pa,
    h.prev_ab,
    h.prev_hits,
    h.prev_hr,
    h.prev_bb,
    h.prev_pitches,
    h.prev_ops,
    h.prev_k_rate,

    /* -------------------- hitter statcast rolling -------------------- */

    -- last 3 simple
    s.avg_sc_pitches_seen_last_3,
    s.avg_whiff_rate_last_3,
    s.avg_contact_rate_last_3,
    s.avg_swing_rate_last_3,
    s.avg_chase_rate_last_3,
    s.avg_zone_swing_rate_last_3,
    s.avg_zone_rate_last_3,
    s.avg_called_strike_rate_last_3,
    s.avg_csw_against_rate_last_3,
    s.avg_two_strike_whiff_rate_last_3,
    s.avg_whiff_rate_0_2_last_3,
    s.avg_whiff_rate_1_2_last_3,
    s.avg_whiff_rate_2_2_last_3,
    s.avg_exit_velocity_last_3,
    s.avg_max_exit_velocity_last_3,
    s.avg_launch_angle_last_3,
    s.avg_hit_distance_last_3,
    s.avg_xba_last_3,
    s.avg_xwoba_last_3,
    s.avg_woba_value_last_3,
    s.avg_babip_value_last_3,
    s.avg_iso_value_last_3,
    s.avg_bat_speed_last_3,
    s.avg_swing_length_last_3,
    s.avg_pitch_velocity_seen_last_3,
    s.avg_pitch_spin_seen_last_3,
    s.avg_pitch_extension_seen_last_3,
    s.avg_horz_movement_seen_last_3,
    s.avg_vert_movement_seen_last_3,
    s.avg_plate_x_seen_last_3,
    s.avg_plate_z_seen_last_3,
    s.avg_ff_seen_pct_last_3,
    s.avg_si_seen_pct_last_3,
    s.avg_fc_seen_pct_last_3,
    s.avg_sl_seen_pct_last_3,
    s.avg_cu_seen_pct_last_3,
    s.avg_ch_seen_pct_last_3,
    s.avg_fs_seen_pct_last_3,
    s.avg_whiff_rate_vs_rhp_last_3,
    s.avg_whiff_rate_vs_lhp_last_3,

    -- last 3 weighted
    s.weighted_whiff_rate_last_3,
    s.weighted_contact_rate_last_3,
    s.weighted_swing_rate_last_3,
    s.weighted_chase_rate_last_3,
    s.weighted_zone_swing_rate_last_3,
    s.weighted_zone_rate_last_3,
    s.weighted_called_strike_rate_last_3,
    s.weighted_csw_against_rate_last_3,
    s.weighted_two_strike_whiff_rate_last_3,
    s.weighted_whiff_rate_0_2_last_3,
    s.weighted_whiff_rate_1_2_last_3,
    s.weighted_whiff_rate_2_2_last_3,
    s.weighted_ff_seen_pct_last_3,
    s.weighted_si_seen_pct_last_3,
    s.weighted_fc_seen_pct_last_3,
    s.weighted_sl_seen_pct_last_3,
    s.weighted_cu_seen_pct_last_3,
    s.weighted_ch_seen_pct_last_3,
    s.weighted_fs_seen_pct_last_3,
    s.weighted_whiff_rate_vs_rhp_last_3,
    s.weighted_whiff_rate_vs_lhp_last_3,

    -- last 5 simple
    s.avg_sc_pitches_seen_last_5,
    s.avg_whiff_rate_last_5,
    s.avg_contact_rate_last_5,
    s.avg_swing_rate_last_5,
    s.avg_chase_rate_last_5,
    s.avg_zone_swing_rate_last_5,
    s.avg_zone_rate_last_5,
    s.avg_called_strike_rate_last_5,
    s.avg_csw_against_rate_last_5,
    s.avg_two_strike_whiff_rate_last_5,
    s.avg_whiff_rate_0_2_last_5,
    s.avg_whiff_rate_1_2_last_5,
    s.avg_whiff_rate_2_2_last_5,
    s.avg_exit_velocity_last_5,
    s.avg_max_exit_velocity_last_5,
    s.avg_launch_angle_last_5,
    s.avg_hit_distance_last_5,
    s.avg_xba_last_5,
    s.avg_xwoba_last_5,
    s.avg_woba_value_last_5,
    s.avg_babip_value_last_5,
    s.avg_iso_value_last_5,
    s.avg_bat_speed_last_5,
    s.avg_swing_length_last_5,
    s.avg_pitch_velocity_seen_last_5,
    s.avg_pitch_spin_seen_last_5,
    s.avg_pitch_extension_seen_last_5,
    s.avg_horz_movement_seen_last_5,
    s.avg_vert_movement_seen_last_5,
    s.avg_plate_x_seen_last_5,
    s.avg_plate_z_seen_last_5,
    s.avg_ff_seen_pct_last_5,
    s.avg_si_seen_pct_last_5,
    s.avg_fc_seen_pct_last_5,
    s.avg_sl_seen_pct_last_5,
    s.avg_cu_seen_pct_last_5,
    s.avg_ch_seen_pct_last_5,
    s.avg_fs_seen_pct_last_5,
    s.avg_whiff_rate_vs_rhp_last_5,
    s.avg_whiff_rate_vs_lhp_last_5,

    -- last 5 weighted
    s.weighted_whiff_rate_last_5,
    s.weighted_contact_rate_last_5,
    s.weighted_swing_rate_last_5,
    s.weighted_chase_rate_last_5,
    s.weighted_zone_swing_rate_last_5,
    s.weighted_zone_rate_last_5,
    s.weighted_called_strike_rate_last_5,
    s.weighted_csw_against_rate_last_5,
    s.weighted_two_strike_whiff_rate_last_5,
    s.weighted_whiff_rate_0_2_last_5,
    s.weighted_whiff_rate_1_2_last_5,
    s.weighted_whiff_rate_2_2_last_5,
    s.weighted_ff_seen_pct_last_5,
    s.weighted_si_seen_pct_last_5,
    s.weighted_fc_seen_pct_last_5,
    s.weighted_sl_seen_pct_last_5,
    s.weighted_cu_seen_pct_last_5,
    s.weighted_ch_seen_pct_last_5,
    s.weighted_fs_seen_pct_last_5,
    s.weighted_whiff_rate_vs_rhp_last_5,
    s.weighted_whiff_rate_vs_lhp_last_5,

    -- last 10 simple
    s.avg_sc_pitches_seen_last_10,
    s.avg_whiff_rate_last_10,
    s.avg_contact_rate_last_10,
    s.avg_swing_rate_last_10,
    s.avg_chase_rate_last_10,
    s.avg_zone_swing_rate_last_10,
    s.avg_zone_rate_last_10,
    s.avg_called_strike_rate_last_10,
    s.avg_csw_against_rate_last_10,
    s.avg_two_strike_whiff_rate_last_10,
    s.avg_whiff_rate_0_2_last_10,
    s.avg_whiff_rate_1_2_last_10,
    s.avg_whiff_rate_2_2_last_10,
    s.avg_exit_velocity_last_10,
    s.avg_max_exit_velocity_last_10,
    s.avg_launch_angle_last_10,
    s.avg_hit_distance_last_10,
    s.avg_xba_last_10,
    s.avg_xwoba_last_10,
    s.avg_woba_value_last_10,
    s.avg_babip_value_last_10,
    s.avg_iso_value_last_10,
    s.avg_bat_speed_last_10,
    s.avg_swing_length_last_10,
    s.avg_pitch_velocity_seen_last_10,
    s.avg_pitch_spin_seen_last_10,
    s.avg_pitch_extension_seen_last_10,
    s.avg_horz_movement_seen_last_10,
    s.avg_vert_movement_seen_last_10,
    s.avg_plate_x_seen_last_10,
    s.avg_plate_z_seen_last_10,
    s.avg_ff_seen_pct_last_10,
    s.avg_si_seen_pct_last_10,
    s.avg_fc_seen_pct_last_10,
    s.avg_sl_seen_pct_last_10,
    s.avg_cu_seen_pct_last_10,
    s.avg_ch_seen_pct_last_10,
    s.avg_fs_seen_pct_last_10,
    s.avg_whiff_rate_vs_rhp_last_10,
    s.avg_whiff_rate_vs_lhp_last_10,

    -- last 10 weighted
    s.weighted_whiff_rate_last_10,
    s.weighted_contact_rate_last_10,
    s.weighted_swing_rate_last_10,
    s.weighted_chase_rate_last_10,
    s.weighted_zone_swing_rate_last_10,
    s.weighted_zone_rate_last_10,
    s.weighted_called_strike_rate_last_10,
    s.weighted_csw_against_rate_last_10,
    s.weighted_two_strike_whiff_rate_last_10,
    s.weighted_whiff_rate_0_2_last_10,
    s.weighted_whiff_rate_1_2_last_10,
    s.weighted_whiff_rate_2_2_last_10,
    s.weighted_ff_seen_pct_last_10,
    s.weighted_si_seen_pct_last_10,
    s.weighted_fc_seen_pct_last_10,
    s.weighted_sl_seen_pct_last_10,
    s.weighted_cu_seen_pct_last_10,
    s.weighted_ch_seen_pct_last_10,
    s.weighted_fs_seen_pct_last_10,
    s.weighted_whiff_rate_vs_rhp_last_10,
    s.weighted_whiff_rate_vs_lhp_last_10,

    -- previous statcast game
    s.prev_whiff_rate,
    s.prev_contact_rate,
    s.prev_chase_rate,
    s.prev_exit_velocity,
    s.prev_xwoba,
    s.prev_bat_speed

INTO mlb.dbo.fact_hitter_model_featuresv2
FROM mlb.dbo.fact_hitter_rolling_featuresv2 h
LEFT JOIN mlb.dbo.fact_hitter_statcast_rolling_featuresv2 s
    ON h.player_id = s.player_id
   AND h.gamePk = s.game_pk
   AND h.season = s.season;
    """

    execute_sql(sql)
    logger.info("Finished building mlb.dbo.fact_hitter_model_featuresv2")


def run_all_hitter_features() -> None:
    logger.info("Starting hitter feature pipeline")
    build_hitter_statcast_game_agg()
    build_hitter_statcast_rolling_features()
    build_hitter_rolling_features()
    build_hitter_model_features()
    logger.info("Finished hitter feature pipeline")


if __name__ == "__main__":
    run_all_hitter_features()