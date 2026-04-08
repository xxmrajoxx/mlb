import logging
from sql.sql_loader import execute_sql

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


def build_hitter_statcast_game_agg() -> None:
    logger.info("Building mlb.dbo.fact_hitter_statcast_game_agg")

    sql = """
IF OBJECT_ID('mlb.dbo.fact_hitter_statcast_game_agg', 'U') IS NOT NULL
    DROP TABLE mlb.dbo.fact_hitter_statcast_game_agg;

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

INTO mlb.dbo.fact_hitter_statcast_game_agg
FROM agg;
    """
    execute_sql(sql)
    logger.info("Finished building mlb.dbo.fact_hitter_statcast_game_agg")


def build_hitter_statcast_rolling_features() -> None:
    logger.info("Building mlb.dbo.fact_hitter_statcast_rolling_features")

    sql = """
IF OBJECT_ID('mlb.dbo.fact_hitter_statcast_rolling_features', 'U') IS NOT NULL
    DROP TABLE mlb.dbo.fact_hitter_statcast_rolling_features;

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

        TRY_CAST(whiff_rate_vs_rhp AS float) AS whiff_rate_vs_rhp,
        TRY_CAST(whiff_rate_vs_lhp AS float) AS whiff_rate_vs_lhp

    FROM mlb.dbo.fact_hitter_statcast_game_agg
),

rolling AS (
    SELECT
        game_pk,
        game_date,
        season,
        player_id,
        player_name,
        team_id,

        -- last 3
        AVG(total_pitches_seen) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_sc_pitches_seen_last_3,

        AVG(whiff_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_whiff_rate_last_3,

        AVG(contact_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_contact_rate_last_3,

        AVG(swing_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_swing_rate_last_3,

        AVG(chase_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_chase_rate_last_3,

        AVG(zone_swing_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_zone_swing_rate_last_3,

        AVG(zone_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_zone_rate_last_3,

        AVG(called_strike_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_called_strike_rate_last_3,

        AVG(csw_against_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_csw_against_rate_last_3,

        AVG(two_strike_whiff_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_two_strike_whiff_rate_last_3,

        AVG(whiff_rate_0_2) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_whiff_rate_0_2_last_3,

        AVG(whiff_rate_1_2) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_whiff_rate_1_2_last_3,

        AVG(whiff_rate_2_2) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_whiff_rate_2_2_last_3,

        AVG(avg_exit_velocity) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_exit_velocity_last_3,

        AVG(max_exit_velocity) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_max_exit_velocity_last_3,

        AVG(avg_launch_angle) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_launch_angle_last_3,

        AVG(avg_hit_distance) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_hit_distance_last_3,

        AVG(avg_xba) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_xba_last_3,

        AVG(avg_xwoba) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_xwoba_last_3,

        AVG(avg_woba_value) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_woba_value_last_3,

        AVG(avg_babip_value) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_babip_value_last_3,

        AVG(avg_iso_value) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_iso_value_last_3,

        AVG(avg_bat_speed) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_bat_speed_last_3,

        AVG(avg_swing_length) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_swing_length_last_3,

        AVG(avg_pitch_velocity_seen) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_pitch_velocity_seen_last_3,

        AVG(avg_pitch_spin_seen) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_pitch_spin_seen_last_3,

        AVG(avg_horz_movement_seen) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_horz_movement_seen_last_3,

        AVG(avg_vert_movement_seen) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_vert_movement_seen_last_3,

        AVG(ff_seen_pct) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_ff_seen_pct_last_3,

        AVG(sl_seen_pct) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_sl_seen_pct_last_3,

        AVG(cu_seen_pct) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_cu_seen_pct_last_3,

        AVG(ch_seen_pct) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_ch_seen_pct_last_3,

        AVG(whiff_rate_vs_rhp) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_whiff_rate_vs_rhp_last_3,

        AVG(whiff_rate_vs_lhp) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_whiff_rate_vs_lhp_last_3,

        -- last 5
        AVG(total_pitches_seen) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_sc_pitches_seen_last_5,

        AVG(whiff_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_whiff_rate_last_5,

        AVG(contact_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_contact_rate_last_5,

        AVG(chase_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_chase_rate_last_5,

        AVG(zone_swing_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_zone_swing_rate_last_5,

        AVG(csw_against_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_csw_against_rate_last_5,

        AVG(avg_exit_velocity) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_exit_velocity_last_5,

        AVG(avg_xwoba) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_xwoba_last_5,

        AVG(avg_bat_speed) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_bat_speed_last_5,

        AVG(whiff_rate_vs_rhp) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_whiff_rate_vs_rhp_last_5,

        AVG(whiff_rate_vs_lhp) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_whiff_rate_vs_lhp_last_5,

        -- previous game
        LAG(whiff_rate, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
        ) AS prev_whiff_rate,

        LAG(contact_rate, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
        ) AS prev_contact_rate,

        LAG(chase_rate, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
        ) AS prev_chase_rate,

        LAG(avg_exit_velocity, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
        ) AS prev_exit_velocity,

        LAG(avg_xwoba, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
        ) AS prev_xwoba,

        LAG(avg_bat_speed, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
        ) AS prev_bat_speed

    FROM base
)

SELECT *
INTO mlb.dbo.fact_hitter_statcast_rolling_features
FROM rolling;
    """
    execute_sql(sql)
    logger.info("Finished building mlb.dbo.fact_hitter_statcast_rolling_features")


def build_hitter_rolling_features() -> None:
    logger.info("Building mlb.dbo.fact_hitter_rolling_features")

    sql = """
IF OBJECT_ID('mlb.dbo.fact_hitter_rolling_features', 'U') IS NOT NULL
    DROP TABLE mlb.dbo.fact_hitter_rolling_features;

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

        TRY_CAST(strikeOuts AS float) * 1.0 / NULLIF(TRY_CAST(plateAppearances AS float), 0) AS k_rate,
        TRY_CAST(baseOnBalls AS float) * 1.0 / NULLIF(TRY_CAST(plateAppearances AS float), 0) AS walk_rate,
        TRY_CAST(hits AS float) * 1.0 / NULLIF(TRY_CAST(plateAppearances AS float), 0) AS hit_rate,
        TRY_CAST(totalBases AS float) * 1.0 / NULLIF(TRY_CAST(plateAppearances AS float), 0) AS tb_rate,
        TRY_CAST(homeRuns AS float) * 1.0 / NULLIF(TRY_CAST(plateAppearances AS float), 0) AS hr_rate

    FROM mlb.dbo.fact_player_hitting_gamelogs
    WHERE player_id IS NOT NULL
      AND gamePk IS NOT NULL
),

rolling AS (
    SELECT
        gamePk,
        game_date,
        season,
        player_id,
        player_name,
        position,
        team_id,
        team_name,

        strikeOuts,

        DATEDIFF(
            DAY,
            LAG(game_date, 1) OVER (
                PARTITION BY player_id, season
                ORDER BY game_date, gamePk
            ),
            game_date
        ) AS days_since_last_game,

        AVG(strikeOuts) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_k_last_3,
        AVG(plateAppearances) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_pa_last_3,
        AVG(atBats) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_ab_last_3,
        AVG(hits) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_hits_last_3,
        AVG(homeRuns) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_hr_last_3,
        AVG(baseOnBalls) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_bb_last_3,
        AVG(numberOfPitches) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_pitches_last_3,
        AVG(totalBases) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_tb_last_3,
        AVG(rbi) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_rbi_last_3,
        AVG(obp) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_obp_last_3,
        AVG(slg) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_slg_last_3,
        AVG(ops) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_ops_last_3,
        AVG(babip) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_babip_last_3,
        AVG(batting_avg) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_batting_avg_last_3,
        AVG(k_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_k_rate_last_3,
        AVG(walk_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_walk_rate_last_3,
        AVG(hit_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_hit_rate_last_3,
        AVG(tb_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_tb_rate_last_3,
        AVG(hr_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_hr_rate_last_3,
        SUM(plateAppearances) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS sum_pa_last_3,
        SUM(atBats) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS sum_ab_last_3,
        AVG(CASE WHEN strikeOuts >= 1 THEN 1.0 ELSE 0.0 END) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS pct_1plus_k_last_3,
        AVG(CASE WHEN strikeOuts >= 2 THEN 1.0 ELSE 0.0 END) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS pct_2plus_k_last_3,

        AVG(strikeOuts) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_k_last_5,
        AVG(plateAppearances) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_pa_last_5,
        AVG(atBats) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_ab_last_5,
        AVG(hits) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_hits_last_5,
        AVG(homeRuns) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_hr_last_5,
        AVG(baseOnBalls) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_bb_last_5,
        AVG(numberOfPitches) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_pitches_last_5,
        AVG(totalBases) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_tb_last_5,
        AVG(rbi) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_rbi_last_5,
        AVG(obp) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_obp_last_5,
        AVG(slg) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_slg_last_5,
        AVG(ops) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_ops_last_5,
        AVG(babip) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_babip_last_5,
        AVG(batting_avg) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_batting_avg_last_5,
        AVG(k_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_k_rate_last_5,
        AVG(walk_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_walk_rate_last_5,
        AVG(hit_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_hit_rate_last_5,
        AVG(tb_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_tb_rate_last_5,
        AVG(hr_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_hr_rate_last_5,
        SUM(plateAppearances) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS sum_pa_last_5,
        SUM(atBats) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS sum_ab_last_5,
        AVG(CASE WHEN strikeOuts >= 1 THEN 1.0 ELSE 0.0 END) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS pct_1plus_k_last_5,
        AVG(CASE WHEN strikeOuts >= 2 THEN 1.0 ELSE 0.0 END) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS pct_2plus_k_last_5,

        LAG(strikeOuts, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
        ) AS prev_k,
        LAG(plateAppearances, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
        ) AS prev_pa,
        LAG(atBats, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
        ) AS prev_ab,
        LAG(hits, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
        ) AS prev_hits,
        LAG(homeRuns, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
        ) AS prev_hr,
        LAG(baseOnBalls, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
        ) AS prev_bb,
        LAG(numberOfPitches, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
        ) AS prev_pitches,
        LAG(ops, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
        ) AS prev_ops,
        LAG(k_rate, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
        ) AS prev_k_rate

    FROM base
)

SELECT *
INTO mlb.dbo.fact_hitter_rolling_features
FROM rolling;
    """

    execute_sql(sql)
    logger.info("Finished building mlb.dbo.fact_hitter_rolling_features")


def build_hitter_model_features() -> None:
    logger.info("Building mlb.dbo.fact_hitter_model_features")

    sql = """
IF OBJECT_ID('mlb.dbo.fact_hitter_model_features', 'U') IS NOT NULL
    DROP TABLE mlb.dbo.fact_hitter_model_features;

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

    h.days_since_last_game,

    h.avg_k_last_3,
    h.avg_pa_last_3,
    h.avg_ab_last_3,
    h.avg_hits_last_3,
    h.avg_hr_last_3,
    h.avg_bb_last_3,
    h.avg_pitches_last_3,
    h.avg_tb_last_3,
    h.avg_rbi_last_3,
    h.avg_obp_last_3,
    h.avg_slg_last_3,
    h.avg_ops_last_3,
    h.avg_babip_last_3,
    h.avg_batting_avg_last_3,
    h.avg_k_rate_last_3,
    h.avg_walk_rate_last_3,
    h.avg_hit_rate_last_3,
    h.avg_tb_rate_last_3,
    h.avg_hr_rate_last_3,
    h.sum_pa_last_3,
    h.sum_ab_last_3,
    h.pct_1plus_k_last_3,
    h.pct_2plus_k_last_3,

    h.avg_k_last_5,
    h.avg_pa_last_5,
    h.avg_ab_last_5,
    h.avg_hits_last_5,
    h.avg_hr_last_5,
    h.avg_bb_last_5,
    h.avg_pitches_last_5,
    h.avg_tb_last_5,
    h.avg_rbi_last_5,
    h.avg_obp_last_5,
    h.avg_slg_last_5,
    h.avg_ops_last_5,
    h.avg_babip_last_5,
    h.avg_batting_avg_last_5,
    h.avg_k_rate_last_5,
    h.avg_walk_rate_last_5,
    h.avg_hit_rate_last_5,
    h.avg_tb_rate_last_5,
    h.avg_hr_rate_last_5,
    h.sum_pa_last_5,
    h.sum_ab_last_5,
    h.pct_1plus_k_last_5,
    h.pct_2plus_k_last_5,

    h.prev_k,
    h.prev_pa,
    h.prev_ab,
    h.prev_hits,
    h.prev_hr,
    h.prev_bb,
    h.prev_pitches,
    h.prev_ops,
    h.prev_k_rate,

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
    s.avg_horz_movement_seen_last_3,
    s.avg_vert_movement_seen_last_3,
    s.avg_ff_seen_pct_last_3,
    s.avg_sl_seen_pct_last_3,
    s.avg_cu_seen_pct_last_3,
    s.avg_ch_seen_pct_last_3,
    s.avg_whiff_rate_vs_rhp_last_3,
    s.avg_whiff_rate_vs_lhp_last_3,

    s.avg_sc_pitches_seen_last_5,
    s.avg_whiff_rate_last_5,
    s.avg_contact_rate_last_5,
    s.avg_chase_rate_last_5,
    s.avg_zone_swing_rate_last_5,
    s.avg_csw_against_rate_last_5,
    s.avg_exit_velocity_last_5,
    s.avg_xwoba_last_5,
    s.avg_bat_speed_last_5,
    s.avg_whiff_rate_vs_rhp_last_5,
    s.avg_whiff_rate_vs_lhp_last_5,

    s.prev_whiff_rate,
    s.prev_contact_rate,
    s.prev_chase_rate,
    s.prev_exit_velocity,
    s.prev_xwoba,
    s.prev_bat_speed

INTO mlb.dbo.fact_hitter_model_features
FROM mlb.dbo.fact_hitter_rolling_features h
LEFT JOIN mlb.dbo.fact_hitter_statcast_rolling_features s
    ON h.player_id = s.player_id
   AND h.gamePk = s.game_pk
   AND h.season = s.season;
    """

    execute_sql(sql)
    logger.info("Finished building mlb.dbo.fact_hitter_model_features")


def run_all_hitter_features() -> None:
    logger.info("Starting hitter feature pipeline")
    build_hitter_statcast_game_agg()
    build_hitter_statcast_rolling_features()
    build_hitter_rolling_features()
    build_hitter_model_features()
    logger.info("Finished hitter feature pipeline")


if __name__ == "__main__":
    run_all_hitter_features()