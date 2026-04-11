import logging
from sql.sql_loader import execute_sql

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


def build_pitcher_statcast_game_agg() -> None:
    logger.info("Building mlb.dbo.fact_pitcher_statcast_game_agg")

    sql = """
IF OBJECT_ID('mlb.dbo.fact_pitcher_statcast_game_agg', 'U') IS NOT NULL
    DROP TABLE mlb.dbo.fact_pitcher_statcast_game_agg;

WITH statcast_base AS (
    SELECT
        game_pk,
        CAST(game_date AS date) AS game_date,
        player_id,
        pitcher,
        player_name,
        team_id,
        stand,
        pitch_type,
        pitch_name,
        description,
        type,
        strikes,
        balls,
        pitch_number,

        TRY_CAST(release_speed AS float) AS release_speed,
        TRY_CAST(release_spin_rate AS float) AS release_spin_rate,
        TRY_CAST(release_extension AS float) AS release_extension,
        TRY_CAST(launch_speed AS float) AS launch_speed,

        TRY_CAST(pfx_x AS float) AS pfx_x,
        TRY_CAST(pfx_z AS float) AS pfx_z,
        TRY_CAST(plate_x AS float) AS plate_x,
        TRY_CAST(plate_z AS float) AS plate_z

    FROM mlb.dbo.fact_player_pitch_statcast
    WHERE player_id IS NOT NULL
      AND game_pk IS NOT NULL
),

agg AS (
    SELECT
        game_pk,
        game_date,
        player_id,
        MAX(player_name) AS player_name,
        MAX(team_id) AS team_id,

        COUNT(*) AS total_pitches,

        -- core strike / whiff events
        SUM(CASE
                WHEN description IN ('swinging_strike', 'swinging_strike_blocked')
                THEN 1 ELSE 0
            END) AS whiffs,

        SUM(CASE
                WHEN description = 'called_strike'
                THEN 1 ELSE 0
            END) AS called_strikes,

        SUM(CASE
                WHEN type = 'S'
                THEN 1 ELSE 0
            END) AS strike_events,

        SUM(CASE
                WHEN pitch_number = 1
                 AND type IN ('S', 'X')
                THEN 1 ELSE 0
            END) AS first_pitch_strikes,

        SUM(CASE
                WHEN strikes = 2
                THEN 1 ELSE 0
            END) AS two_strike_pitches,

        SUM(CASE
                WHEN strikes = 2
                 AND description IN ('swinging_strike', 'swinging_strike_blocked')
                THEN 1 ELSE 0
            END) AS two_strike_whiffs,

        -- swing / chase / zone context
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

        SUM(CASE
                WHEN plate_x BETWEEN -0.83 AND 0.83
                 AND plate_z BETWEEN 1.5 AND 3.5
                THEN 1 ELSE 0
            END) AS pitches_in_zone,

        -- count context
        SUM(CASE
                WHEN balls = 0 AND strikes = 2 THEN 1 ELSE 0
            END) AS pitches_0_2,
        SUM(CASE
                WHEN balls = 0 AND strikes = 2
                 AND description IN ('swinging_strike', 'swinging_strike_blocked')
                THEN 1 ELSE 0
            END) AS whiffs_0_2,

        SUM(CASE
                WHEN balls = 1 AND strikes = 2 THEN 1 ELSE 0
            END) AS pitches_1_2,
        SUM(CASE
                WHEN balls = 1 AND strikes = 2
                 AND description IN ('swinging_strike', 'swinging_strike_blocked')
                THEN 1 ELSE 0
            END) AS whiffs_1_2,

        SUM(CASE
                WHEN balls = 2 AND strikes = 2 THEN 1 ELSE 0
            END) AS pitches_2_2,
        SUM(CASE
                WHEN balls = 2 AND strikes = 2
                 AND description IN ('swinging_strike', 'swinging_strike_blocked')
                THEN 1 ELSE 0
            END) AS whiffs_2_2,

        -- pitch quality
        AVG(release_speed) AS avg_velocity,
        MAX(release_speed) AS max_velocity,
        AVG(release_spin_rate) AS avg_spin_rate,
        AVG(release_extension) AS avg_extension,
        AVG(launch_speed) AS avg_exit_velocity_allowed,

        AVG(pfx_x) AS avg_horz_movement,
        AVG(pfx_z) AS avg_vert_movement,
        AVG(plate_x) AS avg_plate_x,
        AVG(plate_z) AS avg_plate_z,

        -- pitch counts
        SUM(CASE WHEN pitch_type = 'FF' THEN 1 ELSE 0 END) AS ff_count,
        SUM(CASE WHEN pitch_type = 'SI' THEN 1 ELSE 0 END) AS si_count,
        SUM(CASE WHEN pitch_type = 'FC' THEN 1 ELSE 0 END) AS fc_count,
        SUM(CASE WHEN pitch_type = 'SL' THEN 1 ELSE 0 END) AS sl_count,
        SUM(CASE WHEN pitch_type = 'CU' THEN 1 ELSE 0 END) AS cu_count,
        SUM(CASE WHEN pitch_type = 'CH' THEN 1 ELSE 0 END) AS ch_count,
        SUM(CASE WHEN pitch_type = 'FS' THEN 1 ELSE 0 END) AS fs_count,

        -- pitch-type whiffs
        SUM(CASE
                WHEN pitch_type = 'FF'
                 AND description IN ('swinging_strike', 'swinging_strike_blocked')
                THEN 1 ELSE 0
            END) AS ff_whiffs,

        SUM(CASE
                WHEN pitch_type = 'SI'
                 AND description IN ('swinging_strike', 'swinging_strike_blocked')
                THEN 1 ELSE 0
            END) AS si_whiffs,

        SUM(CASE
                WHEN pitch_type = 'FC'
                 AND description IN ('swinging_strike', 'swinging_strike_blocked')
                THEN 1 ELSE 0
            END) AS fc_whiffs,

        SUM(CASE
                WHEN pitch_type = 'SL'
                 AND description IN ('swinging_strike', 'swinging_strike_blocked')
                THEN 1 ELSE 0
            END) AS sl_whiffs,

        SUM(CASE
                WHEN pitch_type = 'CU'
                 AND description IN ('swinging_strike', 'swinging_strike_blocked')
                THEN 1 ELSE 0
            END) AS cu_whiffs,

        SUM(CASE
                WHEN pitch_type = 'CH'
                 AND description IN ('swinging_strike', 'swinging_strike_blocked')
                THEN 1 ELSE 0
            END) AS ch_whiffs,

        SUM(CASE
                WHEN pitch_type = 'FS'
                 AND description IN ('swinging_strike', 'swinging_strike_blocked')
                THEN 1 ELSE 0
            END) AS fs_whiffs,

        -- batter handedness split
        SUM(CASE WHEN stand = 'R' THEN 1 ELSE 0 END) AS pitches_vs_rhb,
        SUM(CASE
                WHEN stand = 'R'
                 AND description IN ('swinging_strike', 'swinging_strike_blocked')
                THEN 1 ELSE 0
            END) AS whiffs_vs_rhb,

        SUM(CASE WHEN stand = 'L' THEN 1 ELSE 0 END) AS pitches_vs_lhb,
        SUM(CASE
                WHEN stand = 'L'
                 AND description IN ('swinging_strike', 'swinging_strike_blocked')
                THEN 1 ELSE 0
            END) AS whiffs_vs_lhb

    FROM statcast_base
    GROUP BY
        game_pk,
        game_date,
        player_id
)

SELECT
    game_pk,
    game_date,
    player_id,
    player_name,
    team_id,

    total_pitches,
    whiffs,
    called_strikes,
    strike_events,
    first_pitch_strikes,
    two_strike_pitches,
    two_strike_whiffs,
    swings,
    chase_swings,
    pitches_in_zone,

    -- main rates
    whiffs * 1.0 / NULLIF(total_pitches, 0) AS whiff_rate,
    called_strikes * 1.0 / NULLIF(total_pitches, 0) AS called_strike_rate,
    (whiffs + called_strikes) * 1.0 / NULLIF(total_pitches, 0) AS csw_rate,
    strike_events * 1.0 / NULLIF(total_pitches, 0) AS strike_rate,
    first_pitch_strikes * 1.0 / NULLIF(total_pitches, 0) AS approx_first_pitch_strike_rate,
    two_strike_whiffs * 1.0 / NULLIF(two_strike_pitches, 0) AS putaway_rate,

    swings * 1.0 / NULLIF(total_pitches, 0) AS swing_rate,
    chase_swings * 1.0 / NULLIF(swings, 0) AS chase_rate,
    pitches_in_zone * 1.0 / NULLIF(total_pitches, 0) AS zone_rate,

    -- count-context rates
    pitches_0_2,
    whiffs_0_2,
    whiffs_0_2 * 1.0 / NULLIF(pitches_0_2, 0) AS whiff_rate_0_2,

    pitches_1_2,
    whiffs_1_2,
    whiffs_1_2 * 1.0 / NULLIF(pitches_1_2, 0) AS whiff_rate_1_2,

    pitches_2_2,
    whiffs_2_2,
    whiffs_2_2 * 1.0 / NULLIF(pitches_2_2, 0) AS whiff_rate_2_2,

    -- pitch quality
    avg_velocity,
    max_velocity,
    avg_spin_rate,
    avg_extension,
    avg_exit_velocity_allowed,

    avg_horz_movement,
    avg_vert_movement,
    avg_plate_x,
    avg_plate_z,

    -- pitch mix counts
    ff_count,
    si_count,
    fc_count,
    sl_count,
    cu_count,
    ch_count,
    fs_count,

    -- pitch mix %
    ff_count * 1.0 / NULLIF(total_pitches, 0) AS ff_pct,
    si_count * 1.0 / NULLIF(total_pitches, 0) AS si_pct,
    fc_count * 1.0 / NULLIF(total_pitches, 0) AS fc_pct,
    sl_count * 1.0 / NULLIF(total_pitches, 0) AS sl_pct,
    cu_count * 1.0 / NULLIF(total_pitches, 0) AS cu_pct,
    ch_count * 1.0 / NULLIF(total_pitches, 0) AS ch_pct,
    fs_count * 1.0 / NULLIF(total_pitches, 0) AS fs_pct,

    -- pitch-type whiff effectiveness
    ff_whiffs,
    si_whiffs,
    fc_whiffs,
    sl_whiffs,
    cu_whiffs,
    ch_whiffs,
    fs_whiffs,

    ff_whiffs * 1.0 / NULLIF(ff_count, 0) AS ff_whiff_rate,
    si_whiffs * 1.0 / NULLIF(si_count, 0) AS si_whiff_rate,
    fc_whiffs * 1.0 / NULLIF(fc_count, 0) AS fc_whiff_rate,
    sl_whiffs * 1.0 / NULLIF(sl_count, 0) AS sl_whiff_rate,
    cu_whiffs * 1.0 / NULLIF(cu_count, 0) AS cu_whiff_rate,
    ch_whiffs * 1.0 / NULLIF(ch_count, 0) AS ch_whiff_rate,
    fs_whiffs * 1.0 / NULLIF(fs_count, 0) AS fs_whiff_rate,

    -- handedness splits
    pitches_vs_rhb,
    whiffs_vs_rhb,
    whiffs_vs_rhb * 1.0 / NULLIF(pitches_vs_rhb, 0) AS whiff_rate_vs_rhb,

    pitches_vs_lhb,
    whiffs_vs_lhb,
    whiffs_vs_lhb * 1.0 / NULLIF(pitches_vs_lhb, 0) AS whiff_rate_vs_lhb,

    pitches_vs_rhb * 1.0 / NULLIF(total_pitches, 0) AS pitches_vs_rhb_pct,
    pitches_vs_lhb * 1.0 / NULLIF(total_pitches, 0) AS pitches_vs_lhb_pct,

    whiffs_vs_rhb * 1.0 / NULLIF(total_pitches, 0) AS whiffs_vs_rhb_pct,
    whiffs_vs_lhb * 1.0 / NULLIF(total_pitches, 0) AS whiffs_vs_lhb_pct

INTO mlb.dbo.fact_pitcher_statcast_game_agg
FROM agg;
    """

    execute_sql(sql)
    logger.info("Finished building mlb.dbo.fact_pitcher_statcast_game_agg")


def build_pitcher_statcast_rolling_features() -> None:
    logger.info("Building mlb.dbo.fact_pitcher_statcast_rolling_features")

    sql = """
IF OBJECT_ID('mlb.dbo.fact_pitcher_statcast_rolling_features', 'U') IS NOT NULL
    DROP TABLE mlb.dbo.fact_pitcher_statcast_rolling_features;

WITH base AS (
    SELECT
        game_pk,
        CAST(game_date AS date) AS game_date,
        YEAR(CAST(game_date AS date)) AS season,
        player_id,
        player_name,
        team_id,

        TRY_CAST(total_pitches AS float) AS total_pitches,
        TRY_CAST(whiff_rate AS float) AS whiff_rate,
        TRY_CAST(called_strike_rate AS float) AS called_strike_rate,
        TRY_CAST(csw_rate AS float) AS csw_rate,
        TRY_CAST(strike_rate AS float) AS strike_rate,
        TRY_CAST(approx_first_pitch_strike_rate AS float) AS approx_first_pitch_strike_rate,
        TRY_CAST(putaway_rate AS float) AS putaway_rate,

        TRY_CAST(swing_rate AS float) AS swing_rate,
        TRY_CAST(chase_rate AS float) AS chase_rate,
        TRY_CAST(zone_rate AS float) AS zone_rate,

        TRY_CAST(whiff_rate_0_2 AS float) AS whiff_rate_0_2,
        TRY_CAST(whiff_rate_1_2 AS float) AS whiff_rate_1_2,
        TRY_CAST(whiff_rate_2_2 AS float) AS whiff_rate_2_2,

        TRY_CAST(avg_velocity AS float) AS avg_velocity,
        TRY_CAST(max_velocity AS float) AS max_velocity,
        TRY_CAST(avg_spin_rate AS float) AS avg_spin_rate,
        TRY_CAST(avg_extension AS float) AS avg_extension,
        TRY_CAST(avg_exit_velocity_allowed AS float) AS avg_exit_velocity_allowed,

        TRY_CAST(avg_horz_movement AS float) AS avg_horz_movement,
        TRY_CAST(avg_vert_movement AS float) AS avg_vert_movement,
        TRY_CAST(avg_plate_x AS float) AS avg_plate_x,
        TRY_CAST(avg_plate_z AS float) AS avg_plate_z,

        TRY_CAST(ff_pct AS float) AS ff_pct,
        TRY_CAST(si_pct AS float) AS si_pct,
        TRY_CAST(fc_pct AS float) AS fc_pct,
        TRY_CAST(sl_pct AS float) AS sl_pct,
        TRY_CAST(cu_pct AS float) AS cu_pct,
        TRY_CAST(ch_pct AS float) AS ch_pct,
        TRY_CAST(fs_pct AS float) AS fs_pct,

        TRY_CAST(ff_whiff_rate AS float) AS ff_whiff_rate,
        TRY_CAST(si_whiff_rate AS float) AS si_whiff_rate,
        TRY_CAST(fc_whiff_rate AS float) AS fc_whiff_rate,
        TRY_CAST(sl_whiff_rate AS float) AS sl_whiff_rate,
        TRY_CAST(cu_whiff_rate AS float) AS cu_whiff_rate,
        TRY_CAST(ch_whiff_rate AS float) AS ch_whiff_rate,
        TRY_CAST(fs_whiff_rate AS float) AS fs_whiff_rate,

        TRY_CAST(whiff_rate_vs_rhb AS float) AS whiff_rate_vs_rhb,
        TRY_CAST(whiff_rate_vs_lhb AS float) AS whiff_rate_vs_lhb

    FROM mlb.dbo.fact_pitcher_statcast_game_agg
    WHERE player_id IS NOT NULL
      AND game_pk IS NOT NULL
),

rolling AS (
    SELECT
        game_pk,
        game_date,
        season,
        player_id,
        player_name,
        team_id,

        -- =========================
        -- SIMPLE AVG LAST 3
        -- =========================
        AVG(total_pitches) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_sc_pitches_last_3,

        AVG(whiff_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_whiff_rate_last_3,

        AVG(called_strike_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_called_strike_rate_last_3,

        AVG(csw_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_csw_rate_last_3,

        AVG(strike_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_sc_strike_rate_last_3,

        AVG(approx_first_pitch_strike_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_fps_rate_last_3,

        AVG(putaway_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_putaway_rate_last_3,

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

        AVG(zone_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_zone_rate_last_3,

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

        AVG(avg_velocity) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_velocity_last_3,

        AVG(max_velocity) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_max_velocity_last_3,

        AVG(avg_spin_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_spin_rate_last_3,

        AVG(avg_extension) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_extension_last_3,

        AVG(avg_exit_velocity_allowed) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_ev_allowed_last_3,

        AVG(avg_horz_movement) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_horz_movement_last_3,

        AVG(avg_vert_movement) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_vert_movement_last_3,

        AVG(avg_plate_x) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_plate_x_last_3,

        AVG(avg_plate_z) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_plate_z_last_3,

        AVG(ff_pct) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_ff_pct_last_3,

        AVG(si_pct) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_si_pct_last_3,

        AVG(fc_pct) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_fc_pct_last_3,

        AVG(sl_pct) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_sl_pct_last_3,

        AVG(cu_pct) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_cu_pct_last_3,

        AVG(ch_pct) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_ch_pct_last_3,

        AVG(fs_pct) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_fs_pct_last_3,

        AVG(ff_whiff_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_ff_whiff_rate_last_3,

        AVG(si_whiff_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_si_whiff_rate_last_3,

        AVG(fc_whiff_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_fc_whiff_rate_last_3,

        AVG(sl_whiff_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_sl_whiff_rate_last_3,

        AVG(cu_whiff_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_cu_whiff_rate_last_3,

        AVG(ch_whiff_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_ch_whiff_rate_last_3,

        AVG(fs_whiff_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_fs_whiff_rate_last_3,

        AVG(whiff_rate_vs_rhb) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_whiff_vs_rhb_last_3,

        AVG(whiff_rate_vs_lhb) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_whiff_vs_lhb_last_3,

        -- =========================
        -- SIMPLE AVG LAST 5
        -- =========================
        AVG(total_pitches) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_sc_pitches_last_5,

        AVG(whiff_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_whiff_rate_last_5,

        AVG(called_strike_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_called_strike_rate_last_5,

        AVG(csw_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_csw_rate_last_5,

        AVG(strike_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_sc_strike_rate_last_5,

        AVG(approx_first_pitch_strike_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_fps_rate_last_5,

        AVG(putaway_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_putaway_rate_last_5,

        AVG(swing_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_swing_rate_last_5,

        AVG(chase_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_chase_rate_last_5,

        AVG(zone_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_zone_rate_last_5,

        AVG(whiff_rate_0_2) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_whiff_rate_0_2_last_5,

        AVG(whiff_rate_1_2) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_whiff_rate_1_2_last_5,

        AVG(whiff_rate_2_2) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_whiff_rate_2_2_last_5,

        AVG(avg_velocity) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_velocity_last_5,

        AVG(max_velocity) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_max_velocity_last_5,

        AVG(avg_spin_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_spin_rate_last_5,

        AVG(avg_extension) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_extension_last_5,

        AVG(avg_exit_velocity_allowed) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_ev_allowed_last_5,

        AVG(avg_horz_movement) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_horz_movement_last_5,

        AVG(avg_vert_movement) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_vert_movement_last_5,

        AVG(avg_plate_x) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_plate_x_last_5,

        AVG(avg_plate_z) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_plate_z_last_5,

        AVG(ff_pct) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_ff_pct_last_5,

        AVG(si_pct) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_si_pct_last_5,

        AVG(fc_pct) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_fc_pct_last_5,

        AVG(sl_pct) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_sl_pct_last_5,

        AVG(cu_pct) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_cu_pct_last_5,

        AVG(ch_pct) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_ch_pct_last_5,

        AVG(fs_pct) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_fs_pct_last_5,

        AVG(ff_whiff_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_ff_whiff_rate_last_5,

        AVG(si_whiff_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_si_whiff_rate_last_5,

        AVG(fc_whiff_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_fc_whiff_rate_last_5,

        AVG(sl_whiff_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_sl_whiff_rate_last_5,

        AVG(cu_whiff_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_cu_whiff_rate_last_5,

        AVG(ch_whiff_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_ch_whiff_rate_last_5,

        AVG(fs_whiff_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_fs_whiff_rate_last_5,

        AVG(whiff_rate_vs_rhb) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_whiff_vs_rhb_last_5,

        AVG(whiff_rate_vs_lhb) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_whiff_vs_lhb_last_5,

        -- =========================
        -- SIMPLE AVG LAST 10
        -- =========================
        AVG(total_pitches) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_sc_pitches_last_10,

        AVG(whiff_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_whiff_rate_last_10,

        AVG(called_strike_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_called_strike_rate_last_10,

        AVG(csw_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_csw_rate_last_10,

        AVG(strike_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_sc_strike_rate_last_10,

        AVG(approx_first_pitch_strike_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_fps_rate_last_10,

        AVG(putaway_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_putaway_rate_last_10,

        AVG(swing_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_swing_rate_last_10,

        AVG(chase_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_chase_rate_last_10,

        AVG(zone_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_zone_rate_last_10,

        AVG(whiff_rate_0_2) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_whiff_rate_0_2_last_10,

        AVG(whiff_rate_1_2) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_whiff_rate_1_2_last_10,

        AVG(whiff_rate_2_2) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_whiff_rate_2_2_last_10,

        AVG(avg_velocity) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_velocity_last_10,

        AVG(max_velocity) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_max_velocity_last_10,

        AVG(avg_spin_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_spin_rate_last_10,

        AVG(avg_extension) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_extension_last_10,

        AVG(avg_exit_velocity_allowed) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_ev_allowed_last_10,

        AVG(avg_horz_movement) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_horz_movement_last_10,

        AVG(avg_vert_movement) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_vert_movement_last_10,

        AVG(avg_plate_x) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_plate_x_last_10,

        AVG(avg_plate_z) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_plate_z_last_10,

        AVG(ff_pct) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_ff_pct_last_10,

        AVG(si_pct) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_si_pct_last_10,

        AVG(fc_pct) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_fc_pct_last_10,

        AVG(sl_pct) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_sl_pct_last_10,

        AVG(cu_pct) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_cu_pct_last_10,

        AVG(ch_pct) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_ch_pct_last_10,

        AVG(fs_pct) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_fs_pct_last_10,

        AVG(ff_whiff_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_ff_whiff_rate_last_10,

        AVG(si_whiff_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_si_whiff_rate_last_10,

        AVG(fc_whiff_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_fc_whiff_rate_last_10,

        AVG(sl_whiff_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_sl_whiff_rate_last_10,

        AVG(cu_whiff_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_cu_whiff_rate_last_10,

        AVG(ch_whiff_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_ch_whiff_rate_last_10,

        AVG(fs_whiff_rate) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_fs_whiff_rate_last_10,

        AVG(whiff_rate_vs_rhb) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_whiff_vs_rhb_last_10,

        AVG(whiff_rate_vs_lhb) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_whiff_vs_lhb_last_10,

        -- =========================
        -- WEIGHTED LAST 3
        -- weights: 0.50, 0.30, 0.20
        -- =========================
        (
            0.50 * LAG(total_pitches, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            0.30 * LAG(total_pitches, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            0.20 * LAG(total_pitches, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk)
        ) /
        NULLIF(
            (CASE WHEN LAG(total_pitches, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 0.50 ELSE 0 END) +
            (CASE WHEN LAG(total_pitches, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 0.30 ELSE 0 END) +
            (CASE WHEN LAG(total_pitches, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 0.20 ELSE 0 END),
            0
        ) AS weighted_sc_pitches_last_3,

        (
            0.50 * LAG(whiff_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            0.30 * LAG(whiff_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            0.20 * LAG(whiff_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk)
        ) /
        NULLIF(
            (CASE WHEN LAG(whiff_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 0.50 ELSE 0 END) +
            (CASE WHEN LAG(whiff_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 0.30 ELSE 0 END) +
            (CASE WHEN LAG(whiff_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 0.20 ELSE 0 END),
            0
        ) AS weighted_whiff_rate_last_3,

        (
            0.50 * LAG(csw_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            0.30 * LAG(csw_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            0.20 * LAG(csw_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk)
        ) /
        NULLIF(
            (CASE WHEN LAG(csw_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 0.50 ELSE 0 END) +
            (CASE WHEN LAG(csw_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 0.30 ELSE 0 END) +
            (CASE WHEN LAG(csw_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 0.20 ELSE 0 END),
            0
        ) AS weighted_csw_rate_last_3,

        (
            0.50 * LAG(strike_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            0.30 * LAG(strike_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            0.20 * LAG(strike_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk)
        ) /
        NULLIF(
            (CASE WHEN LAG(strike_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 0.50 ELSE 0 END) +
            (CASE WHEN LAG(strike_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 0.30 ELSE 0 END) +
            (CASE WHEN LAG(strike_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 0.20 ELSE 0 END),
            0
        ) AS weighted_sc_strike_rate_last_3,

        (
            0.50 * LAG(avg_velocity, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            0.30 * LAG(avg_velocity, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            0.20 * LAG(avg_velocity, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk)
        ) /
        NULLIF(
            (CASE WHEN LAG(avg_velocity, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 0.50 ELSE 0 END) +
            (CASE WHEN LAG(avg_velocity, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 0.30 ELSE 0 END) +
            (CASE WHEN LAG(avg_velocity, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 0.20 ELSE 0 END),
            0
        ) AS weighted_velocity_last_3,

        (
            0.50 * LAG(avg_spin_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            0.30 * LAG(avg_spin_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            0.20 * LAG(avg_spin_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk)
        ) /
        NULLIF(
            (CASE WHEN LAG(avg_spin_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 0.50 ELSE 0 END) +
            (CASE WHEN LAG(avg_spin_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 0.30 ELSE 0 END) +
            (CASE WHEN LAG(avg_spin_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 0.20 ELSE 0 END),
            0
        ) AS weighted_spin_rate_last_3,

        (
            0.50 * LAG(chase_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            0.30 * LAG(chase_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            0.20 * LAG(chase_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk)
        ) /
        NULLIF(
            (CASE WHEN LAG(chase_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 0.50 ELSE 0 END) +
            (CASE WHEN LAG(chase_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 0.30 ELSE 0 END) +
            (CASE WHEN LAG(chase_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 0.20 ELSE 0 END),
            0
        ) AS weighted_chase_rate_last_3,

        (
            0.50 * LAG(putaway_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            0.30 * LAG(putaway_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            0.20 * LAG(putaway_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk)
        ) /
        NULLIF(
            (CASE WHEN LAG(putaway_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 0.50 ELSE 0 END) +
            (CASE WHEN LAG(putaway_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 0.30 ELSE 0 END) +
            (CASE WHEN LAG(putaway_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 0.20 ELSE 0 END),
            0
        ) AS weighted_putaway_rate_last_3,

        -- =========================
        -- WEIGHTED LAST 5
        -- weights: 5,4,3,2,1
        -- =========================
        (
            5.0 * LAG(total_pitches, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            4.0 * LAG(total_pitches, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            3.0 * LAG(total_pitches, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            2.0 * LAG(total_pitches, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            1.0 * LAG(total_pitches, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk)
        ) /
        NULLIF(
            (CASE WHEN LAG(total_pitches, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 5.0 ELSE 0 END) +
            (CASE WHEN LAG(total_pitches, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 4.0 ELSE 0 END) +
            (CASE WHEN LAG(total_pitches, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 3.0 ELSE 0 END) +
            (CASE WHEN LAG(total_pitches, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 2.0 ELSE 0 END) +
            (CASE WHEN LAG(total_pitches, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 1.0 ELSE 0 END),
            0
        ) AS weighted_sc_pitches_last_5,

        (
            5.0 * LAG(whiff_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            4.0 * LAG(whiff_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            3.0 * LAG(whiff_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            2.0 * LAG(whiff_rate, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            1.0 * LAG(whiff_rate, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk)
        ) /
        NULLIF(
            (CASE WHEN LAG(whiff_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 5.0 ELSE 0 END) +
            (CASE WHEN LAG(whiff_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 4.0 ELSE 0 END) +
            (CASE WHEN LAG(whiff_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 3.0 ELSE 0 END) +
            (CASE WHEN LAG(whiff_rate, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 2.0 ELSE 0 END) +
            (CASE WHEN LAG(whiff_rate, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 1.0 ELSE 0 END),
            0
        ) AS weighted_whiff_rate_last_5,

        (
            5.0 * LAG(csw_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            4.0 * LAG(csw_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            3.0 * LAG(csw_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            2.0 * LAG(csw_rate, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            1.0 * LAG(csw_rate, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk)
        ) /
        NULLIF(
            (CASE WHEN LAG(csw_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 5.0 ELSE 0 END) +
            (CASE WHEN LAG(csw_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 4.0 ELSE 0 END) +
            (CASE WHEN LAG(csw_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 3.0 ELSE 0 END) +
            (CASE WHEN LAG(csw_rate, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 2.0 ELSE 0 END) +
            (CASE WHEN LAG(csw_rate, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 1.0 ELSE 0 END),
            0
        ) AS weighted_csw_rate_last_5,

        (
            5.0 * LAG(strike_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            4.0 * LAG(strike_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            3.0 * LAG(strike_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            2.0 * LAG(strike_rate, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            1.0 * LAG(strike_rate, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk)
        ) /
        NULLIF(
            (CASE WHEN LAG(strike_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 5.0 ELSE 0 END) +
            (CASE WHEN LAG(strike_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 4.0 ELSE 0 END) +
            (CASE WHEN LAG(strike_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 3.0 ELSE 0 END) +
            (CASE WHEN LAG(strike_rate, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 2.0 ELSE 0 END) +
            (CASE WHEN LAG(strike_rate, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 1.0 ELSE 0 END),
            0
        ) AS weighted_sc_strike_rate_last_5,

        (
            5.0 * LAG(avg_velocity, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            4.0 * LAG(avg_velocity, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            3.0 * LAG(avg_velocity, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            2.0 * LAG(avg_velocity, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            1.0 * LAG(avg_velocity, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk)
        ) /
        NULLIF(
            (CASE WHEN LAG(avg_velocity, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 5.0 ELSE 0 END) +
            (CASE WHEN LAG(avg_velocity, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 4.0 ELSE 0 END) +
            (CASE WHEN LAG(avg_velocity, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 3.0 ELSE 0 END) +
            (CASE WHEN LAG(avg_velocity, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 2.0 ELSE 0 END) +
            (CASE WHEN LAG(avg_velocity, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 1.0 ELSE 0 END),
            0
        ) AS weighted_velocity_last_5,

        (
            5.0 * LAG(avg_spin_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            4.0 * LAG(avg_spin_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            3.0 * LAG(avg_spin_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            2.0 * LAG(avg_spin_rate, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            1.0 * LAG(avg_spin_rate, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk)
        ) /
        NULLIF(
            (CASE WHEN LAG(avg_spin_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 5.0 ELSE 0 END) +
            (CASE WHEN LAG(avg_spin_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 4.0 ELSE 0 END) +
            (CASE WHEN LAG(avg_spin_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 3.0 ELSE 0 END) +
            (CASE WHEN LAG(avg_spin_rate, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 2.0 ELSE 0 END) +
            (CASE WHEN LAG(avg_spin_rate, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 1.0 ELSE 0 END),
            0
        ) AS weighted_spin_rate_last_5,

        (
            5.0 * LAG(chase_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            4.0 * LAG(chase_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            3.0 * LAG(chase_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            2.0 * LAG(chase_rate, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            1.0 * LAG(chase_rate, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk)
        ) /
        NULLIF(
            (CASE WHEN LAG(chase_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 5.0 ELSE 0 END) +
            (CASE WHEN LAG(chase_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 4.0 ELSE 0 END) +
            (CASE WHEN LAG(chase_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 3.0 ELSE 0 END) +
            (CASE WHEN LAG(chase_rate, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 2.0 ELSE 0 END) +
            (CASE WHEN LAG(chase_rate, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 1.0 ELSE 0 END),
            0
        ) AS weighted_chase_rate_last_5,

        (
            5.0 * LAG(putaway_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            4.0 * LAG(putaway_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            3.0 * LAG(putaway_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            2.0 * LAG(putaway_rate, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
            1.0 * LAG(putaway_rate, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk)
        ) /
        NULLIF(
            (CASE WHEN LAG(putaway_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 5.0 ELSE 0 END) +
            (CASE WHEN LAG(putaway_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 4.0 ELSE 0 END) +
            (CASE WHEN LAG(putaway_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 3.0 ELSE 0 END) +
            (CASE WHEN LAG(putaway_rate, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 2.0 ELSE 0 END) +
            (CASE WHEN LAG(putaway_rate, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 1.0 ELSE 0 END),
            0
        ) AS weighted_putaway_rate_last_5,

        -- =========================
        -- WEIGHTED LAST 10
        -- weights: 10,9,8,7,6,5,4,3,2,1
        -- =========================
        (
            10.0 * LAG(total_pitches, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             9.0 * LAG(total_pitches, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             8.0 * LAG(total_pitches, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             7.0 * LAG(total_pitches, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             6.0 * LAG(total_pitches, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             5.0 * LAG(total_pitches, 6) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             4.0 * LAG(total_pitches, 7) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             3.0 * LAG(total_pitches, 8) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             2.0 * LAG(total_pitches, 9) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             1.0 * LAG(total_pitches,10) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk)
        ) /
        NULLIF(
            (CASE WHEN LAG(total_pitches, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 10.0 ELSE 0 END) +
            (CASE WHEN LAG(total_pitches, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  9.0 ELSE 0 END) +
            (CASE WHEN LAG(total_pitches, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  8.0 ELSE 0 END) +
            (CASE WHEN LAG(total_pitches, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  7.0 ELSE 0 END) +
            (CASE WHEN LAG(total_pitches, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  6.0 ELSE 0 END) +
            (CASE WHEN LAG(total_pitches, 6) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  5.0 ELSE 0 END) +
            (CASE WHEN LAG(total_pitches, 7) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  4.0 ELSE 0 END) +
            (CASE WHEN LAG(total_pitches, 8) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  3.0 ELSE 0 END) +
            (CASE WHEN LAG(total_pitches, 9) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  2.0 ELSE 0 END) +
            (CASE WHEN LAG(total_pitches,10) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  1.0 ELSE 0 END),
            0
        ) AS weighted_sc_pitches_last_10,

        (
            10.0 * LAG(whiff_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             9.0 * LAG(whiff_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             8.0 * LAG(whiff_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             7.0 * LAG(whiff_rate, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             6.0 * LAG(whiff_rate, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             5.0 * LAG(whiff_rate, 6) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             4.0 * LAG(whiff_rate, 7) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             3.0 * LAG(whiff_rate, 8) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             2.0 * LAG(whiff_rate, 9) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             1.0 * LAG(whiff_rate,10) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk)
        ) /
        NULLIF(
            (CASE WHEN LAG(whiff_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 10.0 ELSE 0 END) +
            (CASE WHEN LAG(whiff_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  9.0 ELSE 0 END) +
            (CASE WHEN LAG(whiff_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  8.0 ELSE 0 END) +
            (CASE WHEN LAG(whiff_rate, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  7.0 ELSE 0 END) +
            (CASE WHEN LAG(whiff_rate, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  6.0 ELSE 0 END) +
            (CASE WHEN LAG(whiff_rate, 6) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  5.0 ELSE 0 END) +
            (CASE WHEN LAG(whiff_rate, 7) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  4.0 ELSE 0 END) +
            (CASE WHEN LAG(whiff_rate, 8) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  3.0 ELSE 0 END) +
            (CASE WHEN LAG(whiff_rate, 9) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  2.0 ELSE 0 END) +
            (CASE WHEN LAG(whiff_rate,10) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  1.0 ELSE 0 END),
            0
        ) AS weighted_whiff_rate_last_10,

        (
            10.0 * LAG(csw_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             9.0 * LAG(csw_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             8.0 * LAG(csw_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             7.0 * LAG(csw_rate, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             6.0 * LAG(csw_rate, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             5.0 * LAG(csw_rate, 6) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             4.0 * LAG(csw_rate, 7) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             3.0 * LAG(csw_rate, 8) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             2.0 * LAG(csw_rate, 9) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             1.0 * LAG(csw_rate,10) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk)
        ) /
        NULLIF(
            (CASE WHEN LAG(csw_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 10.0 ELSE 0 END) +
            (CASE WHEN LAG(csw_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  9.0 ELSE 0 END) +
            (CASE WHEN LAG(csw_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  8.0 ELSE 0 END) +
            (CASE WHEN LAG(csw_rate, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  7.0 ELSE 0 END) +
            (CASE WHEN LAG(csw_rate, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  6.0 ELSE 0 END) +
            (CASE WHEN LAG(csw_rate, 6) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  5.0 ELSE 0 END) +
            (CASE WHEN LAG(csw_rate, 7) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  4.0 ELSE 0 END) +
            (CASE WHEN LAG(csw_rate, 8) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  3.0 ELSE 0 END) +
            (CASE WHEN LAG(csw_rate, 9) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  2.0 ELSE 0 END) +
            (CASE WHEN LAG(csw_rate,10) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  1.0 ELSE 0 END),
            0
        ) AS weighted_csw_rate_last_10,

        (
            10.0 * LAG(strike_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             9.0 * LAG(strike_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             8.0 * LAG(strike_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             7.0 * LAG(strike_rate, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             6.0 * LAG(strike_rate, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             5.0 * LAG(strike_rate, 6) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             4.0 * LAG(strike_rate, 7) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             3.0 * LAG(strike_rate, 8) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             2.0 * LAG(strike_rate, 9) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             1.0 * LAG(strike_rate,10) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk)
        ) /
        NULLIF(
            (CASE WHEN LAG(strike_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 10.0 ELSE 0 END) +
            (CASE WHEN LAG(strike_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  9.0 ELSE 0 END) +
            (CASE WHEN LAG(strike_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  8.0 ELSE 0 END) +
            (CASE WHEN LAG(strike_rate, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  7.0 ELSE 0 END) +
            (CASE WHEN LAG(strike_rate, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  6.0 ELSE 0 END) +
            (CASE WHEN LAG(strike_rate, 6) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  5.0 ELSE 0 END) +
            (CASE WHEN LAG(strike_rate, 7) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  4.0 ELSE 0 END) +
            (CASE WHEN LAG(strike_rate, 8) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  3.0 ELSE 0 END) +
            (CASE WHEN LAG(strike_rate, 9) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  2.0 ELSE 0 END) +
            (CASE WHEN LAG(strike_rate,10) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  1.0 ELSE 0 END),
            0
        ) AS weighted_sc_strike_rate_last_10,

        (
            10.0 * LAG(avg_velocity, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             9.0 * LAG(avg_velocity, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             8.0 * LAG(avg_velocity, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             7.0 * LAG(avg_velocity, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             6.0 * LAG(avg_velocity, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             5.0 * LAG(avg_velocity, 6) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             4.0 * LAG(avg_velocity, 7) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             3.0 * LAG(avg_velocity, 8) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             2.0 * LAG(avg_velocity, 9) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             1.0 * LAG(avg_velocity,10) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk)
        ) /
        NULLIF(
            (CASE WHEN LAG(avg_velocity, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 10.0 ELSE 0 END) +
            (CASE WHEN LAG(avg_velocity, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  9.0 ELSE 0 END) +
            (CASE WHEN LAG(avg_velocity, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  8.0 ELSE 0 END) +
            (CASE WHEN LAG(avg_velocity, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  7.0 ELSE 0 END) +
            (CASE WHEN LAG(avg_velocity, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  6.0 ELSE 0 END) +
            (CASE WHEN LAG(avg_velocity, 6) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  5.0 ELSE 0 END) +
            (CASE WHEN LAG(avg_velocity, 7) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  4.0 ELSE 0 END) +
            (CASE WHEN LAG(avg_velocity, 8) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  3.0 ELSE 0 END) +
            (CASE WHEN LAG(avg_velocity, 9) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  2.0 ELSE 0 END) +
            (CASE WHEN LAG(avg_velocity,10) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  1.0 ELSE 0 END),
            0
        ) AS weighted_velocity_last_10,

        (
            10.0 * LAG(avg_spin_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             9.0 * LAG(avg_spin_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             8.0 * LAG(avg_spin_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             7.0 * LAG(avg_spin_rate, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             6.0 * LAG(avg_spin_rate, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             5.0 * LAG(avg_spin_rate, 6) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             4.0 * LAG(avg_spin_rate, 7) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             3.0 * LAG(avg_spin_rate, 8) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             2.0 * LAG(avg_spin_rate, 9) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             1.0 * LAG(avg_spin_rate,10) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk)
        ) /
        NULLIF(
            (CASE WHEN LAG(avg_spin_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 10.0 ELSE 0 END) +
            (CASE WHEN LAG(avg_spin_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  9.0 ELSE 0 END) +
            (CASE WHEN LAG(avg_spin_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  8.0 ELSE 0 END) +
            (CASE WHEN LAG(avg_spin_rate, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  7.0 ELSE 0 END) +
            (CASE WHEN LAG(avg_spin_rate, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  6.0 ELSE 0 END) +
            (CASE WHEN LAG(avg_spin_rate, 6) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  5.0 ELSE 0 END) +
            (CASE WHEN LAG(avg_spin_rate, 7) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  4.0 ELSE 0 END) +
            (CASE WHEN LAG(avg_spin_rate, 8) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  3.0 ELSE 0 END) +
            (CASE WHEN LAG(avg_spin_rate, 9) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  2.0 ELSE 0 END) +
            (CASE WHEN LAG(avg_spin_rate,10) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  1.0 ELSE 0 END),
            0
        ) AS weighted_spin_rate_last_10,

        (
            10.0 * LAG(chase_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             9.0 * LAG(chase_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             8.0 * LAG(chase_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             7.0 * LAG(chase_rate, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             6.0 * LAG(chase_rate, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             5.0 * LAG(chase_rate, 6) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             4.0 * LAG(chase_rate, 7) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             3.0 * LAG(chase_rate, 8) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             2.0 * LAG(chase_rate, 9) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             1.0 * LAG(chase_rate,10) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk)
        ) /
        NULLIF(
            (CASE WHEN LAG(chase_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 10.0 ELSE 0 END) +
            (CASE WHEN LAG(chase_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  9.0 ELSE 0 END) +
            (CASE WHEN LAG(chase_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  8.0 ELSE 0 END) +
            (CASE WHEN LAG(chase_rate, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  7.0 ELSE 0 END) +
            (CASE WHEN LAG(chase_rate, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  6.0 ELSE 0 END) +
            (CASE WHEN LAG(chase_rate, 6) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  5.0 ELSE 0 END) +
            (CASE WHEN LAG(chase_rate, 7) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  4.0 ELSE 0 END) +
            (CASE WHEN LAG(chase_rate, 8) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  3.0 ELSE 0 END) +
            (CASE WHEN LAG(chase_rate, 9) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  2.0 ELSE 0 END) +
            (CASE WHEN LAG(chase_rate,10) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  1.0 ELSE 0 END),
            0
        ) AS weighted_chase_rate_last_10,

        (
            10.0 * LAG(putaway_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             9.0 * LAG(putaway_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             8.0 * LAG(putaway_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             7.0 * LAG(putaway_rate, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             6.0 * LAG(putaway_rate, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             5.0 * LAG(putaway_rate, 6) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             4.0 * LAG(putaway_rate, 7) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             3.0 * LAG(putaway_rate, 8) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             2.0 * LAG(putaway_rate, 9) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) +
             1.0 * LAG(putaway_rate,10) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk)
        ) /
        NULLIF(
            (CASE WHEN LAG(putaway_rate, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN 10.0 ELSE 0 END) +
            (CASE WHEN LAG(putaway_rate, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  9.0 ELSE 0 END) +
            (CASE WHEN LAG(putaway_rate, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  8.0 ELSE 0 END) +
            (CASE WHEN LAG(putaway_rate, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  7.0 ELSE 0 END) +
            (CASE WHEN LAG(putaway_rate, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  6.0 ELSE 0 END) +
            (CASE WHEN LAG(putaway_rate, 6) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  5.0 ELSE 0 END) +
            (CASE WHEN LAG(putaway_rate, 7) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  4.0 ELSE 0 END) +
            (CASE WHEN LAG(putaway_rate, 8) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  3.0 ELSE 0 END) +
            (CASE WHEN LAG(putaway_rate, 9) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  2.0 ELSE 0 END) +
            (CASE WHEN LAG(putaway_rate,10) OVER (PARTITION BY player_id, season ORDER BY game_date, game_pk) IS NOT NULL THEN  1.0 ELSE 0 END),
            0
        ) AS weighted_putaway_rate_last_10,

        -- previous game (same season)
        LAG(whiff_rate, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
        ) AS prev_whiff_rate,

        LAG(csw_rate, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
        ) AS prev_csw_rate,

        LAG(avg_velocity, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
        ) AS prev_velocity,

        LAG(avg_spin_rate, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
        ) AS prev_spin_rate,

        LAG(chase_rate, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
        ) AS prev_chase_rate,

        LAG(zone_rate, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
        ) AS prev_zone_rate,

        LAG(sl_whiff_rate, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
        ) AS prev_sl_whiff_rate,

        LAG(ff_whiff_rate, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, game_pk
        ) AS prev_ff_whiff_rate

    FROM base
)

SELECT *
INTO mlb.dbo.fact_pitcher_statcast_rolling_features
FROM rolling;
    """

    execute_sql(sql)
    logger.info("Finished building mlb.dbo.fact_pitcher_statcast_rolling_features")


def build_pitcher_rolling_features() -> None:
    logger.info("Building mlb.dbo.fact_pitcher_rolling_features")

    sql = """
IF OBJECT_ID('mlb.dbo.fact_pitcher_rolling_features', 'U') IS NOT NULL
    DROP TABLE mlb.dbo.fact_pitcher_rolling_features;

WITH base AS (
    SELECT
        gamePk,
        CAST(game_date AS date) AS game_date,
        TRY_CAST(season AS int) AS season,
        TRY_CAST(player_id AS int) AS player_id,
        player_name,
        TRY_CAST(team_id AS int) AS team_id,
        team_name,

        TRY_CAST(gamesStarted AS float) AS gamesStarted,
        TRY_CAST(strikeOuts AS float) AS strikeOuts,
        TRY_CAST(inningsPitched AS float) AS inningsPitched,
        TRY_CAST(battersFaced AS float) AS battersFaced,
        TRY_CAST(numberOfPitches AS float) AS numberOfPitches,
        TRY_CAST(strikePercentage AS float) AS strikePercentage,
        TRY_CAST(strikes AS float) AS strikes,
        TRY_CAST(whip AS float) AS whip,
        TRY_CAST(baseOnBalls AS float) AS baseOnBalls,
        TRY_CAST(era AS float) AS era,
        TRY_CAST(outs AS float) AS outs,
        TRY_CAST(strikeoutsPer9Inn AS float) AS strikeoutsPer9Inn,
        TRY_CAST(pitchesPerInning AS float) AS pitchesPerInning,
        TRY_CAST(strikeoutWalkRatio AS float) AS strikeoutWalkRatio,
        TRY_CAST(walksPer9Inn AS float) AS walksPer9Inn,
        TRY_CAST(hits AS float) AS hits,
        TRY_CAST(atBats AS float) AS atBats,
        TRY_CAST(homeRuns AS float) AS homeRuns,
        TRY_CAST(gamesFinished AS float) AS gamesFinished,
        TRY_CAST(holds AS float) AS holds,
        TRY_CAST(saves AS float) AS saves,
        TRY_CAST(blownSaves AS float) AS blownSaves,
        TRY_CAST(inheritedRunners AS float) AS inheritedRunners,
        TRY_CAST(inheritedRunnersScored AS float) AS inheritedRunnersScored,

        TRY_CAST(hits AS float) / NULLIF(TRY_CAST(atBats AS float), 0) AS batting_avg_allowed,
        TRY_CAST(hits AS float) + TRY_CAST(baseOnBalls AS float) AS baserunners_allowed,

        CASE
            WHEN TRY_CAST(gamesStarted AS float) = 1 THEN 1.0
            ELSE 0.0
        END AS is_starter,

        CASE
            WHEN TRY_CAST(gamesStarted AS float) = 0 THEN 1.0
            ELSE 0.0
        END AS is_reliever

    FROM mlb.dbo.fact_player_pitching_gamelogs
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
        team_id,
        team_name,
        gamesStarted,
        strikeOuts,

        -- days rest
        DATEDIFF(
            DAY,
            LAG(game_date, 1) OVER (
                PARTITION BY player_id, season
                ORDER BY game_date, gamePk
            ),
            game_date
        ) AS days_since_last_appearance,

        -- =========================
        -- SIMPLE AVG LAST 3
        -- =========================
        AVG(strikeOuts) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_k_last_3,

        AVG(inningsPitched) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_ip_last_3,

        AVG(battersFaced) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_bf_last_3,

        AVG(numberOfPitches) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_pitches_last_3,

        AVG(strikePercentage) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_strike_pct_last_3,

        AVG(strikes) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_strikes_last_3,

        AVG(whip) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_whip_last_3,

        AVG(baseOnBalls) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_bb_last_3,

        AVG(era) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_era_last_3,

        AVG(outs) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_outs_last_3,

        AVG(strikeoutsPer9Inn) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_k9_last_3,

        AVG(pitchesPerInning) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_pitches_per_inning_last_3,

        AVG(strikeoutWalkRatio) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_kbb_last_3,

        AVG(walksPer9Inn) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_bb9_last_3,

        AVG(hits) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_hits_last_3,

        AVG(atBats) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_ab_last_3,

        AVG(homeRuns) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_hr_last_3,

        AVG(batting_avg_allowed) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_baa_last_3,

        AVG(baserunners_allowed) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_baserunners_last_3,

        AVG(is_starter) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_is_starter_last_3,

        AVG(gamesStarted) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_games_started_last_3,

        AVG(gamesFinished) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_games_finished_last_3,

        AVG(holds) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_holds_last_3,

        AVG(saves) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_saves_last_3,

        AVG(blownSaves) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_blown_saves_last_3,

        AVG(inheritedRunners) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_inherited_runners_last_3,

        AVG(inheritedRunnersScored) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS avg_inherited_runners_scored_last_3,

        SUM(numberOfPitches) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS sum_pitches_last_3,

        SUM(battersFaced) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS sum_bf_last_3,

        SUM(outs) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS sum_outs_last_3,

        AVG(CASE WHEN inningsPitched >= 5 THEN 1.0 ELSE 0.0 END) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS pct_5plus_ip_last_3,

        AVG(CASE WHEN inningsPitched >= 6 THEN 1.0 ELSE 0.0 END) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS pct_6plus_ip_last_3,

        AVG(CASE WHEN strikeOuts >= 5 THEN 1.0 ELSE 0.0 END) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS pct_5plus_k_last_3,

        AVG(CASE WHEN strikeOuts >= 7 THEN 1.0 ELSE 0.0 END) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS pct_7plus_k_last_3,

        -- =========================
        -- SIMPLE AVG LAST 5
        -- =========================
        AVG(strikeOuts) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_k_last_5,

        AVG(inningsPitched) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_ip_last_5,

        AVG(battersFaced) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_bf_last_5,

        AVG(numberOfPitches) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_pitches_last_5,

        AVG(strikePercentage) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_strike_pct_last_5,

        AVG(strikes) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_strikes_last_5,

        AVG(whip) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_whip_last_5,

        AVG(baseOnBalls) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_bb_last_5,

        AVG(era) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_era_last_5,

        AVG(outs) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_outs_last_5,

        AVG(strikeoutsPer9Inn) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_k9_last_5,

        AVG(pitchesPerInning) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_pitches_per_inning_last_5,

        AVG(strikeoutWalkRatio) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_kbb_last_5,

        AVG(walksPer9Inn) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_bb9_last_5,

        AVG(hits) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_hits_last_5,

        AVG(atBats) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_ab_last_5,

        AVG(homeRuns) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_hr_last_5,

        AVG(batting_avg_allowed) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_baa_last_5,

        AVG(baserunners_allowed) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_baserunners_last_5,

        AVG(is_starter) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_is_starter_last_5,

        AVG(gamesStarted) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_games_started_last_5,

        AVG(gamesFinished) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_games_finished_last_5,

        AVG(holds) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_holds_last_5,

        AVG(saves) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_saves_last_5,

        AVG(blownSaves) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_blown_saves_last_5,

        AVG(inheritedRunners) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_inherited_runners_last_5,

        AVG(inheritedRunnersScored) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS avg_inherited_runners_scored_last_5,

        SUM(numberOfPitches) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS sum_pitches_last_5,

        SUM(battersFaced) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS sum_bf_last_5,

        SUM(outs) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS sum_outs_last_5,

        AVG(CASE WHEN inningsPitched >= 5 THEN 1.0 ELSE 0.0 END) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS pct_5plus_ip_last_5,

        AVG(CASE WHEN inningsPitched >= 6 THEN 1.0 ELSE 0.0 END) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS pct_6plus_ip_last_5,

        AVG(CASE WHEN strikeOuts >= 5 THEN 1.0 ELSE 0.0 END) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS pct_5plus_k_last_5,

        AVG(CASE WHEN strikeOuts >= 7 THEN 1.0 ELSE 0.0 END) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS pct_7plus_k_last_5,

        -- =========================
        -- SIMPLE AVG LAST 10
        -- =========================
        AVG(strikeOuts) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_k_last_10,

        AVG(inningsPitched) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_ip_last_10,

        AVG(battersFaced) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_bf_last_10,

        AVG(numberOfPitches) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_pitches_last_10,

        AVG(strikePercentage) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_strike_pct_last_10,

        AVG(strikes) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_strikes_last_10,

        AVG(whip) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_whip_last_10,

        AVG(baseOnBalls) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_bb_last_10,

        AVG(era) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_era_last_10,

        AVG(outs) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_outs_last_10,

        AVG(strikeoutsPer9Inn) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_k9_last_10,

        AVG(pitchesPerInning) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_pitches_per_inning_last_10,

        AVG(strikeoutWalkRatio) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_kbb_last_10,

        AVG(walksPer9Inn) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_bb9_last_10,

        AVG(hits) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_hits_last_10,

        AVG(atBats) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_ab_last_10,

        AVG(homeRuns) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_hr_last_10,

        AVG(batting_avg_allowed) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_baa_last_10,

        AVG(baserunners_allowed) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_baserunners_last_10,

        AVG(is_starter) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_is_starter_last_10,

        AVG(gamesStarted) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_games_started_last_10,

        AVG(gamesFinished) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_games_finished_last_10,

        AVG(holds) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_holds_last_10,

        AVG(saves) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_saves_last_10,

        AVG(blownSaves) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_blown_saves_last_10,

        AVG(inheritedRunners) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_inherited_runners_last_10,

        AVG(inheritedRunnersScored) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS avg_inherited_runners_scored_last_10,

        SUM(numberOfPitches) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS sum_pitches_last_10,

        SUM(battersFaced) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS sum_bf_last_10,

        SUM(outs) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS sum_outs_last_10,

        AVG(CASE WHEN inningsPitched >= 5 THEN 1.0 ELSE 0.0 END) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS pct_5plus_ip_last_10,

        AVG(CASE WHEN inningsPitched >= 6 THEN 1.0 ELSE 0.0 END) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS pct_6plus_ip_last_10,

        AVG(CASE WHEN strikeOuts >= 5 THEN 1.0 ELSE 0.0 END) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS pct_5plus_k_last_10,

        AVG(CASE WHEN strikeOuts >= 7 THEN 1.0 ELSE 0.0 END) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
            ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING
        ) AS pct_7plus_k_last_10,

        -- =========================
        -- WEIGHTED LAST 3
        -- weights: 0.50, 0.30, 0.20
        -- =========================
        (
            0.50 * LAG(strikeOuts, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            0.30 * LAG(strikeOuts, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            0.20 * LAG(strikeOuts, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk)
        ) /
        NULLIF(
            (CASE WHEN LAG(strikeOuts, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 0.50 ELSE 0 END) +
            (CASE WHEN LAG(strikeOuts, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 0.30 ELSE 0 END) +
            (CASE WHEN LAG(strikeOuts, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 0.20 ELSE 0 END),
            0
        ) AS weighted_k_last_3,

        (
            0.50 * LAG(inningsPitched, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            0.30 * LAG(inningsPitched, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            0.20 * LAG(inningsPitched, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk)
        ) /
        NULLIF(
            (CASE WHEN LAG(inningsPitched, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 0.50 ELSE 0 END) +
            (CASE WHEN LAG(inningsPitched, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 0.30 ELSE 0 END) +
            (CASE WHEN LAG(inningsPitched, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 0.20 ELSE 0 END),
            0
        ) AS weighted_ip_last_3,

        (
            0.50 * LAG(battersFaced, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            0.30 * LAG(battersFaced, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            0.20 * LAG(battersFaced, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk)
        ) /
        NULLIF(
            (CASE WHEN LAG(battersFaced, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 0.50 ELSE 0 END) +
            (CASE WHEN LAG(battersFaced, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 0.30 ELSE 0 END) +
            (CASE WHEN LAG(battersFaced, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 0.20 ELSE 0 END),
            0
        ) AS weighted_bf_last_3,

        (
            0.50 * LAG(numberOfPitches, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            0.30 * LAG(numberOfPitches, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            0.20 * LAG(numberOfPitches, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk)
        ) /
        NULLIF(
            (CASE WHEN LAG(numberOfPitches, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 0.50 ELSE 0 END) +
            (CASE WHEN LAG(numberOfPitches, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 0.30 ELSE 0 END) +
            (CASE WHEN LAG(numberOfPitches, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 0.20 ELSE 0 END),
            0
        ) AS weighted_pitches_last_3,

        (
            0.50 * LAG(strikePercentage, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            0.30 * LAG(strikePercentage, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            0.20 * LAG(strikePercentage, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk)
        ) /
        NULLIF(
            (CASE WHEN LAG(strikePercentage, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 0.50 ELSE 0 END) +
            (CASE WHEN LAG(strikePercentage, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 0.30 ELSE 0 END) +
            (CASE WHEN LAG(strikePercentage, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 0.20 ELSE 0 END),
            0
        ) AS weighted_strike_pct_last_3,

        (
            0.50 * LAG(strikeoutsPer9Inn, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            0.30 * LAG(strikeoutsPer9Inn, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            0.20 * LAG(strikeoutsPer9Inn, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk)
        ) /
        NULLIF(
            (CASE WHEN LAG(strikeoutsPer9Inn, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 0.50 ELSE 0 END) +
            (CASE WHEN LAG(strikeoutsPer9Inn, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 0.30 ELSE 0 END) +
            (CASE WHEN LAG(strikeoutsPer9Inn, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 0.20 ELSE 0 END),
            0
        ) AS weighted_k9_last_3,

        (
            0.50 * LAG(baseOnBalls, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            0.30 * LAG(baseOnBalls, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            0.20 * LAG(baseOnBalls, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk)
        ) /
        NULLIF(
            (CASE WHEN LAG(baseOnBalls, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 0.50 ELSE 0 END) +
            (CASE WHEN LAG(baseOnBalls, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 0.30 ELSE 0 END) +
            (CASE WHEN LAG(baseOnBalls, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 0.20 ELSE 0 END),
            0
        ) AS weighted_bb_last_3,

        (
            0.50 * LAG(whip, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            0.30 * LAG(whip, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            0.20 * LAG(whip, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk)
        ) /
        NULLIF(
            (CASE WHEN LAG(whip, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 0.50 ELSE 0 END) +
            (CASE WHEN LAG(whip, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 0.30 ELSE 0 END) +
            (CASE WHEN LAG(whip, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 0.20 ELSE 0 END),
            0
        ) AS weighted_whip_last_3,

        (
            0.50 * LAG(outs, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            0.30 * LAG(outs, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            0.20 * LAG(outs, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk)
        ) /
        NULLIF(
            (CASE WHEN LAG(outs, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 0.50 ELSE 0 END) +
            (CASE WHEN LAG(outs, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 0.30 ELSE 0 END) +
            (CASE WHEN LAG(outs, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 0.20 ELSE 0 END),
            0
        ) AS weighted_outs_last_3,

        -- =========================
        -- WEIGHTED LAST 5
        -- weights: 5,4,3,2,1
        -- =========================
        (
            5.0 * LAG(strikeOuts, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            4.0 * LAG(strikeOuts, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            3.0 * LAG(strikeOuts, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            2.0 * LAG(strikeOuts, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            1.0 * LAG(strikeOuts, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk)
        ) /
        NULLIF(
            (CASE WHEN LAG(strikeOuts, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 5.0 ELSE 0 END) +
            (CASE WHEN LAG(strikeOuts, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 4.0 ELSE 0 END) +
            (CASE WHEN LAG(strikeOuts, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 3.0 ELSE 0 END) +
            (CASE WHEN LAG(strikeOuts, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 2.0 ELSE 0 END) +
            (CASE WHEN LAG(strikeOuts, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 1.0 ELSE 0 END),
            0
        ) AS weighted_k_last_5,

        (
            5.0 * LAG(inningsPitched, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            4.0 * LAG(inningsPitched, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            3.0 * LAG(inningsPitched, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            2.0 * LAG(inningsPitched, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            1.0 * LAG(inningsPitched, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk)
        ) /
        NULLIF(
            (CASE WHEN LAG(inningsPitched, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 5.0 ELSE 0 END) +
            (CASE WHEN LAG(inningsPitched, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 4.0 ELSE 0 END) +
            (CASE WHEN LAG(inningsPitched, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 3.0 ELSE 0 END) +
            (CASE WHEN LAG(inningsPitched, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 2.0 ELSE 0 END) +
            (CASE WHEN LAG(inningsPitched, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 1.0 ELSE 0 END),
            0
        ) AS weighted_ip_last_5,

        (
            5.0 * LAG(battersFaced, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            4.0 * LAG(battersFaced, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            3.0 * LAG(battersFaced, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            2.0 * LAG(battersFaced, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            1.0 * LAG(battersFaced, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk)
        ) /
        NULLIF(
            (CASE WHEN LAG(battersFaced, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 5.0 ELSE 0 END) +
            (CASE WHEN LAG(battersFaced, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 4.0 ELSE 0 END) +
            (CASE WHEN LAG(battersFaced, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 3.0 ELSE 0 END) +
            (CASE WHEN LAG(battersFaced, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 2.0 ELSE 0 END) +
            (CASE WHEN LAG(battersFaced, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 1.0 ELSE 0 END),
            0
        ) AS weighted_bf_last_5,

        (
            5.0 * LAG(numberOfPitches, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            4.0 * LAG(numberOfPitches, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            3.0 * LAG(numberOfPitches, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            2.0 * LAG(numberOfPitches, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            1.0 * LAG(numberOfPitches, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk)
        ) /
        NULLIF(
            (CASE WHEN LAG(numberOfPitches, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 5.0 ELSE 0 END) +
            (CASE WHEN LAG(numberOfPitches, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 4.0 ELSE 0 END) +
            (CASE WHEN LAG(numberOfPitches, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 3.0 ELSE 0 END) +
            (CASE WHEN LAG(numberOfPitches, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 2.0 ELSE 0 END) +
            (CASE WHEN LAG(numberOfPitches, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 1.0 ELSE 0 END),
            0
        ) AS weighted_pitches_last_5,

        (
            5.0 * LAG(strikePercentage, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            4.0 * LAG(strikePercentage, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            3.0 * LAG(strikePercentage, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            2.0 * LAG(strikePercentage, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            1.0 * LAG(strikePercentage, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk)
        ) /
        NULLIF(
            (CASE WHEN LAG(strikePercentage, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 5.0 ELSE 0 END) +
            (CASE WHEN LAG(strikePercentage, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 4.0 ELSE 0 END) +
            (CASE WHEN LAG(strikePercentage, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 3.0 ELSE 0 END) +
            (CASE WHEN LAG(strikePercentage, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 2.0 ELSE 0 END) +
            (CASE WHEN LAG(strikePercentage, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 1.0 ELSE 0 END),
            0
        ) AS weighted_strike_pct_last_5,

        (
            5.0 * LAG(strikeoutsPer9Inn, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            4.0 * LAG(strikeoutsPer9Inn, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            3.0 * LAG(strikeoutsPer9Inn, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            2.0 * LAG(strikeoutsPer9Inn, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            1.0 * LAG(strikeoutsPer9Inn, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk)
        ) /
        NULLIF(
            (CASE WHEN LAG(strikeoutsPer9Inn, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 5.0 ELSE 0 END) +
            (CASE WHEN LAG(strikeoutsPer9Inn, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 4.0 ELSE 0 END) +
            (CASE WHEN LAG(strikeoutsPer9Inn, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 3.0 ELSE 0 END) +
            (CASE WHEN LAG(strikeoutsPer9Inn, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 2.0 ELSE 0 END) +
            (CASE WHEN LAG(strikeoutsPer9Inn, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 1.0 ELSE 0 END),
            0
        ) AS weighted_k9_last_5,

        (
            5.0 * LAG(baseOnBalls, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            4.0 * LAG(baseOnBalls, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            3.0 * LAG(baseOnBalls, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            2.0 * LAG(baseOnBalls, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            1.0 * LAG(baseOnBalls, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk)
        ) /
        NULLIF(
            (CASE WHEN LAG(baseOnBalls, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 5.0 ELSE 0 END) +
            (CASE WHEN LAG(baseOnBalls, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 4.0 ELSE 0 END) +
            (CASE WHEN LAG(baseOnBalls, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 3.0 ELSE 0 END) +
            (CASE WHEN LAG(baseOnBalls, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 2.0 ELSE 0 END) +
            (CASE WHEN LAG(baseOnBalls, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 1.0 ELSE 0 END),
            0
        ) AS weighted_bb_last_5,

        (
            5.0 * LAG(whip, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            4.0 * LAG(whip, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            3.0 * LAG(whip, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            2.0 * LAG(whip, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            1.0 * LAG(whip, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk)
        ) /
        NULLIF(
            (CASE WHEN LAG(whip, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 5.0 ELSE 0 END) +
            (CASE WHEN LAG(whip, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 4.0 ELSE 0 END) +
            (CASE WHEN LAG(whip, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 3.0 ELSE 0 END) +
            (CASE WHEN LAG(whip, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 2.0 ELSE 0 END) +
            (CASE WHEN LAG(whip, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 1.0 ELSE 0 END),
            0
        ) AS weighted_whip_last_5,

        (
            5.0 * LAG(outs, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            4.0 * LAG(outs, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            3.0 * LAG(outs, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            2.0 * LAG(outs, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
            1.0 * LAG(outs, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk)
        ) /
        NULLIF(
            (CASE WHEN LAG(outs, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 5.0 ELSE 0 END) +
            (CASE WHEN LAG(outs, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 4.0 ELSE 0 END) +
            (CASE WHEN LAG(outs, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 3.0 ELSE 0 END) +
            (CASE WHEN LAG(outs, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 2.0 ELSE 0 END) +
            (CASE WHEN LAG(outs, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 1.0 ELSE 0 END),
            0
        ) AS weighted_outs_last_5,

        -- =========================
        -- WEIGHTED LAST 10
        -- weights: 10,9,8,7,6,5,4,3,2,1
        -- =========================
        (
            10.0 * LAG(strikeOuts, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             9.0 * LAG(strikeOuts, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             8.0 * LAG(strikeOuts, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             7.0 * LAG(strikeOuts, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             6.0 * LAG(strikeOuts, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             5.0 * LAG(strikeOuts, 6) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             4.0 * LAG(strikeOuts, 7) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             3.0 * LAG(strikeOuts, 8) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             2.0 * LAG(strikeOuts, 9) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             1.0 * LAG(strikeOuts,10) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk)
        ) /
        NULLIF(
            (CASE WHEN LAG(strikeOuts, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 10.0 ELSE 0 END) +
            (CASE WHEN LAG(strikeOuts, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  9.0 ELSE 0 END) +
            (CASE WHEN LAG(strikeOuts, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  8.0 ELSE 0 END) +
            (CASE WHEN LAG(strikeOuts, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  7.0 ELSE 0 END) +
            (CASE WHEN LAG(strikeOuts, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  6.0 ELSE 0 END) +
            (CASE WHEN LAG(strikeOuts, 6) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  5.0 ELSE 0 END) +
            (CASE WHEN LAG(strikeOuts, 7) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  4.0 ELSE 0 END) +
            (CASE WHEN LAG(strikeOuts, 8) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  3.0 ELSE 0 END) +
            (CASE WHEN LAG(strikeOuts, 9) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  2.0 ELSE 0 END) +
            (CASE WHEN LAG(strikeOuts,10) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  1.0 ELSE 0 END),
            0
        ) AS weighted_k_last_10,

        (
            10.0 * LAG(inningsPitched, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             9.0 * LAG(inningsPitched, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             8.0 * LAG(inningsPitched, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             7.0 * LAG(inningsPitched, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             6.0 * LAG(inningsPitched, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             5.0 * LAG(inningsPitched, 6) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             4.0 * LAG(inningsPitched, 7) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             3.0 * LAG(inningsPitched, 8) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             2.0 * LAG(inningsPitched, 9) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             1.0 * LAG(inningsPitched,10) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk)
        ) /
        NULLIF(
            (CASE WHEN LAG(inningsPitched, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 10.0 ELSE 0 END) +
            (CASE WHEN LAG(inningsPitched, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  9.0 ELSE 0 END) +
            (CASE WHEN LAG(inningsPitched, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  8.0 ELSE 0 END) +
            (CASE WHEN LAG(inningsPitched, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  7.0 ELSE 0 END) +
            (CASE WHEN LAG(inningsPitched, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  6.0 ELSE 0 END) +
            (CASE WHEN LAG(inningsPitched, 6) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  5.0 ELSE 0 END) +
            (CASE WHEN LAG(inningsPitched, 7) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  4.0 ELSE 0 END) +
            (CASE WHEN LAG(inningsPitched, 8) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  3.0 ELSE 0 END) +
            (CASE WHEN LAG(inningsPitched, 9) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  2.0 ELSE 0 END) +
            (CASE WHEN LAG(inningsPitched,10) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  1.0 ELSE 0 END),
            0
        ) AS weighted_ip_last_10,

        (
            10.0 * LAG(battersFaced, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             9.0 * LAG(battersFaced, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             8.0 * LAG(battersFaced, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             7.0 * LAG(battersFaced, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             6.0 * LAG(battersFaced, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             5.0 * LAG(battersFaced, 6) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             4.0 * LAG(battersFaced, 7) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             3.0 * LAG(battersFaced, 8) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             2.0 * LAG(battersFaced, 9) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             1.0 * LAG(battersFaced,10) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk)
        ) /
        NULLIF(
            (CASE WHEN LAG(battersFaced, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 10.0 ELSE 0 END) +
            (CASE WHEN LAG(battersFaced, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  9.0 ELSE 0 END) +
            (CASE WHEN LAG(battersFaced, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  8.0 ELSE 0 END) +
            (CASE WHEN LAG(battersFaced, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  7.0 ELSE 0 END) +
            (CASE WHEN LAG(battersFaced, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  6.0 ELSE 0 END) +
            (CASE WHEN LAG(battersFaced, 6) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  5.0 ELSE 0 END) +
            (CASE WHEN LAG(battersFaced, 7) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  4.0 ELSE 0 END) +
            (CASE WHEN LAG(battersFaced, 8) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  3.0 ELSE 0 END) +
            (CASE WHEN LAG(battersFaced, 9) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  2.0 ELSE 0 END) +
            (CASE WHEN LAG(battersFaced,10) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  1.0 ELSE 0 END),
            0
        ) AS weighted_bf_last_10,

        (
            10.0 * LAG(numberOfPitches, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             9.0 * LAG(numberOfPitches, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             8.0 * LAG(numberOfPitches, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             7.0 * LAG(numberOfPitches, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             6.0 * LAG(numberOfPitches, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             5.0 * LAG(numberOfPitches, 6) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             4.0 * LAG(numberOfPitches, 7) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             3.0 * LAG(numberOfPitches, 8) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             2.0 * LAG(numberOfPitches, 9) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             1.0 * LAG(numberOfPitches,10) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk)
        ) /
        NULLIF(
            (CASE WHEN LAG(numberOfPitches, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 10.0 ELSE 0 END) +
            (CASE WHEN LAG(numberOfPitches, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  9.0 ELSE 0 END) +
            (CASE WHEN LAG(numberOfPitches, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  8.0 ELSE 0 END) +
            (CASE WHEN LAG(numberOfPitches, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  7.0 ELSE 0 END) +
            (CASE WHEN LAG(numberOfPitches, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  6.0 ELSE 0 END) +
            (CASE WHEN LAG(numberOfPitches, 6) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  5.0 ELSE 0 END) +
            (CASE WHEN LAG(numberOfPitches, 7) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  4.0 ELSE 0 END) +
            (CASE WHEN LAG(numberOfPitches, 8) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  3.0 ELSE 0 END) +
            (CASE WHEN LAG(numberOfPitches, 9) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  2.0 ELSE 0 END) +
            (CASE WHEN LAG(numberOfPitches,10) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  1.0 ELSE 0 END),
            0
        ) AS weighted_pitches_last_10,

        (
            10.0 * LAG(strikePercentage, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             9.0 * LAG(strikePercentage, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             8.0 * LAG(strikePercentage, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             7.0 * LAG(strikePercentage, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             6.0 * LAG(strikePercentage, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             5.0 * LAG(strikePercentage, 6) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             4.0 * LAG(strikePercentage, 7) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             3.0 * LAG(strikePercentage, 8) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             2.0 * LAG(strikePercentage, 9) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             1.0 * LAG(strikePercentage,10) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk)
        ) /
        NULLIF(
            (CASE WHEN LAG(strikePercentage, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 10.0 ELSE 0 END) +
            (CASE WHEN LAG(strikePercentage, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  9.0 ELSE 0 END) +
            (CASE WHEN LAG(strikePercentage, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  8.0 ELSE 0 END) +
            (CASE WHEN LAG(strikePercentage, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  7.0 ELSE 0 END) +
            (CASE WHEN LAG(strikePercentage, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  6.0 ELSE 0 END) +
            (CASE WHEN LAG(strikePercentage, 6) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  5.0 ELSE 0 END) +
            (CASE WHEN LAG(strikePercentage, 7) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  4.0 ELSE 0 END) +
            (CASE WHEN LAG(strikePercentage, 8) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  3.0 ELSE 0 END) +
            (CASE WHEN LAG(strikePercentage, 9) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  2.0 ELSE 0 END) +
            (CASE WHEN LAG(strikePercentage,10) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  1.0 ELSE 0 END),
            0
        ) AS weighted_strike_pct_last_10,

        (
            10.0 * LAG(strikeoutsPer9Inn, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             9.0 * LAG(strikeoutsPer9Inn, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             8.0 * LAG(strikeoutsPer9Inn, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             7.0 * LAG(strikeoutsPer9Inn, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             6.0 * LAG(strikeoutsPer9Inn, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             5.0 * LAG(strikeoutsPer9Inn, 6) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             4.0 * LAG(strikeoutsPer9Inn, 7) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             3.0 * LAG(strikeoutsPer9Inn, 8) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             2.0 * LAG(strikeoutsPer9Inn, 9) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             1.0 * LAG(strikeoutsPer9Inn,10) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk)
        ) /
        NULLIF(
            (CASE WHEN LAG(strikeoutsPer9Inn, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 10.0 ELSE 0 END) +
            (CASE WHEN LAG(strikeoutsPer9Inn, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  9.0 ELSE 0 END) +
            (CASE WHEN LAG(strikeoutsPer9Inn, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  8.0 ELSE 0 END) +
            (CASE WHEN LAG(strikeoutsPer9Inn, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  7.0 ELSE 0 END) +
            (CASE WHEN LAG(strikeoutsPer9Inn, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  6.0 ELSE 0 END) +
            (CASE WHEN LAG(strikeoutsPer9Inn, 6) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  5.0 ELSE 0 END) +
            (CASE WHEN LAG(strikeoutsPer9Inn, 7) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  4.0 ELSE 0 END) +
            (CASE WHEN LAG(strikeoutsPer9Inn, 8) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  3.0 ELSE 0 END) +
            (CASE WHEN LAG(strikeoutsPer9Inn, 9) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  2.0 ELSE 0 END) +
            (CASE WHEN LAG(strikeoutsPer9Inn,10) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  1.0 ELSE 0 END),
            0
        ) AS weighted_k9_last_10,

        (
            10.0 * LAG(baseOnBalls, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             9.0 * LAG(baseOnBalls, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             8.0 * LAG(baseOnBalls, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             7.0 * LAG(baseOnBalls, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             6.0 * LAG(baseOnBalls, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             5.0 * LAG(baseOnBalls, 6) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             4.0 * LAG(baseOnBalls, 7) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             3.0 * LAG(baseOnBalls, 8) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             2.0 * LAG(baseOnBalls, 9) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             1.0 * LAG(baseOnBalls,10) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk)
        ) /
        NULLIF(
            (CASE WHEN LAG(baseOnBalls, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 10.0 ELSE 0 END) +
            (CASE WHEN LAG(baseOnBalls, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  9.0 ELSE 0 END) +
            (CASE WHEN LAG(baseOnBalls, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  8.0 ELSE 0 END) +
            (CASE WHEN LAG(baseOnBalls, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  7.0 ELSE 0 END) +
            (CASE WHEN LAG(baseOnBalls, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  6.0 ELSE 0 END) +
            (CASE WHEN LAG(baseOnBalls, 6) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  5.0 ELSE 0 END) +
            (CASE WHEN LAG(baseOnBalls, 7) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  4.0 ELSE 0 END) +
            (CASE WHEN LAG(baseOnBalls, 8) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  3.0 ELSE 0 END) +
            (CASE WHEN LAG(baseOnBalls, 9) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  2.0 ELSE 0 END) +
            (CASE WHEN LAG(baseOnBalls,10) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  1.0 ELSE 0 END),
            0
        ) AS weighted_bb_last_10,

        (
            10.0 * LAG(whip, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             9.0 * LAG(whip, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             8.0 * LAG(whip, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             7.0 * LAG(whip, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             6.0 * LAG(whip, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             5.0 * LAG(whip, 6) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             4.0 * LAG(whip, 7) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             3.0 * LAG(whip, 8) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             2.0 * LAG(whip, 9) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             1.0 * LAG(whip,10) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk)
        ) /
        NULLIF(
            (CASE WHEN LAG(whip, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 10.0 ELSE 0 END) +
            (CASE WHEN LAG(whip, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  9.0 ELSE 0 END) +
            (CASE WHEN LAG(whip, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  8.0 ELSE 0 END) +
            (CASE WHEN LAG(whip, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  7.0 ELSE 0 END) +
            (CASE WHEN LAG(whip, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  6.0 ELSE 0 END) +
            (CASE WHEN LAG(whip, 6) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  5.0 ELSE 0 END) +
            (CASE WHEN LAG(whip, 7) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  4.0 ELSE 0 END) +
            (CASE WHEN LAG(whip, 8) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  3.0 ELSE 0 END) +
            (CASE WHEN LAG(whip, 9) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  2.0 ELSE 0 END) +
            (CASE WHEN LAG(whip,10) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  1.0 ELSE 0 END),
            0
        ) AS weighted_whip_last_10,

        (
            10.0 * LAG(outs, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             9.0 * LAG(outs, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             8.0 * LAG(outs, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             7.0 * LAG(outs, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             6.0 * LAG(outs, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             5.0 * LAG(outs, 6) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             4.0 * LAG(outs, 7) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             3.0 * LAG(outs, 8) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             2.0 * LAG(outs, 9) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) +
             1.0 * LAG(outs,10) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk)
        ) /
        NULLIF(
            (CASE WHEN LAG(outs, 1) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN 10.0 ELSE 0 END) +
            (CASE WHEN LAG(outs, 2) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  9.0 ELSE 0 END) +
            (CASE WHEN LAG(outs, 3) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  8.0 ELSE 0 END) +
            (CASE WHEN LAG(outs, 4) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  7.0 ELSE 0 END) +
            (CASE WHEN LAG(outs, 5) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  6.0 ELSE 0 END) +
            (CASE WHEN LAG(outs, 6) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  5.0 ELSE 0 END) +
            (CASE WHEN LAG(outs, 7) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  4.0 ELSE 0 END) +
            (CASE WHEN LAG(outs, 8) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  3.0 ELSE 0 END) +
            (CASE WHEN LAG(outs, 9) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  2.0 ELSE 0 END) +
            (CASE WHEN LAG(outs,10) OVER (PARTITION BY player_id, season ORDER BY game_date, gamePk) IS NOT NULL THEN  1.0 ELSE 0 END),
            0
        ) AS weighted_outs_last_10,

        -- previous outing
        LAG(strikeOuts, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
        ) AS prev_k,

        LAG(inningsPitched, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
        ) AS prev_ip,

        LAG(battersFaced, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
        ) AS prev_bf,

        LAG(numberOfPitches, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
        ) AS prev_pitches,

        LAG(strikeoutsPer9Inn, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
        ) AS prev_k9,

        LAG(outs, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
        ) AS prev_outs,

        LAG(strikePercentage, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
        ) AS prev_strike_pct,

        LAG(baseOnBalls, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
        ) AS prev_bb,

        LAG(whip, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
        ) AS prev_whip,

        LAG(hits, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
        ) AS prev_hits,

        LAG(homeRuns, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
        ) AS prev_hr,

        LAG(gamesStarted, 1) OVER (
            PARTITION BY player_id, season
            ORDER BY game_date, gamePk
        ) AS prev_games_started

    FROM base
)

SELECT *
INTO mlb.dbo.fact_pitcher_rolling_features
FROM rolling;
    """

    execute_sql(sql)
    logger.info("Finished building mlb.dbo.fact_pitcher_rolling_features")


def build_pitcher_model_features() -> None:
    logger.info("Building mlb.dbo.fact_pitcher_model_features")

    sql = """
IF OBJECT_ID('mlb.dbo.fact_pitcher_model_features', 'U') IS NOT NULL
    DROP TABLE mlb.dbo.fact_pitcher_model_features;

SELECT
    -- identifiers / target
    p.gamePk,
    p.game_date,
    p.season,
    p.player_id,
    p.player_name,
    p.team_id,
    p.team_name,
    p.gamesStarted,
    p.strikeOuts,

    -- =====================================
    -- PITCHING ROLLING FEATURES (opportunity)
    -- =====================================

    -- rest / usage
    p.days_since_last_appearance,

    -- last 3 simple
    p.avg_k_last_3,
    p.avg_ip_last_3,
    p.avg_bf_last_3,
    p.avg_pitches_last_3,
    p.avg_strike_pct_last_3,
    p.avg_strikes_last_3,
    p.avg_whip_last_3,
    p.avg_bb_last_3,
    p.avg_era_last_3,
    p.avg_outs_last_3,
    p.avg_k9_last_3,
    p.avg_pitches_per_inning_last_3,
    p.avg_kbb_last_3,
    p.avg_bb9_last_3,
    p.avg_hits_last_3,
    p.avg_ab_last_3,
    p.avg_hr_last_3,
    p.avg_baa_last_3,
    p.avg_baserunners_last_3,
    p.avg_is_starter_last_3,
    p.avg_games_started_last_3,
    p.avg_games_finished_last_3,
    p.avg_holds_last_3,
    p.avg_saves_last_3,
    p.avg_blown_saves_last_3,
    p.avg_inherited_runners_last_3,
    p.avg_inherited_runners_scored_last_3,
    p.sum_pitches_last_3,
    p.sum_bf_last_3,
    p.sum_outs_last_3,
    p.pct_5plus_ip_last_3,
    p.pct_6plus_ip_last_3,
    p.pct_5plus_k_last_3,
    p.pct_7plus_k_last_3,

    -- last 5 simple
    p.avg_k_last_5,
    p.avg_ip_last_5,
    p.avg_bf_last_5,
    p.avg_pitches_last_5,
    p.avg_strike_pct_last_5,
    p.avg_strikes_last_5,
    p.avg_whip_last_5,
    p.avg_bb_last_5,
    p.avg_era_last_5,
    p.avg_outs_last_5,
    p.avg_k9_last_5,
    p.avg_pitches_per_inning_last_5,
    p.avg_kbb_last_5,
    p.avg_bb9_last_5,
    p.avg_hits_last_5,
    p.avg_ab_last_5,
    p.avg_hr_last_5,
    p.avg_baa_last_5,
    p.avg_baserunners_last_5,
    p.avg_is_starter_last_5,
    p.avg_games_started_last_5,
    p.avg_games_finished_last_5,
    p.avg_holds_last_5,
    p.avg_saves_last_5,
    p.avg_blown_saves_last_5,
    p.avg_inherited_runners_last_5,
    p.avg_inherited_runners_scored_last_5,
    p.sum_pitches_last_5,
    p.sum_bf_last_5,
    p.sum_outs_last_5,
    p.pct_5plus_ip_last_5,
    p.pct_6plus_ip_last_5,
    p.pct_5plus_k_last_5,
    p.pct_7plus_k_last_5,

    -- last 10 simple
    p.avg_k_last_10,
    p.avg_ip_last_10,
    p.avg_bf_last_10,
    p.avg_pitches_last_10,
    p.avg_strike_pct_last_10,
    p.avg_strikes_last_10,
    p.avg_whip_last_10,
    p.avg_bb_last_10,
    p.avg_era_last_10,
    p.avg_outs_last_10,
    p.avg_k9_last_10,
    p.avg_pitches_per_inning_last_10,
    p.avg_kbb_last_10,
    p.avg_bb9_last_10,
    p.avg_hits_last_10,
    p.avg_ab_last_10,
    p.avg_hr_last_10,
    p.avg_baa_last_10,
    p.avg_baserunners_last_10,
    p.avg_is_starter_last_10,
    p.avg_games_started_last_10,
    p.avg_games_finished_last_10,
    p.avg_holds_last_10,
    p.avg_saves_last_10,
    p.avg_blown_saves_last_10,
    p.avg_inherited_runners_last_10,
    p.avg_inherited_runners_scored_last_10,
    p.sum_pitches_last_10,
    p.sum_bf_last_10,
    p.sum_outs_last_10,
    p.pct_5plus_ip_last_10,
    p.pct_6plus_ip_last_10,
    p.pct_5plus_k_last_10,
    p.pct_7plus_k_last_10,

    -- weighted last 3
    p.weighted_k_last_3,
    p.weighted_ip_last_3,
    p.weighted_bf_last_3,
    p.weighted_pitches_last_3,
    p.weighted_strike_pct_last_3,
    p.weighted_k9_last_3,
    p.weighted_bb_last_3,
    p.weighted_whip_last_3,
    p.weighted_outs_last_3,

    -- weighted last 5
    p.weighted_k_last_5,
    p.weighted_ip_last_5,
    p.weighted_bf_last_5,
    p.weighted_pitches_last_5,
    p.weighted_strike_pct_last_5,
    p.weighted_k9_last_5,
    p.weighted_bb_last_5,
    p.weighted_whip_last_5,
    p.weighted_outs_last_5,

    -- weighted last 10
    p.weighted_k_last_10,
    p.weighted_ip_last_10,
    p.weighted_bf_last_10,
    p.weighted_pitches_last_10,
    p.weighted_strike_pct_last_10,
    p.weighted_k9_last_10,
    p.weighted_bb_last_10,
    p.weighted_whip_last_10,
    p.weighted_outs_last_10,

    -- previous outing
    p.prev_k,
    p.prev_ip,
    p.prev_bf,
    p.prev_pitches,
    p.prev_k9,
    p.prev_outs,
    p.prev_strike_pct,
    p.prev_bb,
    p.prev_whip,
    p.prev_hits,
    p.prev_hr,
    p.prev_games_started,

    -- =====================================
    -- STATCAST ROLLING FEATURES (skill)
    -- =====================================

    -- last 3 simple
    s.avg_sc_pitches_last_3,
    s.avg_whiff_rate_last_3,
    s.avg_called_strike_rate_last_3,
    s.avg_csw_rate_last_3,
    s.avg_sc_strike_rate_last_3,
    s.avg_fps_rate_last_3,
    s.avg_putaway_rate_last_3,
    s.avg_swing_rate_last_3,
    s.avg_chase_rate_last_3,
    s.avg_zone_rate_last_3,
    s.avg_whiff_rate_0_2_last_3,
    s.avg_whiff_rate_1_2_last_3,
    s.avg_whiff_rate_2_2_last_3,
    s.avg_velocity_last_3,
    s.avg_max_velocity_last_3,
    s.avg_spin_rate_last_3,
    s.avg_extension_last_3,
    s.avg_ev_allowed_last_3,
    s.avg_horz_movement_last_3,
    s.avg_vert_movement_last_3,
    s.avg_plate_x_last_3,
    s.avg_plate_z_last_3,
    s.avg_ff_pct_last_3,
    s.avg_si_pct_last_3,
    s.avg_fc_pct_last_3,
    s.avg_sl_pct_last_3,
    s.avg_cu_pct_last_3,
    s.avg_ch_pct_last_3,
    s.avg_fs_pct_last_3,
    s.avg_ff_whiff_rate_last_3,
    s.avg_si_whiff_rate_last_3,
    s.avg_fc_whiff_rate_last_3,
    s.avg_sl_whiff_rate_last_3,
    s.avg_cu_whiff_rate_last_3,
    s.avg_ch_whiff_rate_last_3,
    s.avg_fs_whiff_rate_last_3,
    s.avg_whiff_vs_rhb_last_3,
    s.avg_whiff_vs_lhb_last_3,

    -- last 5 simple
    s.avg_sc_pitches_last_5,
    s.avg_whiff_rate_last_5,
    s.avg_called_strike_rate_last_5,
    s.avg_csw_rate_last_5,
    s.avg_sc_strike_rate_last_5,
    s.avg_fps_rate_last_5,
    s.avg_putaway_rate_last_5,
    s.avg_swing_rate_last_5,
    s.avg_chase_rate_last_5,
    s.avg_zone_rate_last_5,
    s.avg_whiff_rate_0_2_last_5,
    s.avg_whiff_rate_1_2_last_5,
    s.avg_whiff_rate_2_2_last_5,
    s.avg_velocity_last_5,
    s.avg_max_velocity_last_5,
    s.avg_spin_rate_last_5,
    s.avg_extension_last_5,
    s.avg_ev_allowed_last_5,
    s.avg_horz_movement_last_5,
    s.avg_vert_movement_last_5,
    s.avg_plate_x_last_5,
    s.avg_plate_z_last_5,
    s.avg_ff_pct_last_5,
    s.avg_si_pct_last_5,
    s.avg_fc_pct_last_5,
    s.avg_sl_pct_last_5,
    s.avg_cu_pct_last_5,
    s.avg_ch_pct_last_5,
    s.avg_fs_pct_last_5,
    s.avg_ff_whiff_rate_last_5,
    s.avg_si_whiff_rate_last_5,
    s.avg_fc_whiff_rate_last_5,
    s.avg_sl_whiff_rate_last_5,
    s.avg_cu_whiff_rate_last_5,
    s.avg_ch_whiff_rate_last_5,
    s.avg_fs_whiff_rate_last_5,
    s.avg_whiff_vs_rhb_last_5,
    s.avg_whiff_vs_lhb_last_5,

    -- last 10 simple
    s.avg_sc_pitches_last_10,
    s.avg_whiff_rate_last_10,
    s.avg_called_strike_rate_last_10,
    s.avg_csw_rate_last_10,
    s.avg_sc_strike_rate_last_10,
    s.avg_fps_rate_last_10,
    s.avg_putaway_rate_last_10,
    s.avg_swing_rate_last_10,
    s.avg_chase_rate_last_10,
    s.avg_zone_rate_last_10,
    s.avg_whiff_rate_0_2_last_10,
    s.avg_whiff_rate_1_2_last_10,
    s.avg_whiff_rate_2_2_last_10,
    s.avg_velocity_last_10,
    s.avg_max_velocity_last_10,
    s.avg_spin_rate_last_10,
    s.avg_extension_last_10,
    s.avg_ev_allowed_last_10,
    s.avg_horz_movement_last_10,
    s.avg_vert_movement_last_10,
    s.avg_plate_x_last_10,
    s.avg_plate_z_last_10,
    s.avg_ff_pct_last_10,
    s.avg_si_pct_last_10,
    s.avg_fc_pct_last_10,
    s.avg_sl_pct_last_10,
    s.avg_cu_pct_last_10,
    s.avg_ch_pct_last_10,
    s.avg_fs_pct_last_10,
    s.avg_ff_whiff_rate_last_10,
    s.avg_si_whiff_rate_last_10,
    s.avg_fc_whiff_rate_last_10,
    s.avg_sl_whiff_rate_last_10,
    s.avg_cu_whiff_rate_last_10,
    s.avg_ch_whiff_rate_last_10,
    s.avg_fs_whiff_rate_last_10,
    s.avg_whiff_vs_rhb_last_10,
    s.avg_whiff_vs_lhb_last_10,

    -- weighted last 3
    s.weighted_sc_pitches_last_3,
    s.weighted_whiff_rate_last_3,
    s.weighted_csw_rate_last_3,
    s.weighted_sc_strike_rate_last_3,
    s.weighted_velocity_last_3,
    s.weighted_spin_rate_last_3,
    s.weighted_chase_rate_last_3,
    s.weighted_putaway_rate_last_3,

    -- weighted last 5
    s.weighted_sc_pitches_last_5,
    s.weighted_whiff_rate_last_5,
    s.weighted_csw_rate_last_5,
    s.weighted_sc_strike_rate_last_5,
    s.weighted_velocity_last_5,
    s.weighted_spin_rate_last_5,
    s.weighted_chase_rate_last_5,
    s.weighted_putaway_rate_last_5,

    -- weighted last 10
    s.weighted_sc_pitches_last_10,
    s.weighted_whiff_rate_last_10,
    s.weighted_csw_rate_last_10,
    s.weighted_sc_strike_rate_last_10,
    s.weighted_velocity_last_10,
    s.weighted_spin_rate_last_10,
    s.weighted_chase_rate_last_10,
    s.weighted_putaway_rate_last_10,

    -- previous statcast game
    s.prev_whiff_rate,
    s.prev_csw_rate,
    s.prev_velocity,
    s.prev_spin_rate,
    s.prev_chase_rate,
    s.prev_zone_rate,
    s.prev_sl_whiff_rate,
    s.prev_ff_whiff_rate

INTO mlb.dbo.fact_pitcher_model_features
FROM mlb.dbo.fact_pitcher_rolling_features p
LEFT JOIN mlb.dbo.fact_pitcher_statcast_rolling_features s
    ON p.player_id = s.player_id
   AND p.gamePk = s.game_pk
   AND p.season = s.season;
    """

    execute_sql(sql)
    logger.info("Finished building mlb.dbo.fact_pitcher_model_features")


def run_all_pitcher_features() -> None:
    logger.info("Starting pitcher feature pipeline")
    build_pitcher_statcast_game_agg()
    build_pitcher_rolling_features()
    build_pitcher_statcast_rolling_features()
    build_pitcher_model_features()
    logger.info("Finished pitcher feature pipeline")


if __name__ == "__main__":
    run_all_pitcher_features()