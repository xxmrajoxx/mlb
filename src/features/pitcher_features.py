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

        AVG(release_speed) AS avg_velocity,
        MAX(release_speed) AS max_velocity,
        AVG(release_spin_rate) AS avg_spin_rate,
        AVG(release_extension) AS avg_extension,
        AVG(launch_speed) AS avg_exit_velocity_allowed,

        AVG(pfx_x) AS avg_horz_movement,
        AVG(pfx_z) AS avg_vert_movement,
        AVG(plate_x) AS avg_plate_x,
        AVG(plate_z) AS avg_plate_z,

        SUM(CASE WHEN pitch_type = 'FF' THEN 1 ELSE 0 END) AS ff_count,
        SUM(CASE WHEN pitch_type = 'SI' THEN 1 ELSE 0 END) AS si_count,
        SUM(CASE WHEN pitch_type = 'FC' THEN 1 ELSE 0 END) AS fc_count,
        SUM(CASE WHEN pitch_type = 'SL' THEN 1 ELSE 0 END) AS sl_count,
        SUM(CASE WHEN pitch_type = 'CU' THEN 1 ELSE 0 END) AS cu_count,
        SUM(CASE WHEN pitch_type = 'CH' THEN 1 ELSE 0 END) AS ch_count,
        SUM(CASE WHEN pitch_type = 'FS' THEN 1 ELSE 0 END) AS fs_count,

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

    whiffs * 1.0 / NULLIF(total_pitches, 0) AS whiff_rate,
    called_strikes * 1.0 / NULLIF(total_pitches, 0) AS called_strike_rate,
    (whiffs + called_strikes) * 1.0 / NULLIF(total_pitches, 0) AS csw_rate,
    strike_events * 1.0 / NULLIF(total_pitches, 0) AS strike_rate,
    first_pitch_strikes * 1.0 / NULLIF(total_pitches, 0) AS approx_first_pitch_strike_rate,
    two_strike_whiffs * 1.0 / NULLIF(two_strike_pitches, 0) AS putaway_rate,

    avg_velocity,
    max_velocity,
    avg_spin_rate,
    avg_extension,
    avg_exit_velocity_allowed,

    avg_horz_movement,
    avg_vert_movement,
    avg_plate_x,
    avg_plate_z,

    ff_count,
    si_count,
    fc_count,
    sl_count,
    cu_count,
    ch_count,
    fs_count,

    ff_count * 1.0 / NULLIF(total_pitches, 0) AS ff_pct,
    si_count * 1.0 / NULLIF(total_pitches, 0) AS si_pct,
    fc_count * 1.0 / NULLIF(total_pitches, 0) AS fc_pct,
    sl_count * 1.0 / NULLIF(total_pitches, 0) AS sl_pct,
    cu_count * 1.0 / NULLIF(total_pitches, 0) AS cu_pct,
    ch_count * 1.0 / NULLIF(total_pitches, 0) AS ch_pct,
    fs_count * 1.0 / NULLIF(total_pitches, 0) AS fs_pct,

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


def build_pitcher_rolling_features() -> None:
    logger.info("Building mlb.dbo.fact_pitcher_rolling_features")

    sql = """
    IF OBJECT_ID('mlb.dbo.fact_pitcher_rolling_features', 'U') IS NOT NULL
        DROP TABLE mlb.dbo.fact_pitcher_rolling_features;

    WITH base AS (
        SELECT
            gamePk,
            CAST(game_date AS date) AS game_date,
            season,
            player_id,
            player_name,
            team_id,
            team_name,
            gamesStarted,
            strikeOuts,
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
            TRY_CAST(hits AS float) / NULLIF(TRY_CAST(atBats AS float), 0) AS batting_avg_allowed
        FROM mlb.dbo.fact_player_pitching_gamelogs
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

            AVG(inningsPitched) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_ip_last_3,
            AVG(battersFaced) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_bf_last_3,
            AVG(numberOfPitches) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_pitches_last_3,
            AVG(strikePercentage) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_strike_pct_last_3,
            AVG(strikes) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_strikes_last_3,
            AVG(whip) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_whip_last_3,
            AVG(baseOnBalls) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_bb_last_3,
            AVG(era) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_era_last_3,
            AVG(outs) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_outs_last_3,
            AVG(strikeoutsPer9Inn) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_k9_last_3,
            AVG(pitchesPerInning) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_pitches_per_inning_last_3,
            AVG(strikeoutWalkRatio) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_kbb_last_3,
            AVG(walksPer9Inn) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_bb9_last_3,
            AVG(hits) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_hits_last_3,
            AVG(atBats) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_ab_last_3,
            AVG(homeRuns) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_hr_last_3,
            AVG(batting_avg_allowed) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_baa_last_3,

            AVG(inningsPitched) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
                ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
            ) AS avg_ip_last_5,
            AVG(battersFaced) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
                ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
            ) AS avg_bf_last_5,
            AVG(numberOfPitches) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
                ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
            ) AS avg_pitches_last_5,
            AVG(strikePercentage) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
                ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
            ) AS avg_strike_pct_last_5,
            AVG(strikes) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
                ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
            ) AS avg_strikes_last_5,
            AVG(whip) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
                ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
            ) AS avg_whip_last_5,
            AVG(baseOnBalls) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
                ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
            ) AS avg_bb_last_5,
            AVG(era) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
                ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
            ) AS avg_era_last_5,
            AVG(outs) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
                ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
            ) AS avg_outs_last_5,
            AVG(strikeoutsPer9Inn) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
                ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
            ) AS avg_k9_last_5,
            AVG(pitchesPerInning) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
                ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
            ) AS avg_pitches_per_inning_last_5,
            AVG(strikeoutWalkRatio) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
                ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
            ) AS avg_kbb_last_5,
            AVG(walksPer9Inn) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
                ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
            ) AS avg_bb9_last_5,
            AVG(hits) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
                ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
            ) AS avg_hits_last_5,
            AVG(atBats) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
                ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
            ) AS avg_ab_last_5,
            AVG(homeRuns) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
                ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
            ) AS avg_hr_last_5,
            AVG(batting_avg_allowed) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
                ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
            ) AS avg_baa_last_5,

            LAG(strikeOuts, 1) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
            ) AS prev_k,
            LAG(inningsPitched, 1) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
            ) AS prev_ip,
            LAG(battersFaced, 1) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
            ) AS prev_bf,
            LAG(numberOfPitches, 1) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
            ) AS prev_pitches,
            LAG(strikeoutsPer9Inn, 1) OVER (
                PARTITION BY player_id
                ORDER BY game_date, gamePk
            ) AS prev_k9
        FROM base
    )
    SELECT *
    INTO mlb.dbo.fact_pitcher_rolling_features
    FROM rolling;
    """

    execute_sql(sql)
    logger.info("Finished building mlb.dbo.fact_pitcher_rolling_features")


def build_pitcher_statcast_rolling_features() -> None:
    logger.info("Building mlb.dbo.fact_pitcher_statcast_rolling_features")

    sql = """
    IF OBJECT_ID('mlb.dbo.fact_pitcher_statcast_rolling_features', 'U') IS NOT NULL
        DROP TABLE mlb.dbo.fact_pitcher_statcast_rolling_features;

    WITH base AS (
        SELECT
            game_pk,
            game_date,
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
            TRY_CAST(avg_velocity AS float) AS avg_velocity,
            TRY_CAST(max_velocity AS float) AS max_velocity,
            TRY_CAST(avg_spin_rate AS float) AS avg_spin_rate,
            TRY_CAST(avg_extension AS float) AS avg_extension,
            TRY_CAST(avg_exit_velocity_allowed AS float) AS avg_exit_velocity_allowed,
            TRY_CAST(ff_pct AS float) AS ff_pct,
            TRY_CAST(si_pct AS float) AS si_pct,
            TRY_CAST(fc_pct AS float) AS fc_pct,
            TRY_CAST(sl_pct AS float) AS sl_pct,
            TRY_CAST(cu_pct AS float) AS cu_pct,
            TRY_CAST(ch_pct AS float) AS ch_pct,
            TRY_CAST(fs_pct AS float) AS fs_pct,
            TRY_CAST(whiff_rate_vs_rhb AS float) AS whiff_rate_vs_rhb,
            TRY_CAST(whiff_rate_vs_lhb AS float) AS whiff_rate_vs_lhb
        FROM mlb.dbo.fact_pitcher_statcast_game_agg
    ),
    rolling AS (
        SELECT
            game_pk,
            game_date,
            player_id,
            player_name,
            team_id,

            AVG(total_pitches) OVER (
                PARTITION BY player_id
                ORDER BY game_date, game_pk
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_sc_pitches_last_3,
            AVG(whiff_rate) OVER (
                PARTITION BY player_id
                ORDER BY game_date, game_pk
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_whiff_rate_last_3,
            AVG(called_strike_rate) OVER (
                PARTITION BY player_id
                ORDER BY game_date, game_pk
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_called_strike_rate_last_3,
            AVG(csw_rate) OVER (
                PARTITION BY player_id
                ORDER BY game_date, game_pk
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_csw_rate_last_3,
            AVG(strike_rate) OVER (
                PARTITION BY player_id
                ORDER BY game_date, game_pk
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_sc_strike_rate_last_3,
            AVG(approx_first_pitch_strike_rate) OVER (
                PARTITION BY player_id
                ORDER BY game_date, game_pk
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_fps_rate_last_3,
            AVG(putaway_rate) OVER (
                PARTITION BY player_id
                ORDER BY game_date, game_pk
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_putaway_rate_last_3,
            AVG(avg_velocity) OVER (
                PARTITION BY player_id
                ORDER BY game_date, game_pk
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_velocity_last_3,
            AVG(max_velocity) OVER (
                PARTITION BY player_id
                ORDER BY game_date, game_pk
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_max_velocity_last_3,
            AVG(avg_spin_rate) OVER (
                PARTITION BY player_id
                ORDER BY game_date, game_pk
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_spin_rate_last_3,
            AVG(avg_extension) OVER (
                PARTITION BY player_id
                ORDER BY game_date, game_pk
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_extension_last_3,
            AVG(avg_exit_velocity_allowed) OVER (
                PARTITION BY player_id
                ORDER BY game_date, game_pk
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_ev_allowed_last_3,
            AVG(ff_pct) OVER (
                PARTITION BY player_id
                ORDER BY game_date, game_pk
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_ff_pct_last_3,
            AVG(sl_pct) OVER (
                PARTITION BY player_id
                ORDER BY game_date, game_pk
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_sl_pct_last_3,
            AVG(cu_pct) OVER (
                PARTITION BY player_id
                ORDER BY game_date, game_pk
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_cu_pct_last_3,
            AVG(whiff_rate_vs_rhb) OVER (
                PARTITION BY player_id
                ORDER BY game_date, game_pk
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_whiff_vs_rhb_last_3,
            AVG(whiff_rate_vs_lhb) OVER (
                PARTITION BY player_id
                ORDER BY game_date, game_pk
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS avg_whiff_vs_lhb_last_3,

            AVG(total_pitches) OVER (
                PARTITION BY player_id
                ORDER BY game_date, game_pk
                ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
            ) AS avg_sc_pitches_last_5,
            AVG(whiff_rate) OVER (
                PARTITION BY player_id
                ORDER BY game_date, game_pk
                ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
            ) AS avg_whiff_rate_last_5,
            AVG(csw_rate) OVER (
                PARTITION BY player_id
                ORDER BY game_date, game_pk
                ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
            ) AS avg_csw_rate_last_5,
            AVG(avg_velocity) OVER (
                PARTITION BY player_id
                ORDER BY game_date, game_pk
                ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
            ) AS avg_velocity_last_5,
            AVG(avg_spin_rate) OVER (
                PARTITION BY player_id
                ORDER BY game_date, game_pk
                ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
            ) AS avg_spin_rate_last_5,
            AVG(putaway_rate) OVER (
                PARTITION BY player_id
                ORDER BY game_date, game_pk
                ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
            ) AS avg_putaway_rate_last_5,
            AVG(avg_exit_velocity_allowed) OVER (
                PARTITION BY player_id
                ORDER BY game_date, game_pk
                ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
            ) AS avg_ev_allowed_last_5,

            LAG(whiff_rate, 1) OVER (
                PARTITION BY player_id
                ORDER BY game_date, game_pk
            ) AS prev_whiff_rate,
            LAG(csw_rate, 1) OVER (
                PARTITION BY player_id
                ORDER BY game_date, game_pk
            ) AS prev_csw_rate,
            LAG(avg_velocity, 1) OVER (
                PARTITION BY player_id
                ORDER BY game_date, game_pk
            ) AS prev_velocity,
            LAG(avg_spin_rate, 1) OVER (
                PARTITION BY player_id
                ORDER BY game_date, game_pk
            ) AS prev_spin_rate
        FROM base
    )
    SELECT *
    INTO mlb.dbo.fact_pitcher_statcast_rolling_features
    FROM rolling;
    """

    execute_sql(sql)
    logger.info("Finished building mlb.dbo.fact_pitcher_statcast_rolling_features")


def build_pitcher_model_features() -> None:
    logger.info("Building mlb.dbo.fact_pitcher_model_features")

    sql = """
    IF OBJECT_ID('mlb.dbo.fact_pitcher_model_features', 'U') IS NOT NULL
        DROP TABLE mlb.dbo.fact_pitcher_model_features;

    SELECT
        p.gamePk,
        p.game_date,
        p.season,
        p.player_id,
        p.player_name,
        p.team_id,
        p.team_name,
        p.gamesStarted,

        p.strikeOuts,

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

        p.prev_k,
        p.prev_ip,
        p.prev_bf,
        p.prev_pitches,
        p.prev_k9,

        s.avg_sc_pitches_last_3,
        s.avg_whiff_rate_last_3,
        s.avg_called_strike_rate_last_3,
        s.avg_csw_rate_last_3,
        s.avg_sc_strike_rate_last_3,
        s.avg_fps_rate_last_3,
        s.avg_putaway_rate_last_3,
        s.avg_velocity_last_3,
        s.avg_max_velocity_last_3,
        s.avg_spin_rate_last_3,
        s.avg_extension_last_3,
        s.avg_ev_allowed_last_3,
        s.avg_ff_pct_last_3,
        s.avg_sl_pct_last_3,
        s.avg_cu_pct_last_3,
        s.avg_whiff_vs_rhb_last_3,
        s.avg_whiff_vs_lhb_last_3,

        s.avg_sc_pitches_last_5,
        s.avg_whiff_rate_last_5,
        s.avg_csw_rate_last_5,
        s.avg_velocity_last_5,
        s.avg_spin_rate_last_5,
        s.avg_putaway_rate_last_5,
        s.avg_ev_allowed_last_5,

        s.prev_whiff_rate,
        s.prev_csw_rate,
        s.prev_velocity,
        s.prev_spin_rate

    INTO mlb.dbo.fact_pitcher_model_features
    FROM mlb.dbo.fact_pitcher_rolling_features p
    LEFT JOIN mlb.dbo.fact_pitcher_statcast_rolling_features s
        ON p.player_id = s.player_id
       AND p.gamePk = s.game_pk;
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