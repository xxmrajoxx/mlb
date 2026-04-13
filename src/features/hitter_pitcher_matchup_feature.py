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
, hitter_pa_game AS (
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

        COALESCE(
            SUM(CASE
                    WHEN prev.rn = cur.rn - 1 THEN prev.hitter_game_plate_appearances * 3.0
                    WHEN prev.rn = cur.rn - 2 THEN prev.hitter_game_plate_appearances * 2.0
                    WHEN prev.rn = cur.rn - 3 THEN prev.hitter_game_plate_appearances * 1.0
                END)
            / NULLIF(SUM(CASE
                    WHEN prev.rn = cur.rn - 1 THEN 3.0
                    WHEN prev.rn = cur.rn - 2 THEN 2.0
                    WHEN prev.rn = cur.rn - 3 THEN 1.0
                END), 0),
            cur.hitter_game_plate_appearances * 1.0
        ) AS hitter_game_wavg_pa_last_3,

        COALESCE(
            SUM(CASE
                    WHEN prev.rn = cur.rn - 1 THEN prev.hitter_game_strikeouts * 3.0
                    WHEN prev.rn = cur.rn - 2 THEN prev.hitter_game_strikeouts * 2.0
                    WHEN prev.rn = cur.rn - 3 THEN prev.hitter_game_strikeouts * 1.0
                END)
            / NULLIF(SUM(CASE
                    WHEN prev.rn = cur.rn - 1 THEN 3.0
                    WHEN prev.rn = cur.rn - 2 THEN 2.0
                    WHEN prev.rn = cur.rn - 3 THEN 1.0
                END), 0),
            cur.hitter_game_strikeouts * 1.0
        ) AS hitter_game_wavg_strikeouts_last_3,

        COALESCE(
            SUM(CASE
                    WHEN prev.rn = cur.rn - 1 THEN prev.hitter_game_plate_appearances * 5.0
                    WHEN prev.rn = cur.rn - 2 THEN prev.hitter_game_plate_appearances * 4.0
                    WHEN prev.rn = cur.rn - 3 THEN prev.hitter_game_plate_appearances * 3.0
                    WHEN prev.rn = cur.rn - 4 THEN prev.hitter_game_plate_appearances * 2.0
                    WHEN prev.rn = cur.rn - 5 THEN prev.hitter_game_plate_appearances * 1.0
                END)
            / NULLIF(SUM(CASE
                    WHEN prev.rn = cur.rn - 1 THEN 5.0
                    WHEN prev.rn = cur.rn - 2 THEN 4.0
                    WHEN prev.rn = cur.rn - 3 THEN 3.0
                    WHEN prev.rn = cur.rn - 4 THEN 2.0
                    WHEN prev.rn = cur.rn - 5 THEN 1.0
                END), 0),
            cur.hitter_game_plate_appearances * 1.0
        ) AS hitter_game_wavg_pa_last_5,

        COALESCE(
            SUM(CASE
                    WHEN prev.rn = cur.rn - 1 THEN prev.hitter_game_strikeouts * 5.0
                    WHEN prev.rn = cur.rn - 2 THEN prev.hitter_game_strikeouts * 4.0
                    WHEN prev.rn = cur.rn - 3 THEN prev.hitter_game_strikeouts * 3.0
                    WHEN prev.rn = cur.rn - 4 THEN prev.hitter_game_strikeouts * 2.0
                    WHEN prev.rn = cur.rn - 5 THEN prev.hitter_game_strikeouts * 1.0
                END)
            / NULLIF(SUM(CASE
                    WHEN prev.rn = cur.rn - 1 THEN 5.0
                    WHEN prev.rn = cur.rn - 2 THEN 4.0
                    WHEN prev.rn = cur.rn - 3 THEN 3.0
                    WHEN prev.rn = cur.rn - 4 THEN 2.0
                    WHEN prev.rn = cur.rn - 5 THEN 1.0
                END), 0),
            cur.hitter_game_strikeouts * 1.0
        ) AS hitter_game_wavg_strikeouts_last_5,

        COALESCE(
            SUM(CASE
                    WHEN prev.rn = cur.rn - 1  THEN prev.hitter_game_plate_appearances * 10.0
                    WHEN prev.rn = cur.rn - 2  THEN prev.hitter_game_plate_appearances * 9.0
                    WHEN prev.rn = cur.rn - 3  THEN prev.hitter_game_plate_appearances * 8.0
                    WHEN prev.rn = cur.rn - 4  THEN prev.hitter_game_plate_appearances * 7.0
                    WHEN prev.rn = cur.rn - 5  THEN prev.hitter_game_plate_appearances * 6.0
                    WHEN prev.rn = cur.rn - 6  THEN prev.hitter_game_plate_appearances * 5.0
                    WHEN prev.rn = cur.rn - 7  THEN prev.hitter_game_plate_appearances * 4.0
                    WHEN prev.rn = cur.rn - 8  THEN prev.hitter_game_plate_appearances * 3.0
                    WHEN prev.rn = cur.rn - 9  THEN prev.hitter_game_plate_appearances * 2.0
                    WHEN prev.rn = cur.rn - 10 THEN prev.hitter_game_plate_appearances * 1.0
                END)
            / NULLIF(SUM(CASE
                    WHEN prev.rn = cur.rn - 1  THEN 10.0
                    WHEN prev.rn = cur.rn - 2  THEN 9.0
                    WHEN prev.rn = cur.rn - 3  THEN 8.0
                    WHEN prev.rn = cur.rn - 4  THEN 7.0
                    WHEN prev.rn = cur.rn - 5  THEN 6.0
                    WHEN prev.rn = cur.rn - 6  THEN 5.0
                    WHEN prev.rn = cur.rn - 7  THEN 4.0
                    WHEN prev.rn = cur.rn - 8  THEN 3.0
                    WHEN prev.rn = cur.rn - 9  THEN 2.0
                    WHEN prev.rn = cur.rn - 10 THEN 1.0
                END), 0),
            cur.hitter_game_plate_appearances * 1.0
        ) AS hitter_game_wavg_pa_last_10,

        COALESCE(
            SUM(CASE
                    WHEN prev.rn = cur.rn - 1  THEN prev.hitter_game_strikeouts * 10.0
                    WHEN prev.rn = cur.rn - 2  THEN prev.hitter_game_strikeouts * 9.0
                    WHEN prev.rn = cur.rn - 3  THEN prev.hitter_game_strikeouts * 8.0
                    WHEN prev.rn = cur.rn - 4  THEN prev.hitter_game_strikeouts * 7.0
                    WHEN prev.rn = cur.rn - 5  THEN prev.hitter_game_strikeouts * 6.0
                    WHEN prev.rn = cur.rn - 6  THEN prev.hitter_game_strikeouts * 5.0
                    WHEN prev.rn = cur.rn - 7  THEN prev.hitter_game_strikeouts * 4.0
                    WHEN prev.rn = cur.rn - 8  THEN prev.hitter_game_strikeouts * 3.0
                    WHEN prev.rn = cur.rn - 9  THEN prev.hitter_game_strikeouts * 2.0
                    WHEN prev.rn = cur.rn - 10 THEN prev.hitter_game_strikeouts * 1.0
                END)
            / NULLIF(SUM(CASE
                    WHEN prev.rn = cur.rn - 1  THEN 10.0
                    WHEN prev.rn = cur.rn - 2  THEN 9.0
                    WHEN prev.rn = cur.rn - 3  THEN 8.0
                    WHEN prev.rn = cur.rn - 4  THEN 7.0
                    WHEN prev.rn = cur.rn - 5  THEN 6.0
                    WHEN prev.rn = cur.rn - 6  THEN 5.0
                    WHEN prev.rn = cur.rn - 7  THEN 4.0
                    WHEN prev.rn = cur.rn - 8  THEN 3.0
                    WHEN prev.rn = cur.rn - 9  THEN 2.0
                    WHEN prev.rn = cur.rn - 10 THEN 1.0
                END), 0),
            cur.hitter_game_strikeouts * 1.0
        ) AS hitter_game_wavg_strikeouts_last_10
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
       NEW HITTER LINEUP FEATURES
       ========================= */
    hl.hitter_batting_order,
    hl.hitter_lineup_position,
    hl.hitter_lineup_position_name,

    /* =========================
       NEW HITTER GAME PA / K FEATURES
       ========================= */
    hpg.hitter_game_plate_appearances,
    hpg.hitter_game_strikeouts,
    hpg.hitter_game_avg_pa_last_3,
    hpg.hitter_game_avg_pa_last_5,
    hpg.hitter_game_avg_pa_last_10,
    hpg.hitter_game_avg_strikeouts_last_3,
    hpg.hitter_game_avg_strikeouts_last_5,
    hpg.hitter_game_avg_strikeouts_last_10,
    hpg.hitter_game_wavg_pa_last_3,
    hpg.hitter_game_wavg_pa_last_5,
    hpg.hitter_game_wavg_pa_last_10,
    hpg.hitter_game_wavg_strikeouts_last_3,
    hpg.hitter_game_wavg_strikeouts_last_5,
    hpg.hitter_game_wavg_strikeouts_last_10,

        /* =========================
       PITCHER FEATURES
       ========================= */
    p.gamesStarted AS pitcher_gamesStarted,
    p.days_since_last_appearance AS pitcher_days_since_last_appearance,

    -- pitcher rolling last 3 simple
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

    -- pitcher rolling last 5 simple
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

    -- pitcher rolling last 10 simple
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

    -- pitcher weighted rolling last 3
    p.weighted_k_last_3 AS pitcher_weighted_k_last_3,
    p.weighted_ip_last_3 AS pitcher_weighted_ip_last_3,
    p.weighted_bf_last_3 AS pitcher_weighted_bf_last_3,
    p.weighted_pitches_last_3 AS pitcher_weighted_pitches_last_3,
    p.weighted_strike_pct_last_3 AS pitcher_weighted_strike_pct_last_3,
    p.weighted_k9_last_3 AS pitcher_weighted_k9_last_3,
    p.weighted_bb_last_3 AS pitcher_weighted_bb_last_3,
    p.weighted_whip_last_3 AS pitcher_weighted_whip_last_3,
    p.weighted_outs_last_3 AS pitcher_weighted_outs_last_3,

    -- pitcher weighted rolling last 5
    p.weighted_k_last_5 AS pitcher_weighted_k_last_5,
    p.weighted_ip_last_5 AS pitcher_weighted_ip_last_5,
    p.weighted_bf_last_5 AS pitcher_weighted_bf_last_5,
    p.weighted_pitches_last_5 AS pitcher_weighted_pitches_last_5,
    p.weighted_strike_pct_last_5 AS pitcher_weighted_strike_pct_last_5,
    p.weighted_k9_last_5 AS pitcher_weighted_k9_last_5,
    p.weighted_bb_last_5 AS pitcher_weighted_bb_last_5,
    p.weighted_whip_last_5 AS pitcher_weighted_whip_last_5,
    p.weighted_outs_last_5 AS pitcher_weighted_outs_last_5,

    -- pitcher weighted rolling last 10
    p.weighted_k_last_10 AS pitcher_weighted_k_last_10,
    p.weighted_ip_last_10 AS pitcher_weighted_ip_last_10,
    p.weighted_bf_last_10 AS pitcher_weighted_bf_last_10,
    p.weighted_pitches_last_10 AS pitcher_weighted_pitches_last_10,
    p.weighted_strike_pct_last_10 AS pitcher_weighted_strike_pct_last_10,
    p.weighted_k9_last_10 AS pitcher_weighted_k9_last_10,
    p.weighted_bb_last_10 AS pitcher_weighted_bb_last_10,
    p.weighted_whip_last_10 AS pitcher_weighted_whip_last_10,
    p.weighted_outs_last_10 AS pitcher_weighted_outs_last_10,

    -- pitcher previous game
    p.prev_k AS pitcher_prev_k,
    p.prev_ip AS pitcher_prev_ip,
    p.prev_bf AS pitcher_prev_bf,
    p.prev_pitches AS pitcher_prev_pitches,
    p.prev_k9 AS pitcher_prev_k9,

    -- pitcher statcast last 3 simple
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

    -- pitcher statcast last 5 simple
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

    -- pitcher statcast last 10 simple
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

    -- pitcher weighted statcast last 3
    p.weighted_sc_pitches_last_3 AS pitcher_weighted_sc_pitches_last_3,
    p.weighted_whiff_rate_last_3 AS pitcher_weighted_whiff_rate_last_3,
    p.weighted_csw_rate_last_3 AS pitcher_weighted_csw_rate_last_3,
    p.weighted_sc_strike_rate_last_3 AS pitcher_weighted_sc_strike_rate_last_3,
    p.weighted_velocity_last_3 AS pitcher_weighted_velocity_last_3,
    p.weighted_spin_rate_last_3 AS pitcher_weighted_spin_rate_last_3,
    p.weighted_chase_rate_last_3 AS pitcher_weighted_chase_rate_last_3,
    p.weighted_putaway_rate_last_3 AS pitcher_weighted_putaway_rate_last_3,

    -- pitcher weighted statcast last 5
    p.weighted_sc_pitches_last_5 AS pitcher_weighted_sc_pitches_last_5,
    p.weighted_whiff_rate_last_5 AS pitcher_weighted_whiff_rate_last_5,
    p.weighted_csw_rate_last_5 AS pitcher_weighted_csw_rate_last_5,
    p.weighted_sc_strike_rate_last_5 AS pitcher_weighted_sc_strike_rate_last_5,
    p.weighted_velocity_last_5 AS pitcher_weighted_velocity_last_5,
    p.weighted_spin_rate_last_5 AS pitcher_weighted_spin_rate_last_5,
    p.weighted_chase_rate_last_5 AS pitcher_weighted_chase_rate_last_5,
    p.weighted_putaway_rate_last_5 AS pitcher_weighted_putaway_rate_last_5,

    -- pitcher weighted statcast last 10
    p.weighted_sc_pitches_last_10 AS pitcher_weighted_sc_pitches_last_10,
    p.weighted_whiff_rate_last_10 AS pitcher_weighted_whiff_rate_last_10,
    p.weighted_csw_rate_last_10 AS pitcher_weighted_csw_rate_last_10,
    p.weighted_sc_strike_rate_last_10 AS pitcher_weighted_sc_strike_rate_last_10,
    p.weighted_velocity_last_10 AS pitcher_weighted_velocity_last_10,
    p.weighted_spin_rate_last_10 AS pitcher_weighted_spin_rate_last_10,
    p.weighted_chase_rate_last_10 AS pitcher_weighted_chase_rate_last_10,
    p.weighted_putaway_rate_last_10 AS pitcher_weighted_putaway_rate_last_10,

    -- pitcher previous statcast game
    p.prev_whiff_rate AS pitcher_prev_whiff_rate,
    p.prev_csw_rate AS pitcher_prev_csw_rate,
    p.prev_chase_rate AS pitcher_prev_chase_rate,

    -- pitcher target
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
   AND p.player_id = pg.player_id
LEFT JOIN hitter_pa_game_features hpg
    ON h.gamePk = hpg.gamePk
   AND h.player_id = hpg.hitter_id
   AND h.season = hpg.season
LEFT JOIN hitter_lineup hl
    ON h.gamePk = hl.gamePk
   AND h.player_id = hl.hitter_id;
    """

    execute_sql(sql)
    logger.info("Finished building mlb.dbo.fact_hitter_pitcher_matchup_model_features")

if __name__ == "__main__":
    build_hitter_pitcher_matchup_model_features()