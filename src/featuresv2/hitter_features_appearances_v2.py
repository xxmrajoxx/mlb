import logging
from sql.sql_loader import execute_sql

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


def build_hitter_pitcher_pa_game_agg() -> None:
    logger.info("Building mlb.dbo.fact_hitter_pitcher_pa_game_aggv2")

    sql = """
IF OBJECT_ID('mlb.dbo.fact_hitter_pitcher_pa_game_aggv2', 'U') IS NOT NULL
    DROP TABLE mlb.dbo.fact_hitter_pitcher_pa_game_aggv2;

WITH base AS (
    SELECT
        TRY_CAST(gamePk AS int) AS gamePk,
        CAST(game_date AS date) AS game_date,
        YEAR(CAST(game_date AS date)) AS season,
        TRY_CAST(batter_id AS int) AS batter_id,
        batter_name,
        TRY_CAST(pitcher_id AS int) AS pitcher_id,
        pitcher_name,
        TRY_CAST(batter_team_id AS int) AS batter_team_id,
        batter_team_name,
        TRY_CAST(pitcher_team_id AS int) AS pitcher_team_id,
        pitcher_team_name,

        TRY_CAST(inning AS int) AS inning,
        TRY_CAST(plate_appearance_number AS int) AS plate_appearance_number,
        TRY_CAST(rbi AS float) AS rbi,

        TRY_CAST(is_hit AS float) AS is_hit,
        TRY_CAST(is_single AS float) AS is_single,
        TRY_CAST(is_double AS float) AS is_double,
        TRY_CAST(is_triple AS float) AS is_triple,
        TRY_CAST(is_home_run AS float) AS is_home_run,
        TRY_CAST(is_walk AS float) AS is_walk,
        TRY_CAST(is_strikeout AS float) AS is_strikeout,
        TRY_CAST(is_hit_by_pitch AS float) AS is_hit_by_pitch,
        TRY_CAST(is_sac_fly AS float) AS is_sac_fly,
        TRY_CAST(is_sac_bunt AS float) AS is_sac_bunt,
        TRY_CAST(is_out AS float) AS is_out

    FROM mlb.dbo.fact_hitter_plate_appearances
    WHERE batter_id IS NOT NULL
      AND pitcher_id IS NOT NULL
      AND gamePk IS NOT NULL
)

SELECT
    gamePk,
    game_date,
    season,
    batter_id,
    MAX(batter_name) AS batter_name,
    pitcher_id,
    MAX(pitcher_name) AS pitcher_name,
    MAX(batter_team_id) AS batter_team_id,
    MAX(batter_team_name) AS batter_team_name,
    MAX(pitcher_team_id) AS pitcher_team_id,
    MAX(pitcher_team_name) AS pitcher_team_name,

    COUNT(*) AS plate_appearances,
    SUM(is_hit) AS hits,
    SUM(is_single) AS singles,
    SUM(is_double) AS doubles,
    SUM(is_triple) AS triples,
    SUM(is_home_run) AS home_runs,
    SUM(is_walk) AS walks,
    SUM(is_strikeout) AS strikeouts,
    SUM(is_hit_by_pitch) AS hit_by_pitch,
    SUM(is_sac_fly) AS sac_flies,
    SUM(is_sac_bunt) AS sac_bunts,
    SUM(is_out) AS outs_recorded,
    SUM(ISNULL(rbi, 0)) AS rbi,

    MIN(inning) AS first_inning_faced,
    MAX(inning) AS last_inning_faced,

    -- convenience rate
    CAST(SUM(is_strikeout) AS float) / NULLIF(COUNT(*), 0) AS k_rate_in_matchup

INTO mlb.dbo.fact_hitter_pitcher_pa_game_aggv2
FROM base
GROUP BY
    gamePk,
    game_date,
    season,
    batter_id,
    pitcher_id;
    """

    execute_sql(sql)
    logger.info("Finished building mlb.dbo.fact_hitter_pitcher_pa_game_aggv2")


def build_hitter_pitcher_pa_rolling_features() -> None:
    logger.info("Building mlb.dbo.fact_hitter_pitcher_pa_rolling_featuresv2")

    sql = """
IF OBJECT_ID('mlb.dbo.fact_hitter_pitcher_pa_rolling_featuresv2', 'U') IS NOT NULL
    DROP TABLE mlb.dbo.fact_hitter_pitcher_pa_rolling_featuresv2;

WITH base AS (
    SELECT
        TRY_CAST(gamePk AS bigint) AS gamePk,
        CAST(game_date AS date) AS game_date,
        YEAR(CAST(game_date AS date)) AS season,
        TRY_CAST(batter_id AS int) AS batter_id,
        batter_name,
        TRY_CAST(pitcher_id AS int) AS pitcher_id,
        pitcher_name,
        TRY_CAST(batter_team_id AS int) AS batter_team_id,
        batter_team_name,
        TRY_CAST(pitcher_team_id AS int) AS pitcher_team_id,
        pitcher_team_name,

        TRY_CAST(plate_appearances AS float) AS plate_appearances,
        TRY_CAST(hits AS float) AS hits,
        TRY_CAST(singles AS float) AS singles,
        TRY_CAST(doubles AS float) AS doubles,
        TRY_CAST(triples AS float) AS triples,
        TRY_CAST(home_runs AS float) AS home_runs,
        TRY_CAST(walks AS float) AS walks,
        TRY_CAST(strikeouts AS float) AS strikeouts,
        TRY_CAST(hit_by_pitch AS float) AS hit_by_pitch,
        TRY_CAST(sac_flies AS float) AS sac_flies,
        TRY_CAST(sac_bunts AS float) AS sac_bunts,
        TRY_CAST(outs_recorded AS float) AS outs_recorded,
        TRY_CAST(rbi AS float) AS rbi,
        TRY_CAST(first_inning_faced AS float) AS first_inning_faced,
        TRY_CAST(last_inning_faced AS float) AS last_inning_faced

    FROM mlb.dbo.fact_hitter_pitcher_pa_game_aggv2
),

prep AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY batter_id, pitcher_id, season
            ORDER BY game_date, gamePk
        ) AS rn
    FROM base
),

rolling AS (
    SELECT
        p1.gamePk,
        p1.game_date,
        p1.season,
        p1.batter_id,
        p1.batter_name,
        p1.pitcher_id,
        p1.pitcher_name,
        p1.batter_team_id,
        p1.batter_team_name,
        p1.pitcher_team_id,
        p1.pitcher_team_name,

        /* -------------------- MATCHUP GAME COUNTS -------------------- */
        COUNT(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN 1 END) AS matchup_games_last_3,
        COUNT(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN 1 END) AS matchup_games_last_5,
        COUNT(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN 1 END) AS matchup_games_last_10,

        /* -------------------- LAST 3 SIMPLE AVG -------------------- */
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.plate_appearances END) AS avg_pa_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.hits END) AS avg_hits_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.singles END) AS avg_singles_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.doubles END) AS avg_doubles_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.triples END) AS avg_triples_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.home_runs END) AS avg_home_runs_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.walks END) AS avg_walks_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.strikeouts END) AS avg_strikeouts_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.hit_by_pitch END) AS avg_hbp_last_3,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.rbi END) AS avg_rbi_last_3,

        /* -------------------- LAST 3 WEIGHTED AVG -------------------- */
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.plate_appearances * (p2.rn - (p1.rn - 3)) END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN (p2.rn - (p1.rn - 3)) END), 0) AS wavg_pa_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.hits * (p2.rn - (p1.rn - 3)) END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN (p2.rn - (p1.rn - 3)) END), 0) AS wavg_hits_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.singles * (p2.rn - (p1.rn - 3)) END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN (p2.rn - (p1.rn - 3)) END), 0) AS wavg_singles_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.doubles * (p2.rn - (p1.rn - 3)) END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN (p2.rn - (p1.rn - 3)) END), 0) AS wavg_doubles_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.triples * (p2.rn - (p1.rn - 3)) END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN (p2.rn - (p1.rn - 3)) END), 0) AS wavg_triples_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.home_runs * (p2.rn - (p1.rn - 3)) END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN (p2.rn - (p1.rn - 3)) END), 0) AS wavg_home_runs_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.walks * (p2.rn - (p1.rn - 3)) END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN (p2.rn - (p1.rn - 3)) END), 0) AS wavg_walks_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.strikeouts * (p2.rn - (p1.rn - 3)) END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN (p2.rn - (p1.rn - 3)) END), 0) AS wavg_strikeouts_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.hit_by_pitch * (p2.rn - (p1.rn - 3)) END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN (p2.rn - (p1.rn - 3)) END), 0) AS wavg_hbp_last_3,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN p2.rbi * (p2.rn - (p1.rn - 3)) END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 3 AND p1.rn - 1 THEN (p2.rn - (p1.rn - 3)) END), 0) AS wavg_rbi_last_3,

        /* -------------------- LAST 5 SIMPLE AVG -------------------- */
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.plate_appearances END) AS avg_pa_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.hits END) AS avg_hits_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.singles END) AS avg_singles_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.doubles END) AS avg_doubles_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.triples END) AS avg_triples_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.home_runs END) AS avg_home_runs_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.walks END) AS avg_walks_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.strikeouts END) AS avg_strikeouts_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.hit_by_pitch END) AS avg_hbp_last_5,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.rbi END) AS avg_rbi_last_5,

        /* -------------------- LAST 5 WEIGHTED AVG -------------------- */
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.plate_appearances * (p2.rn - (p1.rn - 5)) END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN (p2.rn - (p1.rn - 5)) END), 0) AS wavg_pa_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.hits * (p2.rn - (p1.rn - 5)) END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN (p2.rn - (p1.rn - 5)) END), 0) AS wavg_hits_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.singles * (p2.rn - (p1.rn - 5)) END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN (p2.rn - (p1.rn - 5)) END), 0) AS wavg_singles_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.doubles * (p2.rn - (p1.rn - 5)) END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN (p2.rn - (p1.rn - 5)) END), 0) AS wavg_doubles_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.triples * (p2.rn - (p1.rn - 5)) END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN (p2.rn - (p1.rn - 5)) END), 0) AS wavg_triples_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.home_runs * (p2.rn - (p1.rn - 5)) END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN (p2.rn - (p1.rn - 5)) END), 0) AS wavg_home_runs_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.walks * (p2.rn - (p1.rn - 5)) END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN (p2.rn - (p1.rn - 5)) END), 0) AS wavg_walks_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.strikeouts * (p2.rn - (p1.rn - 5)) END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN (p2.rn - (p1.rn - 5)) END), 0) AS wavg_strikeouts_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.hit_by_pitch * (p2.rn - (p1.rn - 5)) END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN (p2.rn - (p1.rn - 5)) END), 0) AS wavg_hbp_last_5,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN p2.rbi * (p2.rn - (p1.rn - 5)) END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 5 AND p1.rn - 1 THEN (p2.rn - (p1.rn - 5)) END), 0) AS wavg_rbi_last_5,

        /* -------------------- LAST 10 SIMPLE AVG -------------------- */
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.plate_appearances END) AS avg_pa_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.hits END) AS avg_hits_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.singles END) AS avg_singles_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.doubles END) AS avg_doubles_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.triples END) AS avg_triples_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.home_runs END) AS avg_home_runs_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.walks END) AS avg_walks_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.strikeouts END) AS avg_strikeouts_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.hit_by_pitch END) AS avg_hbp_last_10,
        AVG(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.rbi END) AS avg_rbi_last_10,

        /* -------------------- LAST 10 WEIGHTED AVG -------------------- */
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.plate_appearances * (p2.rn - (p1.rn - 10)) END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN (p2.rn - (p1.rn - 10)) END), 0) AS wavg_pa_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.hits * (p2.rn - (p1.rn - 10)) END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN (p2.rn - (p1.rn - 10)) END), 0) AS wavg_hits_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.singles * (p2.rn - (p1.rn - 10)) END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN (p2.rn - (p1.rn - 10)) END), 0) AS wavg_singles_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.doubles * (p2.rn - (p1.rn - 10)) END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN (p2.rn - (p1.rn - 10)) END), 0) AS wavg_doubles_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.triples * (p2.rn - (p1.rn - 10)) END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN (p2.rn - (p1.rn - 10)) END), 0) AS wavg_triples_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.home_runs * (p2.rn - (p1.rn - 10)) END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN (p2.rn - (p1.rn - 10)) END), 0) AS wavg_home_runs_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.walks * (p2.rn - (p1.rn - 10)) END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN (p2.rn - (p1.rn - 10)) END), 0) AS wavg_walks_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.strikeouts * (p2.rn - (p1.rn - 10)) END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN (p2.rn - (p1.rn - 10)) END), 0) AS wavg_strikeouts_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.hit_by_pitch * (p2.rn - (p1.rn - 10)) END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN (p2.rn - (p1.rn - 10)) END), 0) AS wavg_hbp_last_10,
        SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN p2.rbi * (p2.rn - (p1.rn - 10)) END)
            / NULLIF(SUM(CASE WHEN p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1 THEN (p2.rn - (p1.rn - 10)) END), 0) AS wavg_rbi_last_10,

        /* -------------------- PREVIOUS GAME -------------------- */
        MAX(CASE WHEN p2.rn = p1.rn - 1 THEN p2.plate_appearances END) AS prev_pa,
        MAX(CASE WHEN p2.rn = p1.rn - 1 THEN p2.hits END) AS prev_hits,
        MAX(CASE WHEN p2.rn = p1.rn - 1 THEN p2.home_runs END) AS prev_home_runs,
        MAX(CASE WHEN p2.rn = p1.rn - 1 THEN p2.walks END) AS prev_walks,
        MAX(CASE WHEN p2.rn = p1.rn - 1 THEN p2.strikeouts END) AS prev_strikeouts,
        MAX(CASE WHEN p2.rn = p1.rn - 1 THEN p2.rbi END) AS prev_rbi

    FROM prep p1
    LEFT JOIN prep p2
        ON p1.batter_id = p2.batter_id
       AND p1.pitcher_id = p2.pitcher_id
       AND p1.season = p2.season
       AND p2.rn BETWEEN p1.rn - 10 AND p1.rn - 1
    GROUP BY
        p1.gamePk,
        p1.game_date,
        p1.season,
        p1.batter_id,
        p1.batter_name,
        p1.pitcher_id,
        p1.pitcher_name,
        p1.batter_team_id,
        p1.batter_team_name,
        p1.pitcher_team_id,
        p1.pitcher_team_name,
        p1.rn
)

SELECT *
INTO mlb.dbo.fact_hitter_pitcher_pa_rolling_featuresv2
FROM rolling;
    """

    execute_sql(sql)
    logger.info("Finished building mlb.dbo.fact_hitter_pitcher_pa_rolling_featuresv2")


def run_all_hitter_pitcher_pa_features() -> None:
    logger.info("Starting hitter vs pitcher plate appearance feature pipeline (v2)")
    build_hitter_pitcher_pa_game_agg()
    build_hitter_pitcher_pa_rolling_features()
    logger.info("Finished hitter vs pitcher plate appearance feature pipeline (v2)")


if __name__ == "__main__":
    run_all_hitter_pitcher_pa_features()