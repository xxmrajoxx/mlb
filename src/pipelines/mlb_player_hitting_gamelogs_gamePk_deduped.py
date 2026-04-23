import requests
import pandas as pd
import time
import logging
from datetime import datetime, UTC
import os

from dotenv import load_dotenv
from src.ingestion.mlb_player_id_all import fetch_active_mlb_players
from src.ingestion.mlb_gamePk import fetch_completed_games
from sql.sql_loader import load_dataframe, truncate_table, execute_sql

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

load_dotenv(override=True)

MERGE_SQL = """
;WITH deduped AS (
    SELECT
        s.*,
        ROW_NUMBER() OVER (
            PARTITION BY s.player_id, s.gamePk
            ORDER BY s.extract_ts DESC, s.extract_date DESC
        ) AS rn
    FROM mlb.dbo.stg_player_hitting_gamelogs s
)
INSERT INTO mlb.dbo.fact_player_hitting_gamelogs (
    player_id,
    player_name,
    position,
    team_name,
    team_id,
    gamePk,
    game_date,
    dayNight,
    summary,
    gamesPlayed,
    flyOuts,
    groundOuts,
    airOuts,
    runs,
    doubles,
    triples,
    homeRuns,
    strikeOuts,
    baseOnBalls,
    intentionalWalks,
    hits,
    hitByPitch,
    avg,
    atBats,
    obp,
    slg,
    ops,
    caughtStealing,
    stolenBases,
    stolenBasePercentage,
    caughtStealingPercentage,
    groundIntoDoublePlay,
    groundIntoTriplePlay,
    numberOfPitches,
    plateAppearances,
    totalBases,
    rbi,
    leftOnBase,
    sacBunts,
    sacFlies,
    babip,
    groundOutsToAirouts,
    catchersInterference,
    atBatsPerHomeRun,
    season,
    extract_date,
    extract_ts
)
SELECT
    d.player_id,
    d.player_name,
    d.position,
    d.team_name,
    d.team_id,
    d.gamePk,
    d.game_date,
    d.dayNight,
    d.summary,
    d.gamesPlayed,
    d.flyOuts,
    d.groundOuts,
    d.airOuts,
    d.runs,
    d.doubles,
    d.triples,
    d.homeRuns,
    d.strikeOuts,
    d.baseOnBalls,
    d.intentionalWalks,
    d.hits,
    d.hitByPitch,
    d.avg,
    d.atBats,
    d.obp,
    d.slg,
    d.ops,
    d.caughtStealing,
    d.stolenBases,
    d.stolenBasePercentage,
    d.caughtStealingPercentage,
    d.groundIntoDoublePlay,
    d.groundIntoTriplePlay,
    d.numberOfPitches,
    d.plateAppearances,
    d.totalBases,
    d.rbi,
    d.leftOnBase,
    d.sacBunts,
    d.sacFlies,
    d.babip,
    d.groundOutsToAirouts,
    d.catchersInterference,
    d.atBatsPerHomeRun,
    d.season,
    d.extract_date,
    d.extract_ts
FROM deduped d
WHERE d.rn = 1
  AND NOT EXISTS (
      SELECT 1
      FROM mlb.dbo.fact_player_hitting_gamelogs f
      WHERE f.player_id = d.player_id
        AND f.gamePk = d.gamePk
  );
"""


def fetch_player_game_logs(start_dt: str, end_dt: str, season: int = 2026) -> pd.DataFrame:
    player_df = fetch_active_mlb_players()

    if player_df.empty:
        logger.warning("No active MLB players found")
        return pd.DataFrame()

    if not start_dt:
        start_dt = f"{season}-03-01"

    if not end_dt:
        end_dt = datetime.now(UTC).date().isoformat()

    completed_games_df = fetch_completed_games(start_dt, end_dt)

    if completed_games_df.empty:
        logger.warning("No completed games found in date range")
        return pd.DataFrame()

    completed_gamepks = set(
        completed_games_df["gamePk"].dropna().astype(int).tolist()
    )
    logger.info(f"Found {len(completed_gamepks)} completed gamePk values")

    logs = []

    for _, player_row in player_df.iterrows():
        player_id = player_row["player_id"]
        player_name = player_row["player_name"]
        team_id = player_row["team_id"]
        team_position = player_row["position"]
        team_name = player_row["team_name"]

        url = (
            f"https://statsapi.mlb.com/api/v1/people/{player_id}"
            f"/stats?stats=gameLog&group=hitting&season={season}"
        )

        logger.info(f"Fetching game logs for {player_name} ({player_id})")

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            hitting = response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for player_id={player_id}: {e}")
            continue

        stats_list = hitting.get("stats", [])
        if not stats_list:
            logger.warning(f"No stats found for player_id={player_id}")
            continue

        splits = stats_list[0].get("splits", [])
        if not splits:
            logger.warning(f"No hitting splits found for player_id={player_id}")
            continue

        now_dt = datetime.now(UTC).replace(tzinfo=None)
        now_date = now_dt.date()

        for s in splits:
            stat = s.get("stat", {})
            game = s.get("game", {})
            game_date_raw = s.get("date")
            game_pk = game.get("gamePk")

            if not game_date_raw or game_pk is None:
                continue

            try:
                game_pk = int(game_pk)
            except (TypeError, ValueError):
                continue

            if game_pk not in completed_gamepks:
                continue

            try:
                game_date = pd.to_datetime(game_date_raw).date()
            except Exception:
                continue

            row = {
                "player_id": player_id,
                "player_name": player_name,
                "position": team_position,
                "team_name": team_name,
                "team_id": team_id,
                "gamePk": game_pk,
                "game_date": game_date,
                "dayNight": game.get("dayNight"),
                "summary": stat.get("summary"),
                "gamesPlayed": stat.get("gamesPlayed"),
                "flyOuts": stat.get("flyOuts"),
                "groundOuts": stat.get("groundOuts"),
                "airOuts": stat.get("airOuts"),
                "runs": stat.get("runs"),
                "doubles": stat.get("doubles"),
                "triples": stat.get("triples"),
                "homeRuns": stat.get("homeRuns"),
                "strikeOuts": stat.get("strikeOuts"),
                "baseOnBalls": stat.get("baseOnBalls"),
                "intentionalWalks": stat.get("intentionalWalks"),
                "hits": stat.get("hits"),
                "hitByPitch": stat.get("hitByPitch"),
                "avg": stat.get("avg"),
                "atBats": stat.get("atBats"),
                "obp": stat.get("obp"),
                "slg": stat.get("slg"),
                "ops": stat.get("ops"),
                "caughtStealing": stat.get("caughtStealing"),
                "stolenBases": stat.get("stolenBases"),
                "stolenBasePercentage": stat.get("stolenBasePercentage"),
                "caughtStealingPercentage": stat.get("caughtStealingPercentage"),
                "groundIntoDoublePlay": stat.get("groundIntoDoublePlay"),
                "groundIntoTriplePlay": stat.get("groundIntoTriplePlay"),
                "numberOfPitches": stat.get("numberOfPitches"),
                "plateAppearances": stat.get("plateAppearances"),
                "totalBases": stat.get("totalBases"),
                "rbi": stat.get("rbi"),
                "leftOnBase": stat.get("leftOnBase"),
                "sacBunts": stat.get("sacBunts"),
                "sacFlies": stat.get("sacFlies"),
                "babip": stat.get("babip"),
                "groundOutsToAirouts": stat.get("groundOutsToAirouts"),
                "catchersInterference": stat.get("catchersInterference"),
                "atBatsPerHomeRun": stat.get("atBatsPerHomeRun"),
                "season": int(season),
                "extract_date": now_date,
                "extract_ts": now_dt,
            }
            logs.append(row)

        time.sleep(0.2)

    if not logs:
        logger.warning("No player hitting game logs collected")
        return pd.DataFrame()

    final_df = pd.DataFrame(logs).copy()

    logger.info(f"Collected {len(final_df)} raw hitting game log rows")

    before_dedup = len(final_df)
    final_df = (
        final_df.sort_values(["extract_ts", "extract_date"])
        .drop_duplicates(subset=["player_id", "gamePk"], keep="last")
        .reset_index(drop=True)
    )
    after_dedup = len(final_df)

    logger.info(
        f"Deduped hitting game logs: removed {before_dedup - after_dedup} duplicate rows; "
        f"{after_dedup} rows remain"
    )

    return final_df


def load_player_hitting_gamelogs(df: pd.DataFrame):
    if df.empty:
        logger.warning("DataFrame is empty - nothing to load")
        return

    staging_table = "stg_player_hitting_gamelogs"

    logger.info("Loading staging table")
    load_dataframe(df, staging_table, if_exists="replace")

    logger.info("Checking for duplicate keys in staging")
    duplicate_check_sql = f"""
    SELECT
        player_id,
        gamePk,
        COUNT(*) AS row_count
    FROM mlb.dbo.{staging_table}
    GROUP BY player_id, gamePk
    HAVING COUNT(*) > 1;
    """

    logger.info("Merging staging into fact table")
    execute_sql(MERGE_SQL)

    logger.info("Truncating staging table after merge")
    truncate_table(staging_table)

    logger.info("Load complete for mlb.dbo.fact_player_hitting_gamelogs")


if __name__ == "__main__":
    start_dt = os.getenv("start_dt")
    end_dt = os.getenv("end_dt")
    season = os.getenv("SEASON")

    if not season:
        raise ValueError("SEASON is missing in .env file")

    season = int(season)

    df = fetch_player_game_logs(
        season=season,
        start_dt=start_dt,
        end_dt=end_dt
    )

    if not start_dt or not end_dt:
        raise ValueError("check .env file for start_dt and end_dt")

    logger.info("Starting load for fact_player_hitting_gamelogs")
    load_player_hitting_gamelogs(df)