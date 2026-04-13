import pandas as pd
import requests
import logging
import time
import os

from dotenv import load_dotenv
from src.ingestion.mlb_gamePk import fetch_gamePk_with_dates
from sql.sql_loader import load_dataframe, truncate_table, execute_sql

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

load_dotenv(override=True)

MERGE_SQL = """
WITH src AS (
    SELECT *
    FROM (
        SELECT
            s.*,
            ROW_NUMBER() OVER (
                PARTITION BY s.gamePk, s.side, s.batting_order
                ORDER BY s.extract_date DESC
            ) AS rn
        FROM mlb.dbo.stg_fact_hitter_lineup s
    ) x
    WHERE x.rn = 1
)
MERGE mlb.dbo.fact_hitter_lineup AS target
USING src AS source
    ON target.gamePk = source.gamePk
   AND target.side = source.side
   AND target.batting_order = source.batting_order

WHEN MATCHED THEN
    UPDATE SET
        target.game_date = source.game_date,
        target.team_id = source.team_id,
        target.team_name = source.team_name,
        target.player_id = source.player_id,
        target.player_name = source.player_name,
        target.position_abbreviation = source.position_abbreviation,
        target.position_name = source.position_name,
        target.extract_date = source.extract_date

WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        gamePk,
        game_date,
        side,
        team_id,
        team_name,
        batting_order,
        player_id,
        player_name,
        position_abbreviation,
        position_name,
        extract_date
    )
    VALUES (
        source.gamePk,
        source.game_date,
        source.side,
        source.team_id,
        source.team_name,
        source.batting_order,
        source.player_id,
        source.player_name,
        source.position_abbreviation,
        source.position_name,
        source.extract_date
    );
"""


def fetch_hitter_lineup(start_date: str, end_date: str, sleep_sec: float = 0.3) -> pd.DataFrame:
    games = fetch_gamePk_with_dates(start_date, end_date)

    if games.empty:
        logger.warning("No games found for the given date range")
        return pd.DataFrame()

    games = games.drop_duplicates(subset=["gamePk"]).reset_index(drop=True)

    rows = []

    for _, game in games.iterrows():
        gamePk = game["gamePk"]
        game_date = game.get("officialDate")

        url = f"https://statsapi.mlb.com/api/v1/game/{gamePk}/boxscore"
        logger.info("Fetching lineup for gamePk=%s", gamePk)

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            logger.error("Request failed for gamePk=%s: %s", gamePk, e)
            continue

        teams = data.get("teams", {})

        for side in ["away", "home"]:
            team_data = teams.get(side, {})
            team_info = team_data.get("team", {})
            team_id = team_info.get("id")
            team_name = team_info.get("name")

            batters = team_data.get("batters", [])
            players = team_data.get("players", {})

            if not batters:
                logger.warning("No batters found for gamePk=%s side=%s", gamePk, side)
                continue

            for batting_order, player_id in enumerate(batters, start=1):
                player_key = f"ID{player_id}"
                player_info = players.get(player_key, {})

                person = player_info.get("person", {})
                position = player_info.get("position", {})

                rows.append({
                    "gamePk": gamePk,
                    "game_date": game_date,
                    "side": side,
                    "team_id": team_id,
                    "team_name": team_name,
                    "batting_order": batting_order,
                    "player_id": person.get("id"),
                    "player_name": person.get("fullName"),
                    "position_abbreviation": position.get("abbreviation"),
                    "position_name": position.get("name"),
                    "extract_date": pd.Timestamp.now()
                })

        time.sleep(sleep_sec)

    df = pd.DataFrame(rows)

    if df.empty:
        logger.warning("No lineup rows collected")
        return df

    df = df.drop_duplicates(subset=["gamePk", "side", "batting_order"]).reset_index(drop=True)

    logger.info("Collected %s lineup rows after dedupe", len(df))
    return df


if __name__ == "__main__":
    start_date = os.getenv("START_DATE")
    end_date = os.getenv("END_DATE")

    if not start_date or not end_date:
        raise ValueError("START_DATE and END_DATE must be set in .env file")

    logger.info("Using start_date=%s, end_date=%s", start_date, end_date)

    df = fetch_hitter_lineup(
        start_date=start_date,
        end_date=end_date
    )

    if not df.empty:
        truncate_table("stg_fact_hitter_lineup", schema="dbo")
        load_dataframe(df, "stg_fact_hitter_lineup", if_exists="append")
        execute_sql(MERGE_SQL)
        logger.info("Successfully merged data into fact_hitter_lineup")
    else:
        logger.warning("No data to load into fact_hitter_lineup")