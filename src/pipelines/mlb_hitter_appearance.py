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

CREATE_STG_SQL = """
IF OBJECT_ID('mlb.dbo.stg_fact_hitter_plate_appearances', 'U') IS NULL
BEGIN
    CREATE TABLE mlb.dbo.stg_fact_hitter_plate_appearances (
        gamePk BIGINT,
        game_date DATE,
        inning INT,
        half_inning VARCHAR(10),
        at_bat_index INT,
        plate_appearance_number INT,
        batter_id INT,
        batter_name VARCHAR(100),
        pitcher_id INT,
        pitcher_name VARCHAR(100),
        batter_team_id INT,
        batter_team_name VARCHAR(100),
        pitcher_team_id INT,
        pitcher_team_name VARCHAR(100),
        event_type VARCHAR(100),
        event VARCHAR(100),
        event_description VARCHAR(500),
        rbi INT,
        is_hit BIT,
        is_single BIT,
        is_double BIT,
        is_triple BIT,
        is_home_run BIT,
        is_walk BIT,
        is_strikeout BIT,
        is_hit_by_pitch BIT,
        is_sac_fly BIT,
        is_sac_bunt BIT,
        is_out BIT,
        outs_before_play INT,
        outs_after_play INT,
        start_time DATETIME2,
        end_time DATETIME2,
        extract_date DATETIME2
    );
END
"""

CREATE_FACT_SQL = """
IF OBJECT_ID('mlb.dbo.fact_hitter_plate_appearances', 'U') IS NULL
BEGIN
    CREATE TABLE mlb.dbo.fact_hitter_plate_appearances (
        gamePk BIGINT NOT NULL,
        game_date DATE NULL,
        inning INT NULL,
        half_inning VARCHAR(10) NULL,
        at_bat_index INT NOT NULL,
        plate_appearance_number INT NULL,
        batter_id INT NULL,
        batter_name VARCHAR(100) NULL,
        pitcher_id INT NULL,
        pitcher_name VARCHAR(100) NULL,
        batter_team_id INT NULL,
        batter_team_name VARCHAR(100) NULL,
        pitcher_team_id INT NULL,
        pitcher_team_name VARCHAR(100) NULL,
        event_type VARCHAR(100) NULL,
        event VARCHAR(100) NULL,
        event_description VARCHAR(500) NULL,
        rbi INT NULL,
        is_hit BIT NULL,
        is_single BIT NULL,
        is_double BIT NULL,
        is_triple BIT NULL,
        is_home_run BIT NULL,
        is_walk BIT NULL,
        is_strikeout BIT NULL,
        is_hit_by_pitch BIT NULL,
        is_sac_fly BIT NULL,
        is_sac_bunt BIT NULL,
        is_out BIT NULL,
        outs_before_play INT NULL,
        outs_after_play INT NULL,
        start_time DATETIME2 NULL,
        end_time DATETIME2 NULL,
        extract_date DATETIME2 NULL
    );

    CREATE UNIQUE INDEX UX_fact_hitter_plate_appearances
        ON mlb.dbo.fact_hitter_plate_appearances (gamePk, at_bat_index);
END
"""

MERGE_SQL = """
WITH src AS (
    SELECT *
    FROM (
        SELECT
            s.*,
            ROW_NUMBER() OVER (
                PARTITION BY s.gamePk, s.at_bat_index
                ORDER BY s.extract_date DESC
            ) AS rn
        FROM mlb.dbo.stg_fact_hitter_plate_appearances s
    ) x
    WHERE x.rn = 1
)
MERGE mlb.dbo.fact_hitter_plate_appearances AS target
USING src AS source
    ON target.gamePk = source.gamePk
   AND target.at_bat_index = source.at_bat_index

WHEN MATCHED THEN
    UPDATE SET
        target.game_date = source.game_date,
        target.inning = source.inning,
        target.half_inning = source.half_inning,
        target.plate_appearance_number = source.plate_appearance_number,
        target.batter_id = source.batter_id,
        target.batter_name = source.batter_name,
        target.pitcher_id = source.pitcher_id,
        target.pitcher_name = source.pitcher_name,
        target.batter_team_id = source.batter_team_id,
        target.batter_team_name = source.batter_team_name,
        target.pitcher_team_id = source.pitcher_team_id,
        target.pitcher_team_name = source.pitcher_team_name,
        target.event_type = source.event_type,
        target.event = source.event,
        target.event_description = source.event_description,
        target.rbi = source.rbi,
        target.is_hit = source.is_hit,
        target.is_single = source.is_single,
        target.is_double = source.is_double,
        target.is_triple = source.is_triple,
        target.is_home_run = source.is_home_run,
        target.is_walk = source.is_walk,
        target.is_strikeout = source.is_strikeout,
        target.is_hit_by_pitch = source.is_hit_by_pitch,
        target.is_sac_fly = source.is_sac_fly,
        target.is_sac_bunt = source.is_sac_bunt,
        target.is_out = source.is_out,
        target.outs_before_play = source.outs_before_play,
        target.outs_after_play = source.outs_after_play,
        target.start_time = source.start_time,
        target.end_time = source.end_time,
        target.extract_date = source.extract_date

WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        gamePk,
        game_date,
        inning,
        half_inning,
        at_bat_index,
        plate_appearance_number,
        batter_id,
        batter_name,
        pitcher_id,
        pitcher_name,
        batter_team_id,
        batter_team_name,
        pitcher_team_id,
        pitcher_team_name,
        event_type,
        event,
        event_description,
        rbi,
        is_hit,
        is_single,
        is_double,
        is_triple,
        is_home_run,
        is_walk,
        is_strikeout,
        is_hit_by_pitch,
        is_sac_fly,
        is_sac_bunt,
        is_out,
        outs_before_play,
        outs_after_play,
        start_time,
        end_time,
        extract_date
    )
    VALUES (
        source.gamePk,
        source.game_date,
        source.inning,
        source.half_inning,
        source.at_bat_index,
        source.plate_appearance_number,
        source.batter_id,
        source.batter_name,
        source.pitcher_id,
        source.pitcher_name,
        source.batter_team_id,
        source.batter_team_name,
        source.pitcher_team_id,
        source.pitcher_team_name,
        source.event_type,
        source.event,
        source.event_description,
        source.rbi,
        source.is_hit,
        source.is_single,
        source.is_double,
        source.is_triple,
        source.is_home_run,
        source.is_walk,
        source.is_strikeout,
        source.is_hit_by_pitch,
        source.is_sac_fly,
        source.is_sac_bunt,
        source.is_out,
        source.outs_before_play,
        source.outs_after_play,
        source.start_time,
        source.end_time,
        source.extract_date
    );
"""


def safe_int(value):
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def safe_ts(value):
    if not value:
        return None
    try:
        return pd.to_datetime(value)
    except Exception:
        return None


def get_batter_team_info(half_inning: str, home_team: dict, away_team: dict):
    """
    In top inning, away team bats.
    In bottom inning, home team bats.
    """
    if str(half_inning).lower() == "top":
        return away_team
    return home_team


def get_pitcher_team_info(half_inning: str, home_team: dict, away_team: dict):
    """
    In top inning, home team pitches.
    In bottom inning, away team pitches.
    """
    if str(half_inning).lower() == "top":
        return home_team
    return away_team


def fetch_hitter_plate_appearances(start_date: str, end_date: str, sleep_sec: float = 0.3) -> pd.DataFrame:
    schedule_df = fetch_gamePk_with_dates(start_date, end_date)

    if schedule_df.empty:
        logger.warning("No games found for the given date range")
        return pd.DataFrame()

    schedule_df = schedule_df.drop_duplicates(subset=["gamePk"]).reset_index(drop=True)
    logger.info("Games to process: %s", len(schedule_df))

    rows = []

    for _, sched_row in schedule_df.iterrows():
        gamePk = sched_row["gamePk"]
        game_date = sched_row.get("officialDate", sched_row.get("gameDate"))

        url = f"https://statsapi.mlb.com/api/v1.1/game/{gamePk}/feed/live"
        logger.info("Fetching play-by-play for gamePk=%s", gamePk)

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            logger.error("Request failed for gamePk=%s: %s", gamePk, e)
            continue

        all_plays = data.get("liveData", {}).get("plays", {}).get("allPlays", [])
        if not all_plays:
            logger.warning("No allPlays found for gamePk=%s", gamePk)
            continue

        teams = data.get("gameData", {}).get("teams", {})
        home_team = {
            "team_id": teams.get("home", {}).get("id"),
            "team_name": teams.get("home", {}).get("name")
        }
        away_team = {
            "team_id": teams.get("away", {}).get("id"),
            "team_name": teams.get("away", {}).get("name")
        }

        for pa_num, play in enumerate(all_plays, start=1):
            about = play.get("about", {})
            result = play.get("result", {})
            matchup = play.get("matchup", {})
            count = play.get("count", {})

            inning = safe_int(about.get("inning"))
            half_inning = about.get("halfInning")
            at_bat_index = safe_int(play.get("atBatIndex"))

            batter = matchup.get("batter", {})
            pitcher = matchup.get("pitcher", {})

            batter_team = get_batter_team_info(half_inning, home_team, away_team)
            pitcher_team = get_pitcher_team_info(half_inning, home_team, away_team)

            event = result.get("event")
            event_type = result.get("eventType")
            description = result.get("description")
            rbi = safe_int(result.get("rbi"))

            event_lower = str(event).lower() if event else ""
            event_type_lower = str(event_type).lower() if event_type else ""

            is_single = 1 if "single" in event_lower else 0
            is_double = 1 if "double" in event_lower and "ground-rule double" not in event_lower else 0
            is_triple = 1 if "triple" in event_lower else 0
            is_home_run = 1 if "home run" in event_lower or event_type_lower == "home_run" else 0
            is_hit = 1 if any([is_single, is_double, is_triple, is_home_run]) else 0
            is_walk = 1 if event_type_lower in ["walk", "intent_walk"] or "walk" in event_lower else 0
            is_strikeout = 1 if "strikeout" in event_type_lower or "strikeout" in event_lower else 0
            is_hit_by_pitch = 1 if event_type_lower == "hit_by_pitch" or "hit by pitch" in event_lower else 0
            is_sac_fly = 1 if event_type_lower == "sac_fly" or "sac fly" in event_lower else 0
            is_sac_bunt = 1 if event_type_lower == "sac_bunt" or "sac bunt" in event_lower else 0

            outs_before = safe_int(count.get("outs"))
            outs_after = safe_int(about.get("outs"))

            # A rough out flag. This is useful for game-level aggregation.
            is_out = 1 if (
                (outs_before is not None and outs_after is not None and outs_after > outs_before)
                or is_strikeout == 1
                or is_sac_fly == 1
                or is_sac_bunt == 1
            ) else 0

            rows.append({
                "gamePk": gamePk,
                "game_date": game_date,
                "inning": inning,
                "half_inning": half_inning,
                "at_bat_index": at_bat_index,
                "plate_appearance_number": pa_num,
                "batter_id": safe_int(batter.get("id")),
                "batter_name": batter.get("fullName"),
                "pitcher_id": safe_int(pitcher.get("id")),
                "pitcher_name": pitcher.get("fullName"),
                "batter_team_id": batter_team.get("team_id"),
                "batter_team_name": batter_team.get("team_name"),
                "pitcher_team_id": pitcher_team.get("team_id"),
                "pitcher_team_name": pitcher_team.get("team_name"),
                "event_type": event_type,
                "event": event,
                "event_description": description,
                "rbi": rbi,
                "is_hit": is_hit,
                "is_single": is_single,
                "is_double": is_double,
                "is_triple": is_triple,
                "is_home_run": is_home_run,
                "is_walk": is_walk,
                "is_strikeout": is_strikeout,
                "is_hit_by_pitch": is_hit_by_pitch,
                "is_sac_fly": is_sac_fly,
                "is_sac_bunt": is_sac_bunt,
                "is_out": is_out,
                "outs_before_play": outs_before,
                "outs_after_play": outs_after,
                "start_time": safe_ts(about.get("startTime")),
                "end_time": safe_ts(about.get("endTime")),
                "extract_date": pd.Timestamp.now()
            })

        time.sleep(sleep_sec)

    df = pd.DataFrame(rows)

    if df.empty:
        logger.warning("No plate appearance rows collected")
        return df

    df = df.drop_duplicates(subset=["gamePk", "at_bat_index"]).reset_index(drop=True)
    logger.info("Collected %s plate appearance rows after dedupe", len(df))
    return df


if __name__ == "__main__":
    start_date = os.getenv("START_DATE")
    end_date = os.getenv("END_DATE")

    if not start_date or not end_date:
        raise ValueError("START_DATE and END_DATE must be set in .env file")

    logger.info("Using start_date=%s, end_date=%s", start_date, end_date)

    execute_sql(CREATE_STG_SQL)
    execute_sql(CREATE_FACT_SQL)

    df = fetch_hitter_plate_appearances(start_date=start_date, end_date=end_date)

    logger.info("Final dataframe shape: %s", df.shape)
    print(df.head(20))

    if not df.empty:
        truncate_table("stg_fact_hitter_plate_appearances", schema="dbo")
        load_dataframe(df, "stg_fact_hitter_plate_appearances", if_exists="append")
        logger.info("Loaded dataframe to staging table")

        execute_sql(MERGE_SQL)
        logger.info("Successfully merged data into fact_hitter_plate_appearances")
    else:
        logger.warning("No data to load into fact_hitter_plate_appearances")