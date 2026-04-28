"""
Fetch probable pitchers for upcoming MLB games and store in SQL Server.

Usage:
    python fetch_probable_pitchers.py [--days_ahead 2]

Hits the MLB Stats API at:
    https://statsapi.mlb.com/api/v1/schedule?sportId=1&date=YYYY-MM-DD
        &hydrate=probablePitcher

Writes results to mlb.dbo.dim_probable_pitchers (creates table if missing).

Run this every morning before scoring future games. The MLB API is free,
unauthenticated, and typically updates probable pitchers 2-3 days in advance.
"""
from __future__ import annotations
import argparse
import logging
import sys
from datetime import date, timedelta

import pandas as pd
import requests
from sqlalchemy import text

# Project imports
sys.path.insert(0, ".")
import config
from sql_loader import get_engine, load_dataframe

logger = logging.getLogger("probable_pitchers")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


MLB_API_URL = "https://statsapi.mlb.com/api/v1/schedule"


def fetch_schedule_for_date(target_date: date) -> list[dict]:
    """Hit MLB Stats API for one date with probable pitcher hydration."""
    params = {
        "sportId": 1,
        "date": target_date.strftime("%Y-%m-%d"),
        "hydrate": "probablePitcher,linescore",
    }
    logger.info(f"Fetching schedule for {target_date}")
    resp = requests.get(MLB_API_URL, params=params, timeout=20)
    resp.raise_for_status()
    return resp.json().get("dates", [])


def parse_probable_pitchers(schedule_data: list[dict]) -> pd.DataFrame:
    """Extract probable pitcher rows from MLB API response."""
    rows = []
    for date_block in schedule_data:
        official_date = date_block.get("date")
        for game in date_block.get("games", []):
            game_pk = game.get("gamePk")
            game_date_iso = game.get("gameDate")
            status = game.get("status", {}).get("detailedState")

            for side in ("away", "home"):
                team_block = game.get("teams", {}).get(side, {})
                team = team_block.get("team", {})
                pitcher = team_block.get("probablePitcher", {})

                rows.append({
                    "gamePk": game_pk,
                    "official_date": official_date,
                    "game_datetime": game_date_iso,
                    "game_status": status,
                    "side": side,
                    "team_id": team.get("id"),
                    "team_name": team.get("name"),
                    "pitcher_id": pitcher.get("id"),
                    "pitcher_name": pitcher.get("fullName"),
                })
    return pd.DataFrame(rows)


def ensure_table(engine):
    """Create the probable pitchers table if it doesn't exist."""
    ddl = """
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_probable_pitchers')
    BEGIN
        CREATE TABLE dbo.dim_probable_pitchers (
            gamePk          BIGINT,
            official_date   DATE,
            game_datetime   VARCHAR(40),
            game_status     VARCHAR(40),
            side            VARCHAR(10),       -- 'home' or 'away'
            team_id         INT,
            team_name       VARCHAR(100),
            pitcher_id      INT,
            pitcher_name    VARCHAR(100),
            fetched_at      DATETIME
        )
    END
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def clear_existing_for_dates(engine, dates: list[date]):
    """Remove old probable pitcher rows for these dates so re-runs don't duplicate."""
    if not dates:
        return
    date_str_list = ", ".join(f"'{d.isoformat()}'" for d in dates)
    sql = f"""
        DELETE FROM dbo.dim_probable_pitchers
        WHERE official_date IN ({date_str_list})
    """
    with engine.begin() as conn:
        result = conn.execute(text(sql))
        logger.info(f"Cleared {result.rowcount} existing probable-pitcher rows for re-fetch")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days_ahead", type=int, default=2,
                        help="How many days ahead to fetch (default 2)")
    args = parser.parse_args()

    engine = get_engine()
    ensure_table(engine)

    today = date.today()
    target_dates = [today + timedelta(days=i) for i in range(args.days_ahead + 1)]

    clear_existing_for_dates(engine, target_dates)

    all_rows = []
    for d in target_dates:
        try:
            schedule_data = fetch_schedule_for_date(d)
            df = parse_probable_pitchers(schedule_data)
            if not df.empty:
                all_rows.append(df)
                logger.info(f"  {d}: {len(df)} team-game rows ({df['pitcher_id'].notna().sum()} with probable pitcher)")
            else:
                logger.info(f"  {d}: no games")
        except Exception as exc:
            logger.error(f"  {d}: failed - {exc}")

    if not all_rows:
        logger.warning("No probable pitcher data fetched. Exiting.")
        return

    combined = pd.concat(all_rows, ignore_index=True)
    combined["fetched_at"] = pd.Timestamp.now()
    load_dataframe(combined, "dim_probable_pitchers", if_exists="append")
    logger.info(f"Wrote {len(combined)} rows to dim_probable_pitchers")

    # Quick summary
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT official_date,
                   COUNT(*) AS team_rows,
                   SUM(CASE WHEN pitcher_id IS NOT NULL THEN 1 ELSE 0 END) AS confirmed_pitchers
            FROM dbo.dim_probable_pitchers
            WHERE official_date >= CAST(GETDATE() AS DATE)
            GROUP BY official_date
            ORDER BY official_date
        """))
        logger.info("=" * 60)
        logger.info("Summary of upcoming probable pitchers in DB:")
        for row in result:
            logger.info(f"  {row.official_date}: {row.team_rows} team-rows, "
                        f"{row.confirmed_pitchers} probable pitchers known")


if __name__ == "__main__":
    main()
