import pandas as pd
import requests
import logging
import time

from core.mlb_gamePk import fetch_gamePk
from sql.sql_loader import load_dataframe

# to run the script .venv\Scripts\python.exe -m src.mlb_team_boxscore
 
logging.basicConfig(level=logging.INFO)

def fetch_team_boxscores(start_date: str, end_date: str, sleep_sec: float = 0.3) -> pd.DataFrame:
    schedule_df = fetch_gamePk(start_date, end_date)

    if schedule_df.empty:
        logging.warning("No games found for the given date range")
        return pd.DataFrame()

    rows = []

    for _, sched_row in schedule_df.iterrows():
        gamePk = sched_row["gamePk"]

        url = f"https://statsapi.mlb.com/api/v1/game/{gamePk}/boxscore"
        logging.info(f"Fetching gamePk={gamePk}")

        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            data = response.json()

        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed for gamePk={gamePk}: {e}")
            continue

        teams = data.get("teams", {})

        for side in ["away", "home"]:
            team_data = teams.get(side, {})
            team_info = team_data.get("team", {})
            team_stats = team_data.get("teamStats", {})

            row = {
                "gamePk": gamePk,
                "gameDate": sched_row.get("gameDate"),
                "officialDate": sched_row.get("officialDate"),
                "gameTime": sched_row.get("gameTime"),
                "away_team": sched_row.get("away_team"),
                "home_team": sched_row.get("home_team"),
                "side": side,
                "team_id": team_info.get("id"),
                "team_name": team_info.get("name"),
                "extract_date": pd.Timestamp.now()
            }

            for stat_group in ["batting", "pitching", "fielding"]:
                stats = team_stats.get(stat_group, {})
                for stat_name, stat_value in stats.items():
                    row[f"{stat_group}_{stat_name}"] = stat_value

            rows.append(row)

        time.sleep(sleep_sec)

    df = pd.DataFrame(rows)
    logging.info(f"Collected {len(df)} rows")

    return df


if __name__ == "__main__":
    df = fetch_team_boxscores("2026-05-01", "2026-05-01")
    # print(df.head())
    # print(df.columns.tolist())

    if not df.empty:
        load_dataframe(df, "fact_team_boxscore", if_exists="append")
    else:
        logging.warning("No data to load into fact_team_boxscore")