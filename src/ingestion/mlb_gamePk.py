import pandas as pd
import requests
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)

def fetch_gamePk_with_dates(start_date: str=None, end_date: str=None) -> pd.DataFrame:
    url = (
        "https://statsapi.mlb.com/api/v1/schedule"
        f"?sportId=1&startDate={start_date}&endDate={end_date}&gameType=R"
    )

    logging.info(f"Fetching MLB schedule from {start_date} to {end_date}")

    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        schedule_data = response.json()

    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed: {e}")
        return pd.DataFrame()

    rows = []

    for d in schedule_data.get("dates", []):
        for g in d.get("games", []):
            rows.append({
                "gamePk": g.get("gamePk"),
                "gameDate": g.get("gameDate"),
                "officialDate": g.get("officialDate"),
                "gameTime": g.get("gameDate", "")[11:19] if g.get("gameDate") else None,
                "away_team": g.get("teams", {}).get("away", {}).get("team", {}).get("name"),
                "home_team": g.get("teams", {}).get("home", {}).get("team", {}).get("name"),
                "away_team_id": g.get("teams", {}).get("away", {}).get("team", {}).get("id"),
                "home_team_id": g.get("teams", {}).get("home", {}).get("team", {}).get("id"),
                "status": g.get("status", {}).get("detailedState")
            })

    df = pd.DataFrame(rows)
    logging.info(f"Collected {len(df)} games")

    return df

def fetch_completed_games(start_date:str, end_date:str)-> pd.DataFrame:
    df = fetch_gamePk_with_dates(start_date, end_date)

    if df.empty:
        logging.error("No games found")
        return df
    
    completed_statuses = {
        "Final",
        "Game Over",
        "Completed Early"
    }

    df = df[df["status"].isin(completed_statuses)].copy()

    logging.info(f"Filtered to {len(df)} completed games")

    return df

def fetch_yesterday_games() -> pd.DataFrame:
    yesterday = (datetime.utcnow() - timedelta(days=1)).date().isoformat()

    logging.info(f"Fetching yesterday's games: {yesterday}")

    return fetch_completed_games(yesterday, yesterday)




if __name__ == "__main__":
    df = fetch_completed_games("2026-03-28", "2026-03-28")
    print(df.head())
    print(df.columns.tolist())