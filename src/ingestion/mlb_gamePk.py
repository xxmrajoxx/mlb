import pandas as pd
import requests
import logging

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
                "status": g.get("status", {}).get("detailedState")
            })

    df = pd.DataFrame(rows)
    logging.info(f"Collected {len(df)} games")

    return df



if __name__ == "__main__":
    df = fetch_gamePk_with_dates("2025-03-01", "2025-03-01")
    print(df.head())
    print(df.columns.tolist())