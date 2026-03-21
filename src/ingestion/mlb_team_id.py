import requests
import pandas as pd
import json
import time
import logging 

from sql.sql_loader import load_dataframe

def fetch_team_id()->pd.DataFrame:
    try:
        url = "https://statsapi.mlb.com/api/v1/teams?sportId=1"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed: {e}")
        return pd.DataFrame()


    teams = data["teams"]           # the ["teams"] this is from print(json.dumps(data, indent=2))

    if not teams:
        logging.warning("No teams returned from API")
        return pd.DataFrame()

    rows = []

    for team in teams:
        rows.append({
            "team_id": team.get("id"),
            "team_name": team.get("name"),
            "team_abbr": team.get("abbreviation"),
            "team_code": team.get("teamCode"),
            "league": team.get("league", {}).get("name"),
            "division": team.get("division", {}).get("name"),
        })

    df = pd.DataFrame(rows)                     

    df = df.drop_duplicates(subset=["team_id"])
    logging.info(f"Fetched {len(df)} teams")        
    return df                                   # Make sure this is not in the loop or it will only return 1 row
        
    

if __name__== "__main__":
    logging.info(f'loading teams data')
    df = fetch_team_id()
    load_dataframe(df, "dim_team", if_exists="replace")

    logging.info(f"completed")

    print(df.head())
    print(df.shape)
    