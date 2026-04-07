import pandas as pd
import requests
import logging
import os
from dotenv import load_dotenv
import datetime


logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

load_dotenv(override=True)

def fetch_standings(season: int)->pd.DataFrame:
    url = f"https://statsapi.mlb.com/api/v1/standings?leagueId=103,104&season={season}"
    
    logger.info(f"fetching standings for season {season}")

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        schedule = response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Error with obtaining standings data for season {season}: {e}")
        return pd.DataFrame()
    
    standings = pd.json_normalize(
        schedule["records"],
        record_path=["teamRecords"],
    )

    return standings
    



if __name__=="__main__":
    season = os.getenv("SEASON")

    if not season:
        logger.error("SEASON environment variable not set")
    else:
        season = int(season)
        df = fetch_standings(season=season)
        print(df.columns.tolist())
        print(df.head())


        