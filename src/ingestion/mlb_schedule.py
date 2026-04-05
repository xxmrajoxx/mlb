import pandas as pd
import requests
import json
import logging
from datetime import datetime

from sql.sql_loader import load_dataframe

logging.basicConfig(level=logging.INFO)

logging.info("Starting MLB schedule extraction")

def mlb_schedule()->pd.DataFrame:
     
    url = "https://statsapi.mlb.com/api/v1/schedule?sportId=1&season=2026"
    
    try: 
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

    except requests.exception.requestException as e:
        logging.error(f"Error: {e}")
        return pd.DataFrame()

    full_schedule = pd.json_normalize(
        data["dates"],
        record_path="games"
    ) 

    full_schedule.columns = full_schedule.columns.str.replace(".", "_", regex=False)

    if full_schedule.empty:
        logging.warning("No schedule data returned")
        return pd.DataFrame()

    load_dataframe(full_schedule, "mlb_schedule", if_exists="replace")
    logging.info(f"Loaded {len(full_schedule):,} rows into mlb_schedule")

    return full_schedule

if __name__=="__main__":
    df = mlb_schedule()
    





# print(json.dumps(data, indent=2))
# print(data.keys())
# print(data["dates"][0].keys())
# print(data["dates"][0]["games"][0].keys())
# print(json.dumps(data["dates"][0]["games"][0], indent=2))
# df = pd.json_normalize(data["dates"], record_path="games")
# print (df.columns)
# print (df.head())
