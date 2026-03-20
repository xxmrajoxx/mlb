import pandas as pd
import requests
import json
import logging

from sql.sql_loader import load_dataframe

logging.basicConfig(level=logging.INFO)

logging.info("Starting MLB schedule extraction")

url = "https://statsapi.mlb.com/api/v1/schedule?sportId=1&season=2026"
response = requests.get(url)
response.raise_for_status()

data = response.json()

full_schedule = pd.json_normalize(
    data["dates"],
    record_path="games"
) 

full_schedule.columns = full_schedule.columns.str.replace(".", "_", regex=False)
load_dataframe(full_schedule, "mlb_schedule", if_exists="replace")


# print(json.dumps(data, indent=2))
# print(data.keys())
# print(data["dates"][0].keys())
# print(data["dates"][0]["games"][0].keys())
# print(json.dumps(data["dates"][0]["games"][0], indent=2))
# df = pd.json_normalize(data["dates"], record_path="games")
# print (df.columns)
# print (df.head())
