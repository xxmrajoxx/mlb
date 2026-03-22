import pandas as pd
import requests
import time
import logging

from src.ingestion.mlb_team_id import fetch_team_id

logging.basicConfig(level=logging.INFO)

SEASONS = [2026, 2025, 2024, 2023]

def fetch_team_hitting_average() -> pd.DataFrame:
    team_df = fetch_team_id()

    if team_df.empty:
        logging.warning("No team ids found")
        return pd.DataFrame()

    logs = []

    for _, team_row in team_df.iterrows():
        team_id = team_row["team_id"]
        team_name = team_row["team_name"]

        for season in SEASONS:
            url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}/stats?stats=season&group=hitting&season={season}"

            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                data = response.json()
            except requests.exceptions.RequestException as e:
                logging.error(f"Request failed for team_id={team_id}, season={season}: {e}")
                continue

            splits = data.get("stats", [{}])[0].get("splits", [])

            if not splits:
                logging.warning(f"No data returned for {team_name} ({team_id}) in {season}")
                continue

            for s in splits:
                stat = s.get("stat", {})

                row = {
                    "team_id": team_id,
                    "team_name": team_name,
                    "season": s.get("season", season),
                    **stat
                }
                logs.append(row)

            time.sleep(0.2)

    return pd.DataFrame(logs)


if __name__ == "__main__":
    logging.info("Fetching team hitting averages")
    df = fetch_team_hitting_average()

    print(df.head())
    print(df.shape)










# url = "https://statsapi.mlb.com/api/v1/teams/147/stats?stats=season&group=hitting&season=2025"
# response = requests.get(url)
# response.raise_for_status()
# data = response.json()
# print(json.dumps(data, indent=2))
# print(data.keys())
# print(data["stats"][0].keys())
# print(data["stats"][0]["splits"][0].keys())
# print(data["stats"][0]["splits"][0]["stat"].keys())

        
# all_columns = set()                 # not using [] to prevents duplicates and perfect for API data and best way to get unique column names and also you can also add columns yourself "all_columns.update(["season", "team_id", "team_name"])"
# splits = data["stats"][0]["splits"]  # stats and splits are keys from the API

# for row in splits:
#     stat = row.get("stat", {})
#     all_columns.update(stat.keys())     # update = dd all these keys into the set / Each key added individually

# #1
# # print(sorted(all_columns))
# #2
# df_columns = pd.DataFrame(sorted(all_columns), columns=["column_name"])
# print(df_columns)
#3
# rows = [row.get("stat",{}) for row in splits]
# df = pd.DataFrame(rows)
# print(df.head())
# print(df.columns)

