import requests
import pandas as pd
import time
import logging
import json
from datetime import datetime

from src.ingestion.mlb_player_id_team import fetch_single_team

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

def fetch_player_game_logs()->pd.DataFrame:
    player_df = fetch_single_team(109)

    if player_df.empty:
        logging.warning("No team found")
        return pd.DataFrame()
    
    logs = []
    
    for _, player_row in player_df.iterrows():
        player_id = player_row["player_id"]
        player_name = player_row["player_name"]
        team_id =  player_row["team_id"]
        team_name = player_row.get("team_name")
        team_position = player_row["position"]
        
        url = f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats?stats=gameLog&group=pitching&season=2026"
        
        logging.info(f"Fetching game logs for {player_name} ({player_id})")

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            pitching = response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed for player_id={player_id}: {e}")
            continue

        stats_list = pitching.get("stats", [])

        if not stats_list:
            logging.warning(f"No stats for player_id={player_id}")
            continue

        spilts = stats_list[0].get("splits",[])

        if not stats_list:
            logging.warning(f"No splits for player_id={player_id}")
            continue

        for s in spilts:
            stat = s.get("stat", {})
            game = s.get("game", {})                                # why is it game?
            game_date_raw = s.get("date")

            if not game_date_raw:
                continue

            game_date = pd.to_datetime(game_date_raw).date()

            row = {
                "player_id": player_id,
                "player_name": player_name,
                "position": team_position,
                "team_name": team_id,
                "gamePk": game.get("gamePK"),
                "game_date": game_date,
                "dayNight": game.get("dayNight"),
                **stat
            }
            logs.append(row)

        time.sleep(0.2)
    
    return pd.DataFrame(logs)

if __name__=="__main__":
    df = fetch_player_game_logs()
    print(df.head())
    print(df.shape) 



# print(pitching.keys())
# print(pitching["stats"][0]["splits"][0].keys())
# print(pitching["stats"][0]["splits"][0]["opponent"].keys())

# stat_list = pitching.get("stats", [])
# if not stat_list:
#     print("No pitching data returned")
# else:
#     print(stat_list[0].keys())


# all_columns = set()
# spilts = pitching["stats"][0]["splits"]

# for s in spilts:
#     stats = s.get("stat", {})
#     all_columns.update(stats.keys())

# df_columns = pd.DataFrame(sorted(all_columns), columns=["column_name"])
# print(df_columns)