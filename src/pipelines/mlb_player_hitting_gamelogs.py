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
        team_id = player_row["team_id"]
        team_position = player_row["position"]
        team_name = player_row.get("team_name")

        url = f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats?stats=gameLog&group=hitting&season=2026"
        
        logging.info(f"Fetching game logs for {player_name} ({player_id})")
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            hitting = response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed for player_id={player_id}: {e}")
            continue

        stats_list = hitting.get("stats",[])

        if not stats_list:
            logging.warning(f"No stats for player_id={player_id}")
            continue

        splits = stats_list[0].get("splits",[])

        if not splits:
            logging.warning(f"No Splits for player_id={player_id}")
            continue

        for s in splits:
            stat = s.get("stat", {})                            # print(hitting["stats"][0]["splits"][0]
            game = s.get("game", {})                            # print(hitting["stats"][0]["splits"][0]
            game_date_raw = s.get("date")

            if not game_date_raw:
                continue

            game_date = pd.to_datetime(game_date_raw).date()

            row = {
                "player_id": player_id,
                "player_name": player_name,
                "position": team_position,
                "team_name": team_name,
                "team_id": team_id,
                "gamePk": game.get("gamePk"),
                "game_date": game_date,
                "dayNight": game.get("dayNight"),
                **stat
            }
            logs.append(row)

        time.sleep(0.2)
    
    return pd.DataFrame(logs)

if __name__ == "__main__":
    df = fetch_player_game_logs()
    print(df.head())
    print(df.shape)
    
    # today = datetime.today().strftime("%y%m%d")
    # df.to_csv(f"mlb_player_gamelogs_{today}.csv", index=False)









# print(hitting.keys())                             
# print(hitting["stats"][0].keys())                 
# print(hitting["stats"][0]["splits"][0].keys())

# hitting                               → dict  
# hitting["stats"]                      → list  
# hitting["stats"][0]                   → dict  
# hitting["stats"][0]["splits"]         → list  
# hitting["stats"][0]["splits"][0]      → dict  


# print(type(hitting["stats"][0]))


# all_columns = set()
# splits = hitting["stats"][0]["splits"]

# for row in splits:
#     stat = row.get("stat", {})
#     all_columns.update(stat.keys())

# df_columns = pd.DataFrame(sorted(all_columns))
# print(df_columns)