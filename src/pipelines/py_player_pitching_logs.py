import pandas as pd
import time
import logging
from datetime import datetime, date

from pybaseball import statcast_pitcher
from src.ingestion.mlb_player_id_team import fetch_single_team

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# https://github.com/jldbc/pybaseball/blob/master/docs/statcast_pitcher.md

def player_pitch_stats(start_dt: str, end_dt: str, team_id: int) -> pd.DataFrame:
    roster_df = fetch_single_team(team_id)

    # obtaining rooster of the team 
    if roster_df.empty:
        logging.warning(f"No player found for team_id={team_id}")
        return pd.DataFrame()

    # then filter by only selecting the pitcher     
    pitcher_df = roster_df[roster_df["position"] == "P"].copy()

    if pitcher_df.empty:
        logging.warning(f"No pitcher found for team_id={team_id}")
        return pd.DataFrame()

    all_pitch_data = []
    
    # then grabing the player_id and calling statcast_pitcher
    for _, row in pitcher_df.iterrows():            #_ - I dont care about this value ((index, row) → ignore index, only use row)
        player_id = row["player_id"]
        player_name = row["player_name"]

        logging.info(f"fetching statcast data for {player_name}, ({player_id})")

        try:
            df = statcast_pitcher(
                start_dt=start_dt,
                end_dt=end_dt,
                player_id=player_id   
            )
            if df is None or df.empty:
                logging.info(f" No statcast data for {player_name} ({player_id})")
                continue

            df = df.copy()

            meta_df = pd.DataFrame({
                "player_id": [player_id] * len(df),
                "player_name": [player_name] * len(df),
                "team_id": [team_id] * len(df),
            })

            df = pd.concat([df.reset_index(drop=True), meta_df], axis=1)

            all_pitch_data.append(df)

        except Exception as e:
            logging.error (f"failed for {player_name} ({player_id}): {e}")
            continue

        time.sleep(1)

    if not all_pitch_data:
        logging.warning("No pitching data collected")
        return pd.DataFrame()
    
    final_df = pd.concat(all_pitch_data, ignore_index=True)
    return final_df

if __name__=="__main__":
    df = player_pitch_stats(
        start_dt="2026-03-26",
        end_dt="2026-03-27",
        team_id=109
    )


    print(df.head())
    print(df.shape)

    # df.to_csv("abc.csv" index=False)


  