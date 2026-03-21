import requests
import pandas as pd
import json
import logging
import time

from src.ingestion.mlb_player_id_all import fetch_active_mlb_players
from src.ingestion.mlb_player_id_indiv import fetch_single_team
from sql.sql_loader import load_dataframe

logging.basicConfig(level=logging.INFO)

def fetch_player_bio()-> pd.DataFrame:

    players_df = fetch_active_mlb_players()                              # without () it is the function and not the dataframe
    players_ids = players_df["player_id"].dropna().tolist()              # convert column to a python list

    # for player_id in players_ids:
    #     print(player_id)
    logging.info(f"Found {len(players_ids)} Player IDs")

    players = []
    for player_id in players_ids:
        url = f"https://statsapi.mlb.com/api/v1/people/{player_id}"         # make sure its a f string
    
        logging.info(f"Fetching bio for player_id={player_id}")

        try: 
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            data = response.json()

            people_list = data.get("people",[])

            if not people_list:
                logging.warning(f"No bio returned for player_id={player_id}")       # make sure its the loop
                continue


            person = people_list[0]
            primary_position = person.get("primaryPosition", {})
            bat_side = person.get("batSide", {})
            pitch_hand = person.get("pitchHand", {})
            current_team = person.get("currentTeam", {})

            players.append({
                "player_id": person.get("id"),
                "full_name": person.get("fullName"),
                "first_name": person.get("firstName"),
                "last_name": person.get("lastName"),
                "use_name": person.get("useName"),
                "middle_name": person.get("middleName"),
                "primary_number": person.get("primaryNumber"),
                "birth_date": person.get("birthDate"),
                "current_age": person.get("currentAge"),
                "birth_city": person.get("birthCity"),
                "birth_state_province": person.get("birthStateProvince"),
                "birth_country": person.get("birthCountry"),
                "height": person.get("height"),
                "weight": person.get("weight"),
                "active": person.get("active"),
                "mlb_debut_date": person.get("mlbDebutDate"),
                "bat_side_code": bat_side.get("code"),
                "bat_side_desc": bat_side.get("description"),
                "pitch_hand_code": pitch_hand.get("code"),
                "pitch_hand_desc": pitch_hand.get("description"),
                "primary_position_code": primary_position.get("code"),
                "primary_position_name": primary_position.get("name"),
                "primary_position_type": primary_position.get("type"),
                "primary_position_abbreviation": primary_position.get("abbreviation"),
                "current_team_id": current_team.get("id"),
                "current_team_name": current_team.get("name"),
                "strike_zone_top": person.get("strikeZoneTop"),
                "strike_zone_bottom": person.get("strikeZoneBottom"),
            })


            time.sleep(0.3)

        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed for player_id={player_id}: {e}")
            continue


    return pd.DataFrame(players)

if __name__ == "__main__":
    
    logging.info("Starting MLB player bio extractionand obtaining total number of")
    df = fetch_player_bio()
    load_dataframe(df, "dim_player", if_exists="replace")

    logging.info("Completed")


    # print(df.head())
    # print(df.shape)




    # df.to_csv("mlb_player_bio.csv", index=False)
    # print(data.keys())
    # print(data["people"][0].keys())
    # print(json.dumps(data["people"][0], indent=2))



    # 1. obtain player id via mlb_player_id.py
    # 2. select a random player and use to identity key and structure 