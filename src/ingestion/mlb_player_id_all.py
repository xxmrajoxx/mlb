import requests
import logging
import pandas as pd
import time 
import json

logging.basicConfig(level=logging.INFO)


def fetch_active_mlb_players():
    team_url = "https://statsapi.mlb.com/api/v1/teams?sportId=1"
    response = requests.get(team_url)
    response.raise_for_status()
    teams = response.json()["teams"] 

    logging.info(f"found {len(teams)} teams")

    players = []
    for team in teams:
        team_id = team["id"]                        # Extract the team ID using loop variable using api (dic)
        team_name = team["name"]                    # Extract the team name using loop variable using api (dic)

        logging.info(f"Fetching roster for: {team_name}")

        roster_url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster"
        roster_response = requests.get(roster_url)
        roster_response.raise_for_status()
        roster = roster_response.json()["roster"]

        logging.info(f"{team_name} roster size: {len(roster)} players")

        time.sleep(0.3)

        for player in roster:
            person = player.get("person",{})                    # Extract the person using keys dict
            position = player.get("position",{})                # Extract their position using keys dict
            

            players.append({                                        # Add one player row to the list
                "player_id": person.get("id"),
                "player_name": person.get("fullName"),
                "team_id": team_id,
                "team_name": team_name,
                "position_name": position.get("name"),
                "position_type": position.get("type"),
                "position": position.get("abbreviation"),
                "link": person.get("link"),                           
                "status": player["status"]["description"],          # Not using key as variable 
                "status_2": player["status"]["code"],                
                            
            })

        logging.info(f"Total players collected: {len(players)}")
        
    return pd.DataFrame(players)




if __name__ == "__main__":

    logging.info("Starting MLB player extraction")

    df = fetch_active_mlb_players()

    logging.info("Extraction complete")    

    print(df.head())
    print(df.shape)