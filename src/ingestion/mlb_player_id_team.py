import pandas as pd
import json
import time
import logging
import requests

# url = "https://statsapi.mlb.com/api/v1/teams?sportId=1"
# responses = requests.get(url)
# responses.raise_for_status()
# data = responses.json()
# team_df = pd.DataFrame(data["teams"])
# print(team_df[["id", "name"]])

def fetch_single_team(team_id: str)-> pd.DataFrame:
 #   team_id = "133"
    url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster"
    responses = requests.get(url)
    responses.raise_for_status()
    data = responses.json()

    players = []

    for p in data.get("roster", []):
        players.append({
            "team_id": team_id,
            "player_id": p["person"]["id"],
            "player_name": p["person"]["fullName"],
            "position": p["position"]["abbreviation"]
        })

    return pd.DataFrame(players)

df = fetch_single_team(133)

print(df.head())







# team_ids = [team["id"] for team in data["teams"]]
# print(json.dumps(data, indent=2))
# print(data.keys())
# print(data["teams"][0].keys())
# print(json.dumps(data["teams"][0]))

#      id                   name
# 0   133              Athletics
# 1   134     Pittsburgh Pirates
# 2   135       San Diego Padres
# 3   136       Seattle Mariners
# 4   137   San Francisco Giants
# 5   138    St. Louis Cardinals
# 6   139         Tampa Bay Rays
# 7   140          Texas Rangers
# 8   141      Toronto Blue Jays
# 9   142        Minnesota Twins
# 10  143  Philadelphia Phillies
# 11  144         Atlanta Braves
# 12  145      Chicago White Sox
# 13  146          Miami Marlins
# 14  147       New York Yankees
# 15  158      Milwaukee Brewers
# 16  108     Los Angeles Angels
# 17  109   Arizona Diamondbacks
# 18  110      Baltimore Orioles
# 19  111         Boston Red Sox
# 20  112           Chicago Cubs
# 21  113        Cincinnati Reds
# 22  114    Cleveland Guardians
# 23  115       Colorado Rockies
# 24  116         Detroit Tigers
# 25  117         Houston Astros
# 26  118     Kansas City Royals
# 27  119    Los Angeles Dodgers
# 28  120   Washington Nationals
# 29  121          New York Mets