import requests
import pandas as pd
import time
import logging

url = "https://statsapi.mlb.com/api/v1/people/660271/stats?stats=gameLog&group=pitching&season=2025"
response = requests.get(url)
response.raise_for_status()

pitching = response.json()

print(pitching.keys())
# print(pitching["stats"][0].keys())




stats_list = pitching.get("stats", [])
if not stats_list:
    print("No pitching stats returned")
else:
    print(stats_list[0].keys())

# print(type(pitching["stats"]))
