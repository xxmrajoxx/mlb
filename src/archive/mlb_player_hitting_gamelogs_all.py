import requests
import pandas as pd
import time
import logging
import json
from datetime import datetime, UTC

from src.ingestion.mlb_player_id_team import fetch_single_team
from src.ingestion.mlb_player_id_all import fetch_active_mlb_players
from sql.sql_loader import load_dataframe, truncate_table, execute_sql

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def fetch_player_game_logs(season: int = 2026)->pd.DataFrame:
    player_df = fetch_active_mlb_players()

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

        url = f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats?stats=gameLog&group=hitting&season={season}"
        
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

        now_dt = datetime.now(UTC).replace(tzinfo=None)
        now_date = now_dt.date()

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
                **stat,
                "season": str(season),
                "extract_date": now_date,
                "extract_ts": now_dt,
            }
            logs.append(row)

        time.sleep(0.2)

    if not logs:
        logger.warning("No player hitting game logs collected")
        return pd.DataFrame(logs)
    
    final_df = pd.DataFrame(logs).copy()
    return final_df 

merge_sql = """
INSERT INTO mlb.dbo.fact_player_hitting_gamelogs (
    player_id,
    player_name,
    position,
    team_name,
    team_id,
    gamePk,
    game_date,
    dayNight,
    summary,
    gamesPlayed,
    flyOuts,
    groundOuts,
    airOuts,
    runs,
    doubles,
    triples,
    homeRuns,
    strikeOuts,
    baseOnBalls,
    intentionalWalks,
    hits,
    hitByPitch,
    avg,
    atBats,
    obp,
    slg,
    ops,
    caughtStealing,
    stolenBases,
    stolenBasePercentage,
    caughtStealingPercentage,
    groundIntoDoublePlay,
    groundIntoTriplePlay,
    numberOfPitches,
    plateAppearances,
    totalBases,
    rbi,
    leftOnBase,
    sacBunts,
    sacFlies,
    babip,
    groundOutsToAirouts,
    catchersInterference,
    atBatsPerHomeRun,
    season,
    extract_date,
    extract_ts
)
SELECT
    s.player_id,
    s.player_name,
    s.position,
    s.team_name,
    s.team_id,
    s.gamePk,
    s.game_date,
    s.dayNight,
    s.summary,
    s.gamesPlayed,
    s.flyOuts,
    s.groundOuts,
    s.airOuts,
    s.runs,
    s.doubles,
    s.triples,
    s.homeRuns,
    s.strikeOuts,
    s.baseOnBalls,
    s.intentionalWalks,
    s.hits,
    s.hitByPitch,
    s.avg,
    s.atBats,
    s.obp,
    s.slg,
    s.ops,
    s.caughtStealing,
    s.stolenBases,
    s.stolenBasePercentage,
    s.caughtStealingPercentage,
    s.groundIntoDoublePlay,
    s.groundIntoTriplePlay,
    s.numberOfPitches,
    s.plateAppearances,
    s.totalBases,
    s.rbi,
    s.leftOnBase,
    s.sacBunts,
    s.sacFlies,
    s.babip,
    s.groundOutsToAirouts,
    s.catchersInterference,
    s.atBatsPerHomeRun,
    s.season,
    s.extract_date,
    s.extract_ts
FROM mlb.dbo.stg_player_hitting_gamelogs s
WHERE NOT EXISTS (
    SELECT 1
    FROM mlb.dbo.fact_player_hitting_gamelogs f
    WHERE f.player_id = s.player_id
      AND f.gamePk = s.gamePk
);
"""

def load_player_hitting_gamelogs(df: pd.DataFrame):
    if df.empty:
        logger.warning("Dataframe is empty - nothing to load")
        return
    
    staging_table = "stg_player_hitting_gamelogs"

    logger.info("Loading staging table")
    load_dataframe(df, staging_table, if_exists="replace")

    logger.info("Merging staging into fact table")
    execute_sql(merge_sql)

    logger.info("Truncating staging table after merge")
    truncate_table(staging_table)

if __name__ == "__main__":
    df = fetch_player_game_logs(season=2026)
    print(df.head())
    print(df.shape)
    print(df.columns.to_list())

    load_player_hitting_gamelogs(df)
    
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