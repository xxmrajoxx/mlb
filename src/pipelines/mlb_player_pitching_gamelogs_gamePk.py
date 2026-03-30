import requests
import pandas as pd
import time
import logging
import json
from datetime import UTC, datetime

from src.ingestion.mlb_player_id_all import fetch_active_mlb_players
from src.ingestion.mlb_gamePk import fetch_completed_games
from sql.sql_loader import load_dataframe, truncate_table, execute_sql

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

def fetch_player_game_logs(start_date: str, end_date: str, season: int = 2026)->pd.DataFrame:
    games_df = fetch_completed_games(start_date, end_date)

    if games_df.empty:
        logger.warning("No completed games found")
        return pd.DataFrame()
    
    team_ids = set(games_df["away_team_id"]).union(set(games_df["home_team_id"]))
    game_pks = set(games_df["gamePk"])

    player_df = fetch_active_mlb_players()

    if player_df.empty:
        logger.warning("No active players returned")
        return pd.DataFrame()

    player_df = player_df[
        (player_df["team_id"].isin(team_ids)) &
        (player_df["position"] == "P")
    ].copy()

    if player_df.empty:
        logger.warning("No pitcher found")
        return pd.DataFrame()
    
    logs = []
    
    for _, player_row in player_df.iterrows():
        player_id = player_row["player_id"]
        player_name = player_row["player_name"]
        team_id =  player_row["team_id"]
        team_name = player_row.get("team_name")
        team_position = player_row["position"]
        
        url = f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats?stats=gameLog&group=pitching&season={season}"
        
        logger.info(f"Fetching game logs for {player_name} ({player_id})")

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            pitching = response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for player_id={player_id}: {e}")
            continue

        stats_list = pitching.get("stats", [])
        if not stats_list:
            logger.warning(f"not splits foound for player_id = {player_id}")
            continue

        splits = stats_list[0].get("splits",[])
        if not splits:
            logger.warning("No player found")
            continue

        now_dt = datetime.now(UTC).replace(tzinfo=None)
        now_date = now_dt.date()

        for s in splits:
            stat = s.get("stat", {})
            game = s.get("game", {})                                
            game_pk = game.get("gamePk")
            game_date_raw = s.get("date")

            if game_pk not in game_pks:
                continue

            if not game_date_raw:
                continue

            game_date = pd.to_datetime(game_date_raw).date()

            row = {
                "player_id": player_id,
                "player_name": player_name,
                "position": team_position,
                "team_name": team_name,
                "team_id": team_id,
                "gamePk": game_pk,
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
        logger.warning("No player pitcing game logs collected")
        return pd.DataFrame()
    
    final_df = pd.DataFrame(logs).copy()
    return final_df

merge_sql = """
INSERT INTO mlb.dbo.fact_player_pitching_gamelogs (
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
    gamesStarted,
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
    numberOfPitches,
    era,
    inningsPitched,
    wins,
    losses,
    saves,
    saveOpportunities,
    holds,
    blownSaves,
    earnedRuns,
    whip,
    battersFaced,
    outs,
    gamesPitched,
    completeGames,
    shutouts,
    strikes,
    strikePercentage,
    hitBatsmen,
    balks,
    wildPitches,
    pickoffs,
    totalBases,
    groundOutsToAirouts,
    winPercentage,
    pitchesPerInning,
    gamesFinished,
    strikeoutWalkRatio,
    strikeoutsPer9Inn,
    walksPer9Inn,
    hitsPer9Inn,
    runsScoredPer9,
    homeRunsPer9,
    inheritedRunners,
    inheritedRunnersScored,
    catchersInterference,
    sacBunts,
    sacFlies,
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
    s.gamesStarted,
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
    s.numberOfPitches,
    s.era,
    s.inningsPitched,
    s.wins,
    s.losses,
    s.saves,
    s.saveOpportunities,
    s.holds,
    s.blownSaves,
    s.earnedRuns,
    s.whip,
    s.battersFaced,
    s.outs,
    s.gamesPitched,
    s.completeGames,
    s.shutouts,
    s.strikes,
    s.strikePercentage,
    s.hitBatsmen,
    s.balks,
    s.wildPitches,
    s.pickoffs,
    s.totalBases,
    s.groundOutsToAirouts,
    s.winPercentage,
    s.pitchesPerInning,
    s.gamesFinished,
    s.strikeoutWalkRatio,
    s.strikeoutsPer9Inn,
    s.walksPer9Inn,
    s.hitsPer9Inn,
    s.runsScoredPer9,
    s.homeRunsPer9,
    s.inheritedRunners,
    s.inheritedRunnersScored,
    s.catchersInterference,
    s.sacBunts,
    s.sacFlies,
    s.season,
    s.extract_date,
    s.extract_ts
FROM mlb.dbo.stg_player_pitching_gamelogs s
WHERE NOT EXISTS (
    SELECT 1
    FROM mlb.dbo.fact_player_pitching_gamelogs f
    WHERE f.player_id = s.player_id
      AND f.gamePk = s.gamePk
);
"""

def load_player_pitching_gamelogs(df: pd.DataFrame):
    if df.empty:
        logger.warning("No data to load into fact_player_pitching_gamelogs")
        return
    staging_table = "stg_player_pitching_gamelogs"

    logger.info("Loading staging table")
    load_dataframe(df, staging_table, if_exists="replace")

    logger.info("Merging staging into fact table")
    execute_sql(merge_sql)

    logger.info("Truncating staging table after merge")
    truncate_table(staging_table)
    

if __name__=="__main__":
    df = fetch_player_game_logs(season=2026,
                                start_date="2026-03-28",
                                end_date="2026-03-28")
    
    if df.empty:
        logger.warning("No dataframe returned from fetch_player_game_logs")
    else:
        print(df.head())
        print(df.shape)
        print(df.columns.tolist())

    load_player_pitching_gamelogs(df)




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