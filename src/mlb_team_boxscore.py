import pandas as pd
import requests
import logging
import time

from core.mlb_gamePk import fetch_gamePk
from sql.sql_loader import load_dataframe, truncate_table, execute_sql

logging.basicConfig(level=logging.INFO)

MERGE_SQL = """
MERGE mlb.dbo.fact_team_boxscore AS target
USING mlb.dbo.stg_fact_team_boxscore AS source
    ON target.gamePk = source.gamePk
   AND target.side = source.side

WHEN MATCHED THEN
    UPDATE SET
        target.gameDate = source.gameDate,
        target.officialDate = source.officialDate,
        target.gameTime = source.gameTime,
        target.away_team = source.away_team,
        target.home_team = source.home_team,
        target.team_id = source.team_id,
        target.team_name = source.team_name,
        target.extract_date = source.extract_date,
        target.batting_flyOuts = source.batting_flyOuts,
        target.batting_groundOuts = source.batting_groundOuts,
        target.batting_airOuts = source.batting_airOuts,
        target.batting_runs = source.batting_runs,
        target.batting_doubles = source.batting_doubles,
        target.batting_triples = source.batting_triples,
        target.batting_homeRuns = source.batting_homeRuns,
        target.batting_strikeOuts = source.batting_strikeOuts,
        target.batting_baseOnBalls = source.batting_baseOnBalls,
        target.batting_intentionalWalks = source.batting_intentionalWalks,
        target.batting_hits = source.batting_hits,
        target.batting_hitByPitch = source.batting_hitByPitch,
        target.batting_avg = source.batting_avg,
        target.batting_atBats = source.batting_atBats,
        target.batting_obp = source.batting_obp,
        target.batting_slg = source.batting_slg,
        target.batting_ops = source.batting_ops,
        target.batting_caughtStealing = source.batting_caughtStealing,
        target.batting_stolenBases = source.batting_stolenBases,
        target.batting_stolenBasePercentage = source.batting_stolenBasePercentage,
        target.batting_groundIntoDoublePlay = source.batting_groundIntoDoublePlay,
        target.batting_groundIntoTriplePlay = source.batting_groundIntoTriplePlay,
        target.batting_plateAppearances = source.batting_plateAppearances,
        target.batting_totalBases = source.batting_totalBases,
        target.batting_rbi = source.batting_rbi,
        target.batting_leftOnBase = source.batting_leftOnBase,
        target.batting_sacBunts = source.batting_sacBunts,
        target.batting_sacFlies = source.batting_sacFlies,
        target.batting_catchersInterference = source.batting_catchersInterference,
        target.batting_pickoffs = source.batting_pickoffs,
        target.batting_atBatsPerHomeRun = source.batting_atBatsPerHomeRun,
        target.batting_popOuts = source.batting_popOuts,
        target.batting_lineOuts = source.batting_lineOuts,
        target.pitching_flyOuts = source.pitching_flyOuts,
        target.pitching_groundOuts = source.pitching_groundOuts,
        target.pitching_airOuts = source.pitching_airOuts,
        target.pitching_runs = source.pitching_runs,
        target.pitching_doubles = source.pitching_doubles,
        target.pitching_triples = source.pitching_triples,
        target.pitching_homeRuns = source.pitching_homeRuns,
        target.pitching_strikeOuts = source.pitching_strikeOuts,
        target.pitching_baseOnBalls = source.pitching_baseOnBalls,
        target.pitching_intentionalWalks = source.pitching_intentionalWalks,
        target.pitching_hits = source.pitching_hits,
        target.pitching_hitByPitch = source.pitching_hitByPitch,
        target.pitching_atBats = source.pitching_atBats,
        target.pitching_obp = source.pitching_obp,
        target.pitching_caughtStealing = source.pitching_caughtStealing,
        target.pitching_stolenBases = source.pitching_stolenBases,
        target.pitching_stolenBasePercentage = source.pitching_stolenBasePercentage,
        target.pitching_caughtStealingPercentage = source.pitching_caughtStealingPercentage,
        target.pitching_numberOfPitches = source.pitching_numberOfPitches,
        target.pitching_era = source.pitching_era,
        target.pitching_inningsPitched = source.pitching_inningsPitched,
        target.pitching_saveOpportunities = source.pitching_saveOpportunities,
        target.pitching_earnedRuns = source.pitching_earnedRuns,
        target.pitching_whip = source.pitching_whip,
        target.pitching_battersFaced = source.pitching_battersFaced,
        target.pitching_outs = source.pitching_outs,
        target.pitching_completeGames = source.pitching_completeGames,
        target.pitching_shutouts = source.pitching_shutouts,
        target.pitching_pitchesThrown = source.pitching_pitchesThrown,
        target.pitching_balls = source.pitching_balls,
        target.pitching_strikes = source.pitching_strikes,
        target.pitching_strikePercentage = source.pitching_strikePercentage,
        target.pitching_hitBatsmen = source.pitching_hitBatsmen,
        target.pitching_balks = source.pitching_balks,
        target.pitching_wildPitches = source.pitching_wildPitches,
        target.pitching_pickoffs = source.pitching_pickoffs,
        target.pitching_groundOutsToAirouts = source.pitching_groundOutsToAirouts,
        target.pitching_rbi = source.pitching_rbi,
        target.pitching_pitchesPerInning = source.pitching_pitchesPerInning,
        target.pitching_runsScoredPer9 = source.pitching_runsScoredPer9,
        target.pitching_homeRunsPer9 = source.pitching_homeRunsPer9,
        target.pitching_inheritedRunners = source.pitching_inheritedRunners,
        target.pitching_inheritedRunnersScored = source.pitching_inheritedRunnersScored,
        target.pitching_catchersInterference = source.pitching_catchersInterference,
        target.pitching_sacBunts = source.pitching_sacBunts,
        target.pitching_sacFlies = source.pitching_sacFlies,
        target.pitching_passedBall = source.pitching_passedBall,
        target.pitching_popOuts = source.pitching_popOuts,
        target.pitching_lineOuts = source.pitching_lineOuts,
        target.fielding_caughtStealing = source.fielding_caughtStealing,
        target.fielding_stolenBases = source.fielding_stolenBases,
        target.fielding_stolenBasePercentage = source.fielding_stolenBasePercentage,
        target.fielding_caughtStealingPercentage = source.fielding_caughtStealingPercentage,
        target.fielding_assists = source.fielding_assists,
        target.fielding_putOuts = source.fielding_putOuts,
        target.fielding_errors = source.fielding_errors,
        target.fielding_chances = source.fielding_chances,
        target.fielding_passedBall = source.fielding_passedBall,
        target.fielding_pickoffs = source.fielding_pickoffs

WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        gamePk, gameDate, officialDate, gameTime, away_team, home_team, side,
        team_id, team_name, extract_date,
        batting_flyOuts, batting_groundOuts, batting_airOuts, batting_runs,
        batting_doubles, batting_triples, batting_homeRuns, batting_strikeOuts,
        batting_baseOnBalls, batting_intentionalWalks, batting_hits, batting_hitByPitch,
        batting_avg, batting_atBats, batting_obp, batting_slg, batting_ops,
        batting_caughtStealing, batting_stolenBases, batting_stolenBasePercentage,
        batting_groundIntoDoublePlay, batting_groundIntoTriplePlay, batting_plateAppearances,
        batting_totalBases, batting_rbi, batting_leftOnBase, batting_sacBunts,
        batting_sacFlies, batting_catchersInterference, batting_pickoffs,
        batting_atBatsPerHomeRun, batting_popOuts, batting_lineOuts,
        pitching_flyOuts, pitching_groundOuts, pitching_airOuts, pitching_runs,
        pitching_doubles, pitching_triples, pitching_homeRuns, pitching_strikeOuts,
        pitching_baseOnBalls, pitching_intentionalWalks, pitching_hits, pitching_hitByPitch,
        pitching_atBats, pitching_obp, pitching_caughtStealing, pitching_stolenBases,
        pitching_stolenBasePercentage, pitching_caughtStealingPercentage, pitching_numberOfPitches,
        pitching_era, pitching_inningsPitched, pitching_saveOpportunities, pitching_earnedRuns,
        pitching_whip, pitching_battersFaced, pitching_outs, pitching_completeGames,
        pitching_shutouts, pitching_pitchesThrown, pitching_balls, pitching_strikes,
        pitching_strikePercentage, pitching_hitBatsmen, pitching_balks, pitching_wildPitches,
        pitching_pickoffs, pitching_groundOutsToAirouts, pitching_rbi, pitching_pitchesPerInning,
        pitching_runsScoredPer9, pitching_homeRunsPer9, pitching_inheritedRunners,
        pitching_inheritedRunnersScored, pitching_catchersInterference, pitching_sacBunts,
        pitching_sacFlies, pitching_passedBall, pitching_popOuts, pitching_lineOuts,
        fielding_caughtStealing, fielding_stolenBases, fielding_stolenBasePercentage,
        fielding_caughtStealingPercentage, fielding_assists, fielding_putOuts,
        fielding_errors, fielding_chances, fielding_passedBall, fielding_pickoffs
    )
    VALUES (
        source.gamePk, source.gameDate, source.officialDate, source.gameTime, source.away_team, source.home_team, source.side,
        source.team_id, source.team_name, source.extract_date,
        source.batting_flyOuts, source.batting_groundOuts, source.batting_airOuts, source.batting_runs,
        source.batting_doubles, source.batting_triples, source.batting_homeRuns, source.batting_strikeOuts,
        source.batting_baseOnBalls, source.batting_intentionalWalks, source.batting_hits, source.batting_hitByPitch,
        source.batting_avg, source.batting_atBats, source.batting_obp, source.batting_slg, source.batting_ops,
        source.batting_caughtStealing, source.batting_stolenBases, source.batting_stolenBasePercentage,
        source.batting_groundIntoDoublePlay, source.batting_groundIntoTriplePlay, source.batting_plateAppearances,
        source.batting_totalBases, source.batting_rbi, source.batting_leftOnBase, source.batting_sacBunts,
        source.batting_sacFlies, source.batting_catchersInterference, source.batting_pickoffs,
        source.batting_atBatsPerHomeRun, source.batting_popOuts, source.batting_lineOuts,
        source.pitching_flyOuts, source.pitching_groundOuts, source.pitching_airOuts, source.pitching_runs,
        source.pitching_doubles, source.pitching_triples, source.pitching_homeRuns, source.pitching_strikeOuts,
        source.pitching_baseOnBalls, source.pitching_intentionalWalks, source.pitching_hits, source.pitching_hitByPitch,
        source.pitching_atBats, source.pitching_obp, source.pitching_caughtStealing, source.pitching_stolenBases,
        source.pitching_stolenBasePercentage, source.pitching_caughtStealingPercentage, source.pitching_numberOfPitches,
        source.pitching_era, source.pitching_inningsPitched, source.pitching_saveOpportunities, source.pitching_earnedRuns,
        source.pitching_whip, source.pitching_battersFaced, source.pitching_outs, source.pitching_completeGames,
        source.pitching_shutouts, source.pitching_pitchesThrown, source.pitching_balls, source.pitching_strikes,
        source.pitching_strikePercentage, source.pitching_hitBatsmen, source.pitching_balks, source.pitching_wildPitches,
        source.pitching_pickoffs, source.pitching_groundOutsToAirouts, source.pitching_rbi, source.pitching_pitchesPerInning,
        source.pitching_runsScoredPer9, source.pitching_homeRunsPer9, source.pitching_inheritedRunners,
        source.pitching_inheritedRunnersScored, source.pitching_catchersInterference, source.pitching_sacBunts,
        source.pitching_sacFlies, source.pitching_passedBall, source.pitching_popOuts, source.pitching_lineOuts,
        source.fielding_caughtStealing, source.fielding_stolenBases, source.fielding_stolenBasePercentage,
        source.fielding_caughtStealingPercentage, source.fielding_assists, source.fielding_putOuts,
        source.fielding_errors, source.fielding_chances, source.fielding_passedBall, source.fielding_pickoffs
    );
"""

def fetch_team_boxscores(start_date: str, end_date: str, sleep_sec: float = 0.3) -> pd.DataFrame:
    schedule_df = fetch_gamePk(start_date, end_date)

    if schedule_df.empty:
        logging.warning("No games found for the given date range")
        return pd.DataFrame()

    rows = []

    for _, sched_row in schedule_df.iterrows():
        gamePk = sched_row["gamePk"]

        url = f"https://statsapi.mlb.com/api/v1/game/{gamePk}/boxscore"
        logging.info(f"Fetching gamePk={gamePk}")

        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed for gamePk={gamePk}: {e}")
            continue

        teams = data.get("teams", {})

        for side in ["away", "home"]:
            team_data = teams.get(side, {})
            team_info = team_data.get("team", {})
            team_stats = team_data.get("teamStats", {})

            row = {
                "gamePk": gamePk,
                "gameDate": sched_row.get("gameDate"),
                "officialDate": sched_row.get("officialDate"),
                "gameTime": sched_row.get("gameTime"),
                "away_team": sched_row.get("away_team"),
                "home_team": sched_row.get("home_team"),
                "side": side,
                "team_id": team_info.get("id"),
                "team_name": team_info.get("name"),
                "extract_date": pd.Timestamp.now()
            }

            for stat_group in ["batting", "pitching", "fielding"]:
                stats = team_stats.get(stat_group, {})
                for stat_name, stat_value in stats.items():
                    row[f"{stat_group}_{stat_name}"] = stat_value

            rows.append(row)

        time.sleep(sleep_sec)

    df = pd.DataFrame(rows)
    logging.info(f"Collected {len(df)} rows")
    return df


if __name__ == "__main__":
    df = fetch_team_boxscores("2026-05-01", "2026-05-01")

    if not df.empty:
        truncate_table("stg_fact_team_boxscore", schema="dbo")
        load_dataframe(df, "stg_fact_team_boxscore", if_exists="append")
        execute_sql(MERGE_SQL)
        logging.info("Successfully merged data into fact_team_boxscore")
    else:
        logging.warning("No data to load into fact_team_boxscore")