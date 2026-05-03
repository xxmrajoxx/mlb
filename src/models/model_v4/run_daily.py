"""
Daily betting workflow runner.

Runs the full future-game pipeline in one command:
    1. Fetch probable pitchers from MLB Stats API
    2. Score future games using trained models
    3. Print a summary of upcoming bets ready for odds entry

Usage:
    python run_daily.py [--days_ahead 2]

After this completes, you:
    1. Open Sportsbet, find each pitcher's K prop
    2. UPDATE fact_pitcher_strikeout_betting_ev with sportsbook, line, over_odds, under_odds
    3. Run sql/compute_ev.sql in SSMS
    4. Place the recommended bets
"""
from __future__ import annotations
import argparse
import logging
import subprocess
import sys

import fetch_probable_pitchers
import pipeline as pipeline_module
import score_future_games

logger = logging.getLogger("daily_runner")
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days_ahead", type=int, default=2,
                        help="Days ahead to fetch probable pitchers (default 2)")
    parser.add_argument("--skip_fetch", action="store_true",
                        help="Skip the MLB API fetch step (use if already up to date)")
    parser.add_argument("--recalibrate", action="store_true",
                        help="Re-fit calibrators on completed 2026 games before scoring "
                             "(run weekly, needs ~3-4 weeks of 2026 data)")
    args = parser.parse_args()

    logger.info("=" * 70)
    logger.info("MLB STRIKEOUT - DAILY FUTURE GAMES WORKFLOW")
    logger.info("=" * 70)

    # Step 1: Fetch probable pitchers
    if not args.skip_fetch:
        logger.info("\n>>> STEP 1: Fetching probable pitchers from MLB Stats API")
        sys.argv = ["fetch_probable_pitchers.py", "--days_ahead", str(args.days_ahead)]
        fetch_probable_pitchers.main()
    else:
        logger.info("\n>>> STEP 1: SKIPPED (--skip_fetch)")

    # Optional: re-calibrate on current-season completed games
    if args.recalibrate:
        logger.info("\n>>> STEP 1b: Recalibrating calibrators on completed 2026 games")
        pipeline_module.recalibrate_on_current_season()

    # Step 2: Score future games
    logger.info("\n>>> STEP 2: Scoring future games with trained model")
    score_future_games.run()

    # Step 3: Tell the user what to do next
    logger.info("\n" + "=" * 70)
    logger.info("DONE. Next steps in SSMS:")
    logger.info("=" * 70)
    logger.info("")
    logger.info("1. Look at upcoming games:")
    logger.info("")
    logger.info("   SELECT pitcher_name, opponent_team_name, game_date,")
    logger.info("          predicted_strikeouts, predicted_k_stddev,")
    logger.info("          opp_same_side_count, opp_opposite_side_count")
    logger.info("   FROM mlb.dbo.fact_pitcher_strikeout_betting_ev")
    logger.info("   WHERE game_date >= CAST(GETDATE() AS DATE)")
    logger.info("     AND sportsbook IS NULL")
    logger.info("   ORDER BY game_date, predicted_strikeouts DESC;")
    logger.info("")
    logger.info("2. Open Sportsbet, find each pitcher's strikeout prop")
    logger.info("")
    logger.info("3. Enter odds for each pitcher you want to consider:")
    logger.info("")
    logger.info("   UPDATE mlb.dbo.fact_pitcher_strikeout_betting_ev")
    logger.info("   SET sportsbook = 'Sportsbet', line = 5.5,")
    logger.info("       over_odds = 1.90, under_odds = 1.92")
    logger.info("   WHERE gamePk = ??? AND pitcher_id = ???;")
    logger.info("")
    logger.info("4. Run sql/compute_ev.sql to compute edge / EV / Kelly")
    logger.info("")
    logger.info("5. Place bets only on rows where recommended_side = OVER or UNDER")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
