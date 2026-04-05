import pandas as pd
import logging

from src.ingestion.mlb_schedule import mlb_schedule
from sql.sql_loader import load_dataframe, truncate_table, execute_sql

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def create_mlb_standings_daily_table() -> None:
    """
    Create the standings table if it does not already exist.
    """

    create_sql = """
    IF OBJECT_ID('mlb.dbo.mlb_standings_daily', 'U') IS NULL
    BEGIN
        CREATE TABLE mlb.dbo.mlb_standings_daily (
            officialDate date NOT NULL,
            team_id int NOT NULL,
            team_name varchar(100) NOT NULL,
            wins int NULL,
            losses int NULL,
            win_pct decimal(6,3) NULL,
            extract_date date NULL
        );
    END;
    """

    execute_sql(create_sql)
    logger.info("Checked/created mlb.dbo.mlb_standings_daily")


def build_standings_dataframe(schedule_df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert MLB schedule data into one standings row per team per date.
    """

    if schedule_df.empty:
        logger.warning("Schedule dataframe is empty")
        return pd.DataFrame()

    required_away_cols = [
        "officialDate",
        "teams_away_team_id",
        "teams_away_team_name",
        "teams_away_leagueRecord_wins",
        "teams_away_leagueRecord_losses",
        "teams_away_leagueRecord_pct",
    ]

    required_home_cols = [
        "officialDate",
        "teams_home_team_id",
        "teams_home_team_name",
        "teams_home_leagueRecord_wins",
        "teams_home_leagueRecord_losses",
        "teams_home_leagueRecord_pct",
    ]

    missing_away = [col for col in required_away_cols if col not in schedule_df.columns]
    missing_home = [col for col in required_home_cols if col not in schedule_df.columns]

    if missing_away or missing_home:
        logger.error(
            f"Missing columns in schedule dataframe. "
            f"Away missing: {missing_away}, Home missing: {missing_home}"
        )
        return pd.DataFrame()

    away_df = schedule_df[required_away_cols].copy()
    away_df.columns = [
        "officialDate",
        "team_id",
        "team_name",
        "wins",
        "losses",
        "win_pct",
    ]

    home_df = schedule_df[required_home_cols].copy()
    home_df.columns = [
        "officialDate",
        "team_id",
        "team_name",
        "wins",
        "losses",
        "win_pct",
    ]

    standings_df = pd.concat([away_df, home_df], ignore_index=True)

    standings_df["officialDate"] = pd.to_datetime(
        standings_df["officialDate"], errors="coerce"
    ).dt.date
    standings_df["team_id"] = pd.to_numeric(standings_df["team_id"], errors="coerce")
    standings_df["wins"] = pd.to_numeric(standings_df["wins"], errors="coerce")
    standings_df["losses"] = pd.to_numeric(standings_df["losses"], errors="coerce")
    standings_df["win_pct"] = pd.to_numeric(standings_df["win_pct"], errors="coerce")

    standings_df["team_name"] = standings_df["team_name"].astype(str).str.strip()

    standings_df = standings_df.dropna(subset=["officialDate", "team_id", "team_name"])
    standings_df["team_id"] = standings_df["team_id"].astype(int)

    standings_df = standings_df.sort_values(
        by=["officialDate", "team_id", "wins", "losses"],
        ascending=[True, True, False, False]
    )

    standings_df = standings_df.drop_duplicates(
        subset=["officialDate", "team_id"],
        keep="first"
    ).reset_index(drop=True)

    standings_df["extract_date"] = pd.Timestamp.now().date()

    standings_df = standings_df[
        [
            "officialDate",
            "team_id",
            "team_name",
            "wins",
            "losses",
            "win_pct",
            "extract_date",
        ]
    ]

    logger.info(f"Built standings dataframe with {len(standings_df):,} rows")
    return standings_df


def run_mlb_standings_daily() -> pd.DataFrame:
    """
    Refresh mlb_schedule, build standings dataframe, and load directly
    into mlb.dbo.mlb_standings_daily.
    """

    logger.info("Starting MLB standings daily pipeline")

    create_mlb_standings_daily_table()

    schedule_df = mlb_schedule()

    if schedule_df is None or schedule_df.empty:
        logger.warning("No schedule data returned from mlb_schedule()")
        return pd.DataFrame()

    if "seriesDescription" in schedule_df.columns:
        schedule_df = schedule_df[
            schedule_df["seriesDescription"].astype(str).str.lower() == "regular season"
        ].copy()

    if schedule_df.empty:
        logger.warning("No regular season rows found in schedule dataframe")
        return pd.DataFrame()

    standings_df = build_standings_dataframe(schedule_df)

    if standings_df.empty:
        logger.warning("Standings dataframe is empty")
        return pd.DataFrame()

    truncate_table("mlb_standings_daily", schema="dbo")
    load_dataframe(standings_df, "mlb_standings_daily", if_exists="append")

    logger.info(f"Loaded {len(standings_df):,} rows into mlb.dbo.mlb_standings_daily")
    logger.info("MLB standings daily pipeline completed successfully")

    return standings_df


if __name__ == "__main__":
    df = run_mlb_standings_daily()
    print(df.head())