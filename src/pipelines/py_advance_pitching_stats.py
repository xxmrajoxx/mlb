import logging
from datetime import datetime, UTC
from typing import Optional

import pandas as pd
from pybaseball import (
    statcast_pitcher_expected_stats,
    statcast_pitcher_arsenal_stats,
    statcast_pitcher_pitch_arsenal,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def _add_lineage_columns(
    df: pd.DataFrame,
    season: int,
    table_name: str
) -> pd.DataFrame:
    """
    Add standard lineage / audit columns to a dataframe.
    """
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()
    now_utc = datetime.now(UTC)

    out["season"] = season
    out["source_table"] = table_name
    out["extract_date"] = now_utc.date()
    out["extract_ts_utc"] = now_utc

    return out


def _safe_fetch(fetch_func, season: int, table_name: str, **kwargs) -> pd.DataFrame:
    """
    Shared wrapper to run a pybaseball fetch function safely.
    """
    try:
        logger.info(f"Fetching {table_name} for season={season} with params={kwargs}")
        df = fetch_func(season, **kwargs)

        if df is None or df.empty:
            logger.warning(f"No data returned for {table_name}, season={season}")
            return pd.DataFrame()

        df = _add_lineage_columns(df, season=season, table_name=table_name)

        logger.info(f"Fetched {len(df):,} rows for {table_name}, season={season}")
        return df

    except Exception as e:
        logger.exception(f"Failed fetching {table_name} for season={season}: {e}")
        return pd.DataFrame()


# ---------------------------
# Pitching supporting tables
# ---------------------------

def fetch_pitcher_expected_stats(season: int, min_pa: Optional[int] = None) -> pd.DataFrame:
    """
    Retrieves pitcher expected stats for a season.

    pybaseball:
    statcast_pitcher_expected_stats(year, minPA=[qualified])

    min_pa:
        Minimum plate appearances against.
        If None, pybaseball will use qualified pitchers.
    """
    kwargs = {}
    if min_pa is not None:
        kwargs["minPA"] = min_pa

    return _safe_fetch(
        fetch_func=statcast_pitcher_expected_stats,
        season=season,
        table_name="statcast_pitcher_expected_stats",
        **kwargs
    )


def fetch_pitcher_arsenal_stats(season: int, min_pa: int = 25) -> pd.DataFrame:
    """
    Retrieves pitcher arsenal outcome stats for a season.

    pybaseball:
    statcast_pitcher_arsenal_stats(year, minPA=25)
    """
    return _safe_fetch(
        fetch_func=statcast_pitcher_arsenal_stats,
        season=season,
        table_name="statcast_pitcher_arsenal_stats",
        minPA=min_pa
    )


def fetch_pitcher_pitch_arsenal(
    season: int,
    min_p: Optional[int] = None,
    arsenal_type: str = "average_speed"
) -> pd.DataFrame:
    """
    Retrieves pitcher arsenal summary metrics for a season.

    pybaseball:
    statcast_pitcher_pitch_arsenal(year, minP=[qualified], arsenal_type="average_speed")

    Common arsenal_type examples from pybaseball docs include:
    - "average_speed"
    - "average_spin"
    - "n_"  (pitch usage share/count style output)

    min_p:
        Minimum pitches thrown.
        If None, pybaseball will use qualified pitchers.
    """
    kwargs = {"arsenal_type": arsenal_type}
    if min_p is not None:
        kwargs["minP"] = min_p

    return _safe_fetch(
        fetch_func=statcast_pitcher_pitch_arsenal,
        season=season,
        table_name="statcast_pitcher_pitch_arsenal",
        **kwargs
    )


# ---------------------------
# Convenience wrappers
# ---------------------------

def fetch_all_pitching_support_tables(season: int) -> dict[str, pd.DataFrame]:
    """
    Fetch all recommended season-level pitching support tables.
    """
    return {
        "pitcher_expected_stats": fetch_pitcher_expected_stats(season),
        "pitcher_arsenal_stats": fetch_pitcher_arsenal_stats(season),
        "pitcher_pitch_arsenal": fetch_pitcher_pitch_arsenal(season),
    }



if __name__ == "__main__":
    season = 2026

    pitcher_expected = fetch_pitcher_expected_stats(season)
    pitcher_arsenal_stats = fetch_pitcher_arsenal_stats(season, min_pa=25)
    pitcher_pitch_arsenal = fetch_pitcher_pitch_arsenal(season, min_p=200, arsenal_type="avg_speed")

    #"avg_speed"   # pitch velocity per pitch type
    # "avg_spin"    # spin rate per pitch type
    # "n_"          # pitch usage (counts / %)

    print("pitcher_expected:", pitcher_expected.shape)
    print("pitcher_arsenal_stats:", pitcher_arsenal_stats.shape)
    print("pitcher_pitch_arsenal:", pitcher_pitch_arsenal.shape)

    pitcher_expected.to_csv("statcast_pitcher_expected_stats.csv", index=False)
    pitcher_arsenal_stats.to_csv("statcast_pitcher_arsenal_stats.csv", index=False)
    pitcher_pitch_arsenal.to_csv("statcast_pitcher_pitch_arsenal.csv", index=False)
