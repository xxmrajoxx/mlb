import logging
from datetime import datetime, UTC
from typing import Optional

import pandas as pd
from pybaseball import (
    statcast_batter_expected_stats,
    statcast_batter_pitch_arsenal,
    statcast_batter_exitvelo_barrels,
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
# Batter supporting tables
# ---------------------------

def fetch_batter_expected_stats(season: int, min_pa: Optional[int] = None) -> pd.DataFrame:
    """
    Retrieves batter expected stats for a season.

    pybaseball:
    statcast_batter_expected_stats(year, minPA=[qualified])
    """
    kwargs = {}
    if min_pa is not None:
        kwargs["minPA"] = min_pa

    return _safe_fetch(
        fetch_func=statcast_batter_expected_stats,
        season=season,
        table_name="statcast_batter_expected_stats",
        **kwargs
    )


def fetch_batter_pitch_arsenal(season: int, min_pa: int = 25) -> pd.DataFrame:
    """
    Retrieves batter results split by pitch type for a season.

    pybaseball:
    statcast_batter_pitch_arsenal(year, minPA=25)
    """
    return _safe_fetch(
        fetch_func=statcast_batter_pitch_arsenal,
        season=season,
        table_name="statcast_batter_pitch_arsenal",
        minPA=min_pa
    )


def fetch_batter_exitvelo_barrels(season: int, min_bbe: Optional[int] = None) -> pd.DataFrame:
    """
    Retrieves batter exit velo / barrel stats for a season.

    pybaseball:
    statcast_batter_exitvelo_barrels(year, minBBE=[qualified])
    """
    kwargs = {}
    if min_bbe is not None:
        kwargs["minBBE"] = min_bbe

    return _safe_fetch(
        fetch_func=statcast_batter_exitvelo_barrels,
        season=season,
        table_name="statcast_batter_exitvelo_barrels",
        **kwargs
    )


def fetch_all_batting_support_tables(season: int) -> dict[str, pd.DataFrame]:
    """
    Fetch all recommended season-level batting support tables.
    """
    return {
        "batter_expected_stats": fetch_batter_expected_stats(season),
        "batter_pitch_arsenal": fetch_batter_pitch_arsenal(season),
        "batter_exitvelo_barrels": fetch_batter_exitvelo_barrels(season),
    }


if __name__ == "__main__":
    season = 2026

    batter_expected = fetch_batter_expected_stats(season)
    batter_pitch_arsenal = fetch_batter_pitch_arsenal(season, min_pa=25)
    batter_exitvelo = fetch_batter_exitvelo_barrels(season, min_bbe=50)

    print("batter_expected:", batter_expected.shape)
    print("batter_pitch_arsenal:", batter_pitch_arsenal.shape)
    print("batter_exitvelo:", batter_exitvelo.shape)


    batter_expected.to_csv("statcast_batter_expected_stats.csv", index=False)
    batter_pitch_arsenal.to_csv("statcast_batter_pitch_arsenal.csv", index=False)
    batter_exitvelo.to_csv("statcast_batter_exitvelo_barrels.csv", index=False)