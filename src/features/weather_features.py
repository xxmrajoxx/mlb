import pandas as pd
import logging
import os
from datetime import timedelta
from meteostat import Point, hourly
from sqlalchemy import create_engine, text

# =========================
# logging
# =========================
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/mlb_pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# =========================
# SQL Server helpers
# =========================
SERVER = "localhost"
DATABASE = "mlb"
DRIVER = "ODBC Driver 17 for SQL Server"

def get_engine():
    connection_string = (
        f"mssql+pyodbc://@{SERVER}/{DATABASE}"
        f"?driver={DRIVER.replace(' ', '+')}"
        "&trusted_connection=yes"
    )
    engine = create_engine(connection_string)
    logger.info("SQL Server engine created")
    return engine

engine = get_engine()

def load_dataframe(df, table_name, if_exists="append"):
    try:
        logger.info(f"Starting load for table {table_name}")
        df.to_sql(table_name, con=engine, if_exists=if_exists, index=False)
        logger.info(f"Loaded {len(df)} rows into {table_name}")
    except Exception as e:
        logger.error(f"Error loading data into {table_name}: {e}")
        raise

def truncate_table(table_name: str, schema: str = "dbo"):
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {schema}.{table_name}"))
    logger.info(f"Truncated table {schema}.{table_name}")

def execute_sql(sql: str):
    with engine.begin() as conn:
        conn.execute(text(sql))
    logger.info("SQL executed successfully")


# =========================
# weather pipeline helpers
# =========================
def fetch_schedule_for_weather() -> pd.DataFrame:
    """
    Pull only required columns from mlb_schedule.
    """
    sql = """
    SELECT
        gamePk,
        CAST(gameDate AS datetime2) AS gameDateUtc,
        CAST(officialDate AS date) AS officialDate,
        venue_id,
        venue_name
    FROM mlb.dbo.mlb_schedule
    WHERE gamePk IS NOT NULL
      AND officialDate IS NOT NULL
      AND venue_id IS NOT NULL
      AND venue_name IS NOT NULL
    """
    logger.info("Fetching schedule data for weather join")
    return pd.read_sql(sql, engine)


def fetch_venue_lookup() -> pd.DataFrame:
    """
    Pull venue weather mapping table.
    Assumes you create this table manually once.
    """
    sql = """
    SELECT
        venue_id,
        venue_name,
        latitude,
        longitude,
        elevation_m,
        roof_type
    FROM mlb.dbo.dim_venue_weather
    """
    logger.info("Fetching venue lookup table")
    return pd.read_sql(sql, engine)


def fetch_hourly_weather(lat: float, lon: float, elevation_m, game_dt_utc) -> dict:
    """
    Fetch historical hourly weather from Meteostat for the game hour.
    """
    try:
        point = Point(lat, lon, elevation_m if pd.notna(elevation_m) else None)

        start = pd.Timestamp(game_dt_utc).to_pydatetime().replace(
            minute=0, second=0, microsecond=0
        )
        end = start + timedelta(hours=1)

        weather_df = hourly(point, start, end).fetch()

        if weather_df.empty:
            return {
                "game_hour_utc": None,
                "temp_c": None,
                "dewpoint_c": None,
                "relative_humidity": None,
                "wind_speed_kph": None,
                "wind_direction_deg": None,
                "precip_mm": None,
                "pressure_hpa": None,
                "weather_missing": 1
            }

        row = weather_df.iloc[0]

        return {
            "game_hour_utc": int(weather_df.index[0].hour) if len(weather_df.index) > 0 else None,
            "temp_c": None if pd.isna(row.get("temp")) else round(float(row.get("temp")), 2),
            "dewpoint_c": None if pd.isna(row.get("dwpt")) else round(float(row.get("dwpt")), 2),
            "relative_humidity": None if pd.isna(row.get("rhum")) else round(float(row.get("rhum")), 2),
            "wind_speed_kph": None if pd.isna(row.get("wspd")) else round(float(row.get("wspd")), 2),
            "wind_direction_deg": None if pd.isna(row.get("wdir")) else round(float(row.get("wdir")), 2),
            "precip_mm": None if pd.isna(row.get("prcp")) else round(float(row.get("prcp")), 2),
            "pressure_hpa": None if pd.isna(row.get("pres")) else round(float(row.get("pres")), 2),
            "weather_missing": 0
        }

    except Exception as e:
        logger.error(f"Weather fetch failed for lat={lat}, lon={lon}: {e}")
        return {
            "game_hour_utc": None,
            "temp_c": None,
            "dewpoint_c": None,
            "relative_humidity": None,
            "wind_speed_kph": None,
            "wind_direction_deg": None,
            "precip_mm": None,
            "pressure_hpa": None,
            "weather_missing": 1
        }


def build_game_weather_dataframe() -> pd.DataFrame:
    """
    Build lean game weather dataframe by joining:
    mlb_schedule + dim_venue_weather
    """
    schedule_df = fetch_schedule_for_weather()
    venue_df = fetch_venue_lookup()

    df = schedule_df.merge(
        venue_df,
        on=["venue_id", "venue_name"],
        how="inner"
    ).copy()

    logger.info(f"Joined schedule + venue lookup: {len(df)} rows")

    records = []

    for row in df.itertuples(index=False):
        logger.info(f"Fetching weather for gamePk={row.gamePk} | {row.officialDate} | {row.venue_name}")

        roof_type = str(row.roof_type).lower() if pd.notna(row.roof_type) else None
        is_dome = 1 if roof_type == "dome" else 0

        # If dome, weather is not very relevant
        if is_dome == 1:
            weather = {
                "game_hour_utc": None,
                "temp_c": None,
                "dewpoint_c": None,
                "relative_humidity": None,
                "wind_speed_kph": None,
                "wind_direction_deg": None,
                "precip_mm": None,
                "pressure_hpa": None,
                "weather_missing": 0
            }
        else:
            weather = fetch_hourly_weather(
                lat=float(row.latitude),
                lon=float(row.longitude),
                elevation_m=row.elevation_m,
                game_dt_utc=row.gameDateUtc
            )

        records.append({
            "gamePk": int(row.gamePk),
            "officialDate": row.officialDate,
            "venue_id": int(row.venue_id),
            "venue_name": row.venue_name,
            "roof_type": row.roof_type,
            "is_dome": is_dome,
            "game_hour_utc": weather["game_hour_utc"],
            "temp_c": weather["temp_c"],
            "dewpoint_c": weather["dewpoint_c"],
            "relative_humidity": weather["relative_humidity"],
            "wind_speed_kph": weather["wind_speed_kph"],
            "wind_direction_deg": weather["wind_direction_deg"],
            "precip_mm": weather["precip_mm"],
            "pressure_hpa": weather["pressure_hpa"],
            "weather_missing": weather["weather_missing"],
            "weather_source": "meteostat_hourly"
        })

    out_df = pd.DataFrame(records)
    out_df = out_df.drop_duplicates(subset=["gamePk"]).reset_index(drop=True)

    logger.info(f"Built fact_game_weather dataframe with {len(out_df)} rows")
    return out_df


def create_fact_game_weather_table():
    """
    Create target table if it does not exist.
    """
    sql = """
    IF OBJECT_ID('mlb.dbo.fact_game_weather', 'U') IS NULL
    BEGIN
        CREATE TABLE mlb.dbo.fact_game_weather (
            gamePk              INT             NOT NULL PRIMARY KEY,
            officialDate        DATE            NOT NULL,
            venue_id            INT             NOT NULL,
            venue_name          VARCHAR(150)    NOT NULL,
            roof_type           VARCHAR(20)     NULL,
            is_dome             BIT             NULL,
            game_hour_utc       TINYINT         NULL,
            temp_c              DECIMAL(6,2)    NULL,
            dewpoint_c          DECIMAL(6,2)    NULL,
            relative_humidity   DECIMAL(6,2)    NULL,
            wind_speed_kph      DECIMAL(6,2)    NULL,
            wind_direction_deg  DECIMAL(6,2)    NULL,
            precip_mm           DECIMAL(7,2)    NULL,
            pressure_hpa        DECIMAL(7,2)    NULL,
            weather_missing     BIT             NOT NULL DEFAULT 0,
            weather_source      VARCHAR(50)     NULL,
            extract_ts          DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
        )
    END
    """
    execute_sql(sql)


def merge_fact_game_weather():
    """
    Merge staging table into final fact table.
    """
    merge_sql = """
    MERGE mlb.dbo.fact_game_weather AS tgt
    USING (
        SELECT
            gamePk,
            officialDate,
            venue_id,
            venue_name,
            roof_type,
            is_dome,
            game_hour_utc,
            temp_c,
            dewpoint_c,
            relative_humidity,
            wind_speed_kph,
            wind_direction_deg,
            precip_mm,
            pressure_hpa,
            weather_missing,
            weather_source
        FROM mlb.dbo.fact_game_weather_stg
    ) AS src
    ON tgt.gamePk = src.gamePk
    WHEN MATCHED THEN
        UPDATE SET
            tgt.officialDate       = src.officialDate,
            tgt.venue_id           = src.venue_id,
            tgt.venue_name         = src.venue_name,
            tgt.roof_type          = src.roof_type,
            tgt.is_dome            = src.is_dome,
            tgt.game_hour_utc      = src.game_hour_utc,
            tgt.temp_c             = src.temp_c,
            tgt.dewpoint_c         = src.dewpoint_c,
            tgt.relative_humidity  = src.relative_humidity,
            tgt.wind_speed_kph     = src.wind_speed_kph,
            tgt.wind_direction_deg = src.wind_direction_deg,
            tgt.precip_mm          = src.precip_mm,
            tgt.pressure_hpa       = src.pressure_hpa,
            tgt.weather_missing    = src.weather_missing,
            tgt.weather_source     = src.weather_source,
            tgt.extract_ts         = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
        INSERT (
            gamePk,
            officialDate,
            venue_id,
            venue_name,
            roof_type,
            is_dome,
            game_hour_utc,
            temp_c,
            dewpoint_c,
            relative_humidity,
            wind_speed_kph,
            wind_direction_deg,
            precip_mm,
            pressure_hpa,
            weather_missing,
            weather_source,
            extract_ts
        )
        VALUES (
            src.gamePk,
            src.officialDate,
            src.venue_id,
            src.venue_name,
            src.roof_type,
            src.is_dome,
            src.game_hour_utc,
            src.temp_c,
            src.dewpoint_c,
            src.relative_humidity,
            src.wind_speed_kph,
            src.wind_direction_deg,
            src.precip_mm,
            src.pressure_hpa,
            src.weather_missing,
            src.weather_source,
            SYSUTCDATETIME()
        );
    """
    execute_sql(merge_sql)


def load_fact_game_weather(df: pd.DataFrame):
    """
    Load dataframe into staging then merge into target.
    """
    if df.empty:
        logger.warning("No rows to load into fact_game_weather")
        return

    # Replace staging each run
    load_dataframe(df, "fact_game_weather_stg", if_exists="replace")
    merge_fact_game_weather()
    logger.info("fact_game_weather load complete")


if __name__ == "__main__":
    create_fact_game_weather_table()
    df_weather = build_game_weather_dataframe()
    print(df_weather.head())
    load_fact_game_weather(df_weather)