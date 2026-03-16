from sqlalchemy import create_engine
import logging
import os

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