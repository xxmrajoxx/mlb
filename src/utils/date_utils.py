import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import logging

logger = logging.getLogger(__name__)

def get_start_end_dts(default_offset_days=1, timezone="Australia/Sydney"):
    start_dt = os.getenv("start_dt")
    end_dt = os.getenv("end_dt")

    if start_dt and end_dt:
        logger.info(f"Using start_dt and end_dt from .env: {start_dt}, {end_dt}")
        return start_dt, end_dt

    run_date = datetime.now(ZoneInfo(timezone)).date() - timedelta(days=default_offset_days)
    run_date_str = run_date.strftime("%Y-%m-%d")

    logger.info(f"No dates in .env → auto-using {run_date_str}")
    return run_date_str, run_date_str