import pandas as pd
import time
import logging
from datetime import datetime, date, UTC
import os

from dotenv import load_dotenv
from pybaseball import statcast_pitcher
from src.ingestion.mlb_player_id_all import fetch_active_mlb_players
from sql.sql_loader import load_dataframe, truncate_table, execute_sql

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

load_dotenv(override=True)

logger.info(f"cwd: {os.getcwd()}")
logger.info(f"start_dt from env: {os.getenv('start_dt')}")
logger.info(f"end_dt from env: {os.getenv('end_dt')}")

# https://github.com/jldbc/pybaseball/blob/master/docs/statcast_pitcher.md

def player_pitch_stats(start_dt: str, end_dt: str) -> pd.DataFrame:
    roster_df = fetch_active_mlb_players()

    # obtaining rooster of the team 
    if roster_df.empty:
        logger.warning(f"No player found")
        return pd.DataFrame()

    # then filter by only selecting the pitcher     
    pitcher_df = roster_df[roster_df["position"] == "P"].copy()

    if pitcher_df.empty:
        logger.warning(f"No pitcher found")
        return pd.DataFrame()

    all_pitch_data = []
    
    # then grabing the player_id and calling statcast_pitcher
    for _, row in pitcher_df.iterrows():            #_ - I dont care about this value ((index, row) → ignore index, only use row)
        player_id = row["player_id"]
        player_name = row["player_name"]
        team_id = row["team_id"]

        logger.info(f"fetching statcast data for {player_name}, ({player_id})")

        try:
            df = statcast_pitcher(
                start_dt=start_dt,
                end_dt=end_dt,
                player_id=player_id   
            )
            if df is None or df.empty:
                logger.info(f"No statcast data for {player_name} ({player_id})")
                continue

            df = df.copy()

            now_dt = datetime.now(UTC).replace(tzinfo=None)
            now_date = now_dt.date()

            if "player_id" not in df.columns:
                df["player_id"] = player_id

            if "team_id" not in df.columns:
                df["team_id"] = team_id

            df["extract_date"] = now_date
            df["extract_ts"] = now_dt

            all_pitch_data.append(df)

        except Exception as e:
            logger.error (f"failed for {player_name} ({player_id}): {e}")
            continue

        time.sleep(1)

    if not all_pitch_data:
        logger.warning("No pitching data collected")
        return pd.DataFrame()
    
    final_df = pd.concat(all_pitch_data, ignore_index=True)
    return final_df

def load_player_pitch_statcast(df: pd.DataFrame):
    if df.empty:
        logger.warning("Dataframe is empty. Nothing to load")
        return
    
    staging_table = "stg_player_pitch_statcast"

    logger.info("Truncating staging table")
    truncate_table(staging_table)

    logger.info("Loading staging table")
    load_dataframe(df, staging_table, if_exists="append")

    merge_sql = """
    INSERT INTO mlb.dbo.fact_player_pitch_statcast (
        pitch_type,
        game_date,
        release_speed,
        release_pos_x,
        release_pos_z,
        player_name,
        batter,
        pitcher,
        events,
        description,
        spin_dir,
        spin_rate_deprecated,
        break_angle_deprecated,
        break_length_deprecated,
        zone,
        des,
        game_type,
        stand,
        p_throws,
        home_team,
        away_team,
        type,
        hit_location,
        bb_type,
        balls,
        strikes,
        game_year,
        pfx_x,
        pfx_z,
        plate_x,
        plate_z,
        on_3b,
        on_2b,
        on_1b,
        outs_when_up,
        inning,
        inning_topbot,
        hc_x,
        hc_y,
        tfs_deprecated,
        tfs_zulu_deprecated,
        umpire,
        sv_id,
        vx0,
        vy0,
        vz0,
        ax,
        ay,
        az,
        sz_top,
        sz_bot,
        hit_distance_sc,
        launch_speed,
        launch_angle,
        effective_speed,
        release_spin_rate,
        release_extension,
        game_pk,
        fielder_2,
        fielder_3,
        fielder_4,
        fielder_5,
        fielder_6,
        fielder_7,
        fielder_8,
        fielder_9,
        release_pos_y,
        estimated_ba_using_speedangle,
        estimated_woba_using_speedangle,
        woba_value,
        woba_denom,
        babip_value,
        iso_value,
        launch_speed_angle,
        at_bat_number,
        pitch_number,
        pitch_name,
        home_score,
        away_score,
        bat_score,
        fld_score,
        post_away_score,
        post_home_score,
        post_bat_score,
        post_fld_score,
        if_fielding_alignment,
        of_fielding_alignment,
        spin_axis,
        delta_home_win_exp,
        delta_run_exp,
        api_break_z_with_gravity,
        api_break_x_arm,
        api_break_x_batter_in,
        arm_angle,
        player_id,
        team_id,
        extract_date,
        extract_ts
    )
    SELECT
        s.pitch_type,
        s.game_date,
        s.release_speed,
        s.release_pos_x,
        s.release_pos_z,
        s.player_name,
        s.batter,
        s.pitcher,
        s.events,
        s.description,
        s.spin_dir,
        s.spin_rate_deprecated,
        s.break_angle_deprecated,
        s.break_length_deprecated,
        s.zone,
        s.des,
        s.game_type,
        s.stand,
        s.p_throws,
        s.home_team,
        s.away_team,
        s.type,
        s.hit_location,
        s.bb_type,
        s.balls,
        s.strikes,
        s.game_year,
        s.pfx_x,
        s.pfx_z,
        s.plate_x,
        s.plate_z,
        s.on_3b,
        s.on_2b,
        s.on_1b,
        s.outs_when_up,
        s.inning,
        s.inning_topbot,
        s.hc_x,
        s.hc_y,
        s.tfs_deprecated,
        s.tfs_zulu_deprecated,
        s.umpire,
        s.sv_id,
        s.vx0,
        s.vy0,
        s.vz0,
        s.ax,
        s.ay,
        s.az,
        s.sz_top,
        s.sz_bot,
        s.hit_distance_sc,
        s.launch_speed,
        s.launch_angle,
        s.effective_speed,
        s.release_spin_rate,
        s.release_extension,
        s.game_pk,
        s.fielder_2,
        s.fielder_3,
        s.fielder_4,
        s.fielder_5,
        s.fielder_6,
        s.fielder_7,
        s.fielder_8,
        s.fielder_9,
        s.release_pos_y,
        s.estimated_ba_using_speedangle,
        s.estimated_woba_using_speedangle,
        s.woba_value,
        s.woba_denom,
        s.babip_value,
        s.iso_value,
        s.launch_speed_angle,
        s.at_bat_number,
        s.pitch_number,
        s.pitch_name,
        s.home_score,
        s.away_score,
        s.bat_score,
        s.fld_score,
        s.post_away_score,
        s.post_home_score,
        s.post_bat_score,
        s.post_fld_score,
        s.if_fielding_alignment,
        s.of_fielding_alignment,
        s.spin_axis,
        s.delta_home_win_exp,
        s.delta_run_exp,
        s.api_break_z_with_gravity,
        s.api_break_x_arm,
        s.api_break_x_batter_in,
        s.arm_angle,
        s.player_id,
        s.team_id,
        s.extract_date,
        s.extract_ts
    FROM mlb.dbo.stg_player_pitch_statcast s
    WHERE NOT EXISTS (
        SELECT 1
        FROM dbo.fact_player_pitch_statcast f
        WHERE f.game_pk = s.game_pk
        AND f.at_bat_number = s.at_bat_number
        AND f.pitch_number = s.pitch_number
        AND f.pitcher = s.pitcher
        AND f.batter = s.batter
    );
    """
    logger.info("Merging staging into fact table")
    execute_sql(merge_sql)

    logger.info("Truncating staging table after merge")
    truncate_table(staging_table)


if __name__=="__main__":
    start_dt = os.getenv("start_dt")
    end_dt = os.getenv("end_dt")

    if not start_dt or not end_dt:
        raise ValueError("start_dt and end_dt must be set in the .env file")

    logger.info("dates obtained from .env")

    df = player_pitch_stats(
        start_dt=start_dt,
        end_dt=end_dt
    )
    load_player_pitch_statcast(df)


    # print(df.head())
    # print(df.shape)
    # print(df.columns.tolist())

    # df.to_csv("abc.csv", index=False)


  