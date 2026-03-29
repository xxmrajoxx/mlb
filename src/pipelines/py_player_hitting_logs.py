import pandas as pd
import logging
import time
from datetime import datetime, UTC

from pybaseball import statcast_batter
from src.ingestion.mlb_player_id_team import fetch_single_team
from sql.sql_loader import load_dataframe, truncate_table, execute_sql

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

#https://github.com/jldbc/pybaseball/blob/master/docs/statcast_batter.md

def player_hit_stats(start_dt: str, end_dt: str, team_id: int) -> pd.DataFrame:
    df_roster = fetch_single_team(team_id)

    if df_roster.empty:
        logger.warning(f"No player found for team_id={team_id}")
        return pd.DataFrame()

    players = []

    for row in df_roster.itertuples(index=False):
        player_id = row.player_id
        player_name = row.player_name

        logger.info(f"Fetching statcast data for {player_name} ({player_id})")

        try:
            df = statcast_batter(
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

            meta_df = pd.DataFrame({
                "player_id": [player_id] * len(df),
                "team_id": [team_id] * len(df),
                "extract_date": [now_date] * len(df),
                "extract_ts": [now_dt] * len(df),
            })

            df = pd.concat([df.reset_index(drop=True), meta_df], axis=1)

            players.append(df)

        except Exception as e:
            logger.error(f"Failed for {player_name} ({player_id}): {e}")
            continue

        time.sleep(1)

    if not players:
        logger.warning("No batting data collected")
        return pd.DataFrame()

    final_df = pd.concat(players, ignore_index=True).copy()
    return final_df


def load_player_hit_statcast(df: pd.DataFrame):
    if df.empty:
        logger.warning("DataFrame is empty. Nothing to load.")
        return

    stage_table = "stg_player_hit_statcast"

    logger.info("Truncating staging table")
    truncate_table(stage_table)

    logger.info("Loading staging table")
    load_dataframe(df, stage_table, if_exists="replace")

    merge_sql = """
    INSERT INTO mlb.dbo.fact_player_hit_statcast (
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
        bat_speed,
        swing_length,
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
        s.bat_speed,
        s.swing_length,
        s.player_id,
        s.team_id,
        s.extract_date,
        s.extract_ts
    FROM mlb.dbo.stg_player_hit_statcast s
    WHERE NOT EXISTS (
        SELECT 1
        FROM dbo.fact_player_hit_statcast f
        WHERE f.game_pk = s.game_pk
          AND f.at_bat_number = s.at_bat_number
          AND f.pitch_number = s.pitch_number
          AND f.batter = s.batter
          AND f.pitcher = s.pitcher
    );
    """

    logger.info("Merging staging into fact table")
    execute_sql(merge_sql)

    logger.info("Truncating staging table after merge")
    truncate_table(stage_table)


if __name__ == "__main__":
    df = player_hit_stats(
        start_dt="2026-03-26",
        end_dt="2026-03-27",
        team_id=116
    )

    print(df.head())
    print(df.shape)

    load_player_hit_statcast(df)
