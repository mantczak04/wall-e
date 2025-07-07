from pathlib import Path

import polars as pl

import config
from src.wall_e import extract, transform


def process_demo(demo_path: Path) -> dict[str, pl.DataFrame]:
    """
    Orchestrates the full ETL pipeline for a single demo file.

    1. Extracts raw data using the `extract` module.
    2. Transforms and enriches the data using the `transform` module.
    3. Returns a dictionary of cleaned DataFrames ready for loading.

    Args:
        demo_path: Path to the demo file to be processed.

    Returns:
        A dictionary where keys are table names and values are the final
        Polars DataFrames.
    """
    # 1. Extract Phase
    raw_data = extract.parse_demo(demo_path, config.PLAYER_PROPS)

    # Unpack the raw dataframes for processing
    match_id = raw_data["match_id"]
    matches_df = raw_data["matches"]
    rounds_df = raw_data["rounds"]
    ticks_df = raw_data["ticks"]
    damages_df = raw_data["damages"]
    kills_df = raw_data["kills"]
    bomb_events_df = raw_data["bomb_events"]
    infernos_df = raw_data["infernos"]
    smokes_df = raw_data["smokes"]
    bomb_planted_events = raw_data["bomb_planted_events"]

    # 2. Transform Phase
    # Cast equip_value
    ticks_df = ticks_df.with_columns(pl.col("current_equip_value").cast(pl.Int16))

    # Enrich rounds data
    rounds_df = transform.add_round_winner(rounds_df, ticks_df)
    rounds_df = transform.add_round_equipment_value(rounds_df, ticks_df)
    rounds_df = transform.fix_bomb_sites(rounds_df, bomb_planted_events)

    # Create new derived tables
    game_state_df = transform.create_game_state_table(kills_df, rounds_df)
    entry_kills_df = transform.create_entry_kill_table(rounds_df, kills_df)
    shots_df = transform.create_shots_table(
        raw_data["weapon_fire_events"],
        raw_data["player_hurt_events"],
        raw_data["player_state_at_shot"]
    )
    he_grenades_df = transform.create_he_grenades_table(
        raw_data["he_detonation_events"],
        raw_data["player_hurt_events"],
        smokes_df
    )
    flashbangs_df = transform.create_flashbangs_table(raw_data["flash_detonation_events"])

    # Clean up columns by dropping unnecessary ones
    kills_df = transform.drop_columns(kills_df, config.COLS_TO_DROP["kills"])
    damages_df = transform.drop_columns(damages_df, config.COLS_TO_DROP["damages"])
    infernos_df = transform.drop_columns(infernos_df, config.COLS_TO_DROP["infernos"])
    smokes_df = transform.drop_columns(smokes_df, config.COLS_TO_DROP["smokes"])

    # Add match_id to all tables that need it
    def add_match_id_to_df(df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(pl.lit(match_id).alias("match_id"))

    final_dfs = {
        "matches": matches_df,
        "rounds": add_match_id_to_df(rounds_df),
        "damages": add_match_id_to_df(damages_df),
        "kills": add_match_id_to_df(kills_df),
        "game_state": add_match_id_to_df(game_state_df),
        "bomb_events": add_match_id_to_df(bomb_events_df),
        "entry_kills": add_match_id_to_df(entry_kills_df),
        "shots": add_match_id_to_df(shots_df),
        "he_grenades": add_match_id_to_df(he_grenades_df),
        "flashbangs": add_match_id_to_df(flashbangs_df),
        "infernos": add_match_id_to_df(infernos_df),
        "smokes": add_match_id_to_df(smokes_df),
    }

    return final_dfs