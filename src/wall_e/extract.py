import random
from datetime import datetime
from pathlib import Path

import polars as pl
from awpy import Demo

def create_match_id(tournament_id: str, map_name: str, team1: str, team2: str) -> str:
    pin = "".join(str(random.randint(0, 9)) for _ in range(4))
    return f"{tournament_id}-{map_name}-{team1}-vs-{team2}-{pin}"

def parse_demo(demo_path: Path, player_props: list[str]) -> dict:
    demo = Demo(demo_path, verbose=False)
    demo.parse(player_props=player_props)

    date = datetime.fromtimestamp(demo_path.stat().st_mtime)
    tournament_id = demo_path.parent.name

    # Robustly extract team names
    unique_teams_df = demo.ticks.select("team_clan_name").unique()
    valid_teams_df = unique_teams_df.filter(
        pl.col("team_clan_name").is_not_null() & (pl.col("team_clan_name") != "")
    )
    valid_teams = valid_teams_df.to_series().to_list()

    if len(valid_teams) < 2:
        raise ValueError(f"Could not determine two distinct teams from {demo_path.name}. Found: {valid_teams}")
    
    team1, team2 = valid_teams[0], valid_teams[1]
    
    match_id = create_match_id(tournament_id, demo.header["map_name"], team1, team2)

    # Parse all necessary events once
    weapon_fire_df = pl.from_pandas(demo.parser.parse_event("weapon_fire", other=["total_rounds_played"]))
    player_hurt_df = pl.from_pandas(demo.parser.parse_event("player_hurt", player=["team_name"]))
    he_detonations_df = pl.from_pandas(demo.parser.parse_event("hegrenade_detonate", player=["last_place_name"], other=["total_rounds_played"]))
    flash_detonations_df = pl.from_pandas(demo.parser.parse_event("flashbang_detonate", player=["last_place_name"], other=["total_rounds_played"]))

    shot_ticks = weapon_fire_df["tick"].unique().to_list()
    player_state_at_shot_df = pl.from_pandas(demo.parser.parse_ticks(
        wanted_props=["name", "accuracy_penalty", "flash_duration", "is_airborne"],
        ticks=shot_ticks
    ))

    return {
        "match_id": match_id,
        "rounds": demo.rounds,
        "ticks": demo.ticks,
        "damages": demo.damages,
        "kills": demo.kills,
        "bomb_events": demo.bomb,
        "infernos": demo.infernos,
        "smokes": demo.smokes,
        "bomb_planted_events": demo.events.get("bomb_planted", pl.DataFrame()),
        "weapon_fire_events": weapon_fire_df,
        "player_hurt_events": player_hurt_df,
        "he_detonation_events": he_detonations_df,
        "flash_detonation_events": flash_detonations_df,
        "player_state_at_shot": player_state_at_shot_df,
        "matches": pl.DataFrame({
            "match_id": [match_id],
            "tournament_id": [tournament_id],
            "date": [date],
            "team1": [team1],
            "team2": [team2],
            "map_name": [demo.header["map_name"]],
        }),
    }