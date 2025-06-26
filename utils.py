import random
import re
import polars as pl
import duckdb
from pathlib import Path

def create_match_id(tournament_id: str, map_name: str, team1: str, team2: str) -> str:
    pin = ''.join(str(random.randint(0, 9)) for _ in range(4))
    return f"{tournament_id}-{map_name}-{team1}-vs-{team2}-{pin}"

def extract_teams(filename: str) -> tuple[str, str]:
    # Usuń _numer na końcu
    base = re.sub(r'_\d+$', '', filename)

    # Podziel na team1 i resztę po '-vs-'
    team1_part, rest = base.split('-vs-', 1)

    # Podziel rest na fragmenty po '-'
    parts = rest.split('-')

    # Sprawdź czy jest fragment typu mX (np. m1, m2, ..., m7)
    m_index = None
    for i, part in enumerate(parts):
        if re.match(r'^m[1-7]$', part):
            m_index = i
            break

    if m_index is not None:
        # team2 to wszystko przed mX
        team2_part = '-'.join(parts[:m_index])
    else:
        # nie ma mX, usuwamy ostatni fragment (mapa)
        team2_part = '-'.join(parts[:-1])

    return team1_part, team2_part

def add_match_id(df: pl.DataFrame, match_id: str) -> pl.DataFrame:
    return df.with_columns(pl.lit(match_id).alias("match_id"))


def save_to_duckdb(dfs: dict[str, pl.DataFrame], db_path: str):
    con = duckdb.connect(db_path)
    for name, df in dfs.items():
        con.register("tmp_df", df)
        con.execute(f"CREATE TABLE IF NOT EXISTS {name} AS SELECT * FROM tmp_df LIMIT 0")
        con.execute(f"INSERT INTO {name} SELECT * FROM tmp_df")
        con.unregister("tmp_df")
    con.close()

def process_match(demo_path: Path) -> dict[str, pl.DataFrame]:
    from awpy import Demo
    from demoparser2 import DemoParser
    import features
    import features.table_processing
    import tables.create_table

    # Parsowanie demo
    dem = Demo(demo_path, verbose=False)
    parser_dem = DemoParser(str(demo_path))

    request_player_props = [
        'current_equip_value',
        'team_name',
        'team_clan_name'
    ]

    dem.parse(player_props=request_player_props)

    rounds_df = dem.rounds
    ticks_df = dem.ticks
    damages_df = dem.damages
    kills_df = dem.kills
    bomb_events_df = dem.bomb
    infernos_df = dem.infernos
    smokes_df = dem.smokes

    # Cast equip_value
    ticks_df = ticks_df.with_columns(pl.col('current_equip_value').cast(pl.Int16))

    # Round features
    rounds_df = features.table_processing.add_round_winner(rounds_df, ticks_df)
    rounds_df = features.table_processing.add_round_equipment_value(rounds_df, ticks_df)
    rounds_df = features.table_processing.fix_bomb_sites(rounds_df, dem.events.get('bomb_planted', pl.DataFrame()))

    # Create match_id
    matches_df = tables.create_table.create_matches_table(demo_path, dem.header['map_name'])
    match_id = matches_df.select('match_id').item()  # wyciąga pojedynczy string match_id

    # Generate tables
    game_state_df = tables.create_table.create_game_state_table(kills_df, rounds_df)
    entry_kills_df = tables.create_table.create_entry_kill_table(rounds_df, kills_df)
    shots_df = tables.create_table.create_shots_table(parser_dem)
    he_grenades_df = tables.create_table.create_he_grenades_table(parser_dem, smokes_df)
    flashbangs_df = tables.create_table.create_flashbangs_table(parser_dem)

    # deleting unnecessary columns
    kills_df = features.table_processing.drop_columns(kills_df, [
        'assister_X', 'assister_Y', 'assister_Z', 'assister_health',
        'assister_place', 'assister_team_clan_name', 'assister_current_equip_value',
        'attacker_current_equip_value', 'ct_side', 'noreplay',
        'victim_current_equip_value', 'weapon_fauxitemid', 'weapon_itemid',
        'weapon_originalowner_xuid', 'wipe', 'attacker_steamid', 'victim_steamid'
    ])
    damages_df = features.table_processing.drop_columns(damages_df, [
        'armor', 'attacker_current_equip_value', 'ct_side',
        't_team_clan_name', 'ct_team_clan_name', 't_side',
        'victim_current_equip_value', 'attacker_steamid', 'victim_steamid'
    ])
    infernos_df = features.table_processing.drop_columns(infernos_df, [
        'thrower_current_equip_value', 'thrower_health', 'entity_id', 'thrower_steamid'
    ])
    smokes_df = features.table_processing.drop_columns(smokes_df, [
        'thrower_current_equip_value', 'thrower_health', 'thrower_steamid'
    ])

    # Dodaj match_id do każdej tabeli
    def add_match_id(df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(pl.lit(match_id).alias("match_id"))

    return {
        "matches": matches_df,
        "rounds": add_match_id(rounds_df),
        #"ticks": add_match_id(ticks_df), for now without ticks
        "damages": add_match_id(damages_df),
        "kills": add_match_id(kills_df),
        "game_state": add_match_id(game_state_df),
        "entry_kills": add_match_id(entry_kills_df),
        "shots": add_match_id(shots_df),
        "he_grenades": add_match_id(he_grenades_df),
        "flashbangs": add_match_id(flashbangs_df),
        "infernos": add_match_id(infernos_df),
        "smokes": add_match_id(smokes_df),
    }
