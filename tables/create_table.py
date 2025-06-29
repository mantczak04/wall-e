import polars as pl
from demoparser2 import DemoParser
from pathlib import Path
from datetime import datetime
from awpy import Demo
import re
import utils

def create_entry_kill_table(rounds_df: pl.DataFrame, kills_df: pl.DataFrame) -> pl.DataFrame:
    """
    Add the name of the team, player and victim that were involved in entry-kill of the round.
    """
    sorted_kills_df = kills_df.sort("tick")

    entry_kills_df = sorted_kills_df.group_by('round_num').first()
    
    entry_kills_df = entry_kills_df.select(
        'round_num', 'attacker_team_clan_name', 'attacker_side', 
        'attacker_name', 'attacker_place', 'attacker_X', 'attacker_Y', 'attacker_Z', 'weapon', 'victim_name', 
        'victim_place', 'victim_X', 'victim_Y', 'victim_Z', 'tick'
    )
    
    joined_df = entry_kills_df.join(
        rounds_df.select(['round_num', 'freeze_end']),
        on='round_num',
        how='left'
    )

    joined_df = joined_df.with_columns(
        (pl.col('tick') - pl.col('freeze_end')).alias('ticks_after_freeze')
    )

    return joined_df

def create_shots_table(parser: DemoParser) -> pl.DataFrame:
    """
    Parses a demo to get shots fired, damage dealt, and player state at the time of the shot.

    Args:
        parser: An initialized DemoParser object.

    Returns:
        A Polars DataFrame with enriched shot data.
    """
    # 1. Strzały + numer rundy
    shots_df = pl.from_pandas(parser.parse_event(
        "weapon_fire", other=["total_rounds_played"]
    ))

    # 2. Obrażenia z nazwami drużyn
    damage_df = pl.from_pandas(parser.parse_event(
        "player_hurt", player=["team_name"]
    ))

    # 3. Tylko obrażenia w przeciwników
    opponent_damage = (
        damage_df
        .filter(pl.col("attacker_team_name") != pl.col("user_team_name"))
        .group_by("tick", "attacker_name")
        .agg(pl.sum("dmg_health").alias("damage_dealt"))
    )

    # 4. Tylko ticki, gdzie padły strzały
    shot_ticks = shots_df["tick"].unique().to_list()

    # 5. Stan gracza w tych tickach
    player_state_df = pl.from_pandas(parser.parse_ticks(
        wanted_props=["name", "accuracy_penalty", "flash_duration", "is_airborne"],
        ticks=shot_ticks
    ))

    # 6. Join: strzały + stan
    shots_with_state = shots_df.join(
        player_state_df,
        left_on=["tick", "user_name"],
        right_on=["tick", "name"],
        how="left"
    )

    # 7. Join: + obrażenia
    shots_with_damage = shots_with_state.join(
        opponent_damage,
        left_on=["tick", "user_name"],
        right_on=["tick", "attacker_name"],
        how="left"
    )

    # 8. Finalna tabela
    return shots_with_damage.select(
        (pl.col("total_rounds_played")+1).alias("round_num"),
        pl.col("tick"),
        pl.col("user_name"),
        pl.col("accuracy_penalty"),
        pl.col("flash_duration"),
        pl.col("is_airborne").alias("inair"),
        pl.col("weapon"),
        pl.col("damage_dealt").fill_null(0)
    )

def create_game_state_table(kills_df: pl.DataFrame, rounds_df: pl.DataFrame) -> pl.DataFrame:
    """
    Table that represents game state (XvX players alive) at a time (round, tick).
    """
    start_state = rounds_df.select([
        pl.col("round_num"),
        pl.col("start").alias("tick")
    ]).with_columns([
        pl.col("round_num").cast(pl.Int64),
        pl.col("tick").cast(pl.Int64),
        pl.lit(5).alias("ct_alive"),
        pl.lit(5).alias("t_alive")
    ])

    post_kill_state = kills_df.sort("tick").with_columns([
        pl.when(pl.col("victim_side") == "ct").then(1).otherwise(0)
          .cum_sum().over("round_num").alias("ct_deaths"),
        pl.when(pl.col("victim_side") == "t").then(1).otherwise(0)
          .cum_sum().over("round_num").alias("t_deaths"),
        pl.col("round_num").cast(pl.Int64),
        pl.col("tick").cast(pl.Int64),
    ]).select([
        "round_num",
        "tick",
        (5 - pl.col("ct_deaths")).alias("ct_alive"),
        (5 - pl.col("t_deaths")).alias("t_alive"),
    ])

    game_state = pl.concat([start_state, post_kill_state]).sort(["round_num", "tick"])
    return game_state

def create_he_grenades_table(parser: DemoParser, smokes_df: pl.DataFrame) -> pl.DataFrame:
    """
    Tworzy wzbogaconą tabelę granatów HE, używając już sparsowanych danych.
    Dodaje informacje o zadanych obrażeniach i o tym, czy granat zniszczył smoke'a.

    Args:
        parser: Obiekt demoparser2.
        smokes_df: Ramka danych Polars z informacjami o granatach dymnych (np. z dem.smokes).

    Returns:
        Ramka danych Polars z kompleksowymi informacjami o granatach HE.
    """
    # Krok 1: Pobierz dane o detonacjach HE i obrażeniach
    he_detonations_df = pl.from_pandas(
        parser.parse_event("hegrenade_detonate", player=["last_place_name"], other=["total_rounds_played"])
    )
    damage_df = pl.from_pandas(
        parser.parse_event("player_hurt", player=["team_name"])
    )

    # Krok 2: Przetwórz i zagreguj obrażenia od HE
    he_damage_agg = (
        damage_df
        .filter((pl.col("weapon") == "hegrenade") & (pl.col("attacker_team_name") != pl.col("user_team_name")))
        .group_by("tick", "attacker_name")
        .agg(pl.sum("dmg_health").alias("damage_dealt"))
    )

    # Krok 3: Połącz detonacje z obrażeniami
    he_with_damage = he_detonations_df.join(
        he_damage_agg,
        left_on=["tick", "user_name"], right_on=["tick", "attacker_name"], how="left"
    )

    # Krok 4: Sprawdź, które granaty HE zniszczyły smoke'a
    active_smokes = smokes_df.filter(pl.col("end_tick").is_not_null()).select(
        pl.col("start_tick"),
        pl.col("end_tick"),
        pl.col("X").alias("X_smoke"), # Zmieniamy nazwę na unikalną
        pl.col("Y").alias("Y_smoke"),
        pl.col("Z").alias("Z_smoke")
    )
    
    # Tworzymy wszystkie możliwe pary (HE, smoke) do porównania
    he_x_smokes = he_with_damage.join(active_smokes, how="cross", suffix="_smoke")

    radius = 120.0
    popped_smokes_info = he_x_smokes.filter(
        # Warunek 1: Czas eksplozji HE mieści się w czasie życia smoke'a
        pl.col("tick").is_between(pl.col("start_tick"), pl.col("end_tick")),
        # Warunek 2: Odległość HE od środka smoke'a jest mniejsza niż promień
        (
            (pl.col("x") - pl.col("X_smoke")).pow(2) +
            (pl.col("y") - pl.col("Y_smoke")).pow(2) +
            (pl.col("z") - pl.col("Z_smoke")).pow(2)
        ) < radius**2
    ).select("tick", "user_name").unique() # Pobieramy unikalne HE, które coś zniszczyły

    # Krok 5: Dołącz informację 'smoke_popped' do głównej tabeli HE
    final_table = he_with_damage.join(
        popped_smokes_info.with_columns(pl.lit(True).alias("smoke_popped")),
        on=["tick", "user_name"],
        how="left"
    )

    # Krok 6: Wybierz, uporządkuj i wyczyść finalne kolumny
    return final_table.select(
        (pl.col("total_rounds_played") + 1).alias("round_num"),
        pl.col("tick"),
        pl.col("user_name").alias("thrower_name"),
        pl.col("damage_dealt").fill_null(0),
        pl.col("smoke_popped").fill_null(False),  # Jeśli nie było dopasowania, to False
        pl.col("x").alias("detonation_x"),
        pl.col("y").alias("detonation_y"),
        pl.col("z").alias("detonation_z")
    )

def create_flashbangs_table(parser: DemoParser) -> pl.DataFrame:
    """
    Tworzy wzbogaconą tabelę granatów błyskowych, używając już sparsowanych danych.
    Dodaje informacje o liczbie oślepionych przeciwników.

    Args:
        parser: Obiekt demoparser2.

    Returns:
        Ramka danych Polars z kompleksowymi informacjami o granatach błyskowych.
    """
    # Krok 1: Pobierz dane o detonacjach flashbangów
    flash_detonations_df = pl.from_pandas(
        parser.parse_event("flashbang_detonate", player=["last_place_name"], other=["total_rounds_played"])
    )

    # Krok 4: Wybierz, uporządkuj i wyczyść finalne kolumny
    return flash_detonations_df.select(
        (pl.col("total_rounds_played") + 1).alias("round_num"),
        pl.col("tick"),
        pl.col("user_name").alias("thrower_name"),
        pl.col("x").alias("detonation_x"),
        pl.col("y").alias("detonation_y"),
        pl.col("z").alias("detonation_z")
    )

def create_matches_table(demo_path: Path, map_name: str, ticks_df: pl.DataFrame) -> pl.DataFrame:
    date = datetime.fromtimestamp(demo_path.stat().st_mtime)
    tournament_id= demo_path.parent.name

    team1, team2 = ticks_df.select('team_clan_name').unique().to_series().to_list()

    match_id = utils.create_match_id(tournament_id, map_name, team1, team2)

    return pl.DataFrame({
        "match_id":      [match_id],
        "tournament_id": [tournament_id],
        "date":     [date],
        "team1":         [team1],
        "team2":         [team2],
        "map_name":      [map_name]
    })