import polars as pl
from demoparser2 import DemoParser

def add_round_winner(rounds_df: pl.DataFrame, ticks_df: pl.DataFrame) -> pl.DataFrame:
    side1, clan_name1 = ticks_df.select(["side", "team_clan_name"]).row(0)
    side2, clan_name2 = ticks_df.select(["side", "team_clan_name"]).row(7)
    rounds_winner_name = rounds_df.with_columns([
        pl.when(pl.col("round_num") <= 12)
        .then(
            pl.when(pl.col("winner") == side1)
            .then(pl.lit(clan_name1))
            .otherwise(pl.lit(clan_name2))
        )
        .otherwise(
            pl.when(pl.col("winner") != side1)
            .then(pl.lit(clan_name1))
            .otherwise(pl.lit(clan_name2))
        )
        .alias("winner_team_name")
    ])
    return rounds_winner_name

def add_round_equipment_value(rounds_df: pl.DataFrame, ticks_df: pl.DataFrame) -> pl.DataFrame:
    freeze_end_ticks_list = rounds_df['freeze_end'].to_list()
    eq_value_df = ticks_df.filter(pl.col('tick').is_in(freeze_end_ticks_list)).select(
        ['round_num', 'tick', 'side', 'name', 'current_equip_value']
    )
    eq_value_df = eq_value_df.group_by(['round_num', 'side']).agg(
        pl.col('current_equip_value').sum().alias('sum_equip_value')
    )
    eq_value_df = eq_value_df.pivot(
        values='sum_equip_value',
        index='round_num',
        on='side'
    ).rename({
        't': 't_equip_value',
        'ct': 'ct_equip_value'
    })
    eq_value_df = eq_value_df.sort(by='round_num')
    result_df = rounds_df.join(eq_value_df, on='round_num', how='left')
    return result_df

def fix_bomb_sites(rounds_df: pl.DataFrame, bomb_plant_events: pl.DataFrame) -> pl.DataFrame:
    plant_ticks = rounds_df.select(["round_num", "bomb_plant"]).drop_nulls()
    plant_positions = bomb_plant_events.join(
        plant_ticks,
        left_on="tick",
        right_on="bomb_plant",
        how="inner"
    )
    t_players = plant_positions.filter(pl.col("user_side") == "t")
    site_by_round = (
        t_players
        .group_by("round_num", "user_place")
        .len()
        .sort("len", descending=True)
        .group_by("round_num")
        .agg(pl.first("user_place").alias("true_bomb_site"))
    )
    return (
        rounds_df
        .join(site_by_round, on="round_num", how="left")
        .drop("bomb_site")
        .rename({"true_bomb_site": "bomb_site"})
    )

def drop_columns(df: pl.DataFrame, cols_to_drop: list[str]) -> pl.DataFrame:
    return df.drop(cols_to_drop)

def create_entry_kill_table(rounds_df: pl.DataFrame, kills_df: pl.DataFrame) -> pl.DataFrame:
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

def create_shots_table(
    shots_df: pl.DataFrame,
    damage_df: pl.DataFrame,
    player_state_df: pl.DataFrame
) -> pl.DataFrame:
    opponent_damage = (
        damage_df
        .filter(pl.col("attacker_team_name") != pl.col("user_team_name"))
        .group_by("tick", "attacker_name")
        .agg(pl.sum("dmg_health").alias("damage_dealt"))
    )
    shots_with_state = shots_df.join(
        player_state_df,
        left_on=["tick", "user_name"],
        right_on=["tick", "name"],
        how="left"
    )
    shots_with_damage = shots_with_state.join(
        opponent_damage,
        left_on=["tick", "user_name"],
        right_on=["tick", "attacker_name"],
        how="left"
    )
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

def create_he_grenades_table(
    he_detonations_df: pl.DataFrame,
    damage_df: pl.DataFrame,
    smokes_df: pl.DataFrame
) -> pl.DataFrame:
    he_damage_agg = (
        damage_df
        .filter((pl.col("weapon") == "hegrenade") & (pl.col("attacker_team_name") != pl.col("user_team_name")))
        .group_by("tick", "attacker_name")
        .agg(pl.sum("dmg_health").alias("damage_dealt"))
    )
    he_with_damage = he_detonations_df.join(
        he_damage_agg,
        left_on=["tick", "user_name"], right_on=["tick", "attacker_name"], how="left"
    )
    active_smokes = smokes_df.filter(pl.col("end_tick").is_not_null()).select(
        pl.col("start_tick"),
        pl.col("end_tick"),
        pl.col("X").alias("X_smoke"),
        pl.col("Y").alias("Y_smoke"),
        pl.col("Z").alias("Z_smoke")
    )
    he_x_smokes = he_with_damage.join(active_smokes, how="cross", suffix="_smoke")
    radius = 120.0
    popped_smokes_info = he_x_smokes.filter(
        pl.col("tick").is_between(pl.col("start_tick"), pl.col("end_tick")),
        (
            (pl.col("x") - pl.col("X_smoke")).pow(2) +
            (pl.col("y") - pl.col("Y_smoke")).pow(2) +
            (pl.col("z") - pl.col("Z_smoke")).pow(2)
        ) < radius**2
    ).select("tick", "user_name").unique()
    final_table = he_with_damage.join(
        popped_smokes_info.with_columns(pl.lit(True).alias("smoke_popped")),
        on=["tick", "user_name"],
        how="left"
    )
    return final_table.select(
        (pl.col("total_rounds_played") + 1).alias("round_num"),
        pl.col("tick"),
        pl.col("user_name").alias("thrower_name"),
        pl.col("damage_dealt").fill_null(0),
        pl.col("smoke_popped").fill_null(False),
        pl.col("x").alias("detonation_x"),
        pl.col("y").alias("detonation_y"),
        pl.col("z").alias("detonation_z")
    )

def create_flashbangs_table(flash_detonations_df: pl.DataFrame) -> pl.DataFrame:
    return flash_detonations_df.select(
        (pl.col("total_rounds_played") + 1).alias("round_num"),
        pl.col("tick"),
        pl.col("user_name").alias("thrower_name"),
        pl.col("x").alias("detonation_x"),
        pl.col("y").alias("detonation_y"),
        pl.col("z").alias("detonation_z")
    )