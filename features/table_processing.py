import polars as pl

def add_round_winner(rounds_df: pl.DataFrame, ticks_df: pl.DataFrame) -> pl.DataFrame:
    """
    Add name of the team that won the round.
    """
    side1, clan_name1 = ticks_df.select(["side", "team_clan_name"]).row(0)
    side2, clan_name2 = ticks_df.select(["side", "team_clan_name"]).row(7) #WARNING: it can lead to error if there are more than 6 TTs

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
    """
    Add sum of T/CT equipment value for every round start.
    """
    freeze_end_ticks_list = rounds_df['freeze_end'].to_list()
    eq_value_df = ticks_df.filter(pl.col('tick').is_in(freeze_end_ticks_list))[['round_num', 'tick', 'side', 'name', 'current_equip_value']]
    
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
    
    eq_value_df.sort(by='round_num')

    result_df = rounds_df.join(eq_value_df, on='round_num', how='left')
    return result_df

def fix_bomb_sites(rounds_df: pl.DataFrame, bomb_plant_events: pl.DataFrame) -> pl.DataFrame:
    # 1. round_num ↔ tick plantu
    plant_ticks = rounds_df.select(["round_num", "bomb_plant"]).drop_nulls()

    # 2. bomb_plant_events z tickami plantu
    plant_positions = bomb_plant_events.join(
        plant_ticks,
        left_on="tick",
        right_on="bomb_plant",
        how="inner"
    )

    # 3. tylko gracze z drużyny T (atakujący)
    t_players = plant_positions.filter(pl.col("user_side") == "t")

    # 4. dominujące user_place (czyli bombsite) w danej rundzie
    site_by_round = (
        t_players
        .group_by("round_num", "user_place")
        .len()
        .sort("len", descending=True)
        .group_by("round_num")
        .agg(pl.first("user_place").alias("true_bomb_site"))
    )

    # 5. podmień bomb_site na poprawione
    return (
        rounds_df
        .join(site_by_round, on="round_num", how="left")
        .drop("bomb_site")
        .rename({"true_bomb_site": "bomb_site"})
    )

def drop_columns(df: pl.DataFrame, cols_to_drop: list[str]) -> pl.DataFrame:
    return df.drop(cols_to_drop)

def drop_steamid_columns(df: pl.DataFrame) -> pl.DataFrame:
    cols_to_drop = [col for col in df.columns if "steamid" in col.lower()]
    return df.drop(cols_to_drop)