DATABASE_PATH = "G:/walle-databases/walle-prod.duckdb"
DEMO_DIRECTORY_PATH = "D:/walle-database/7902/"
MAX_WORKERS = 4

PLAYER_PROPS = [
    "X",
    "Y",
    "Z",
    "name",
    "team_clan_name",
    "current_equip_value"
]

COLS_TO_DROP = {
    "kills": [
        "assister_X", "assister_Y", "assister_Z", "assister_health",
        "assister_place", "assister_team_clan_name",
        "assister_current_equip_value",
        'assister_steamid',
        "attacker_current_equip_value", "ct_side", "noreplay",
        "victim_current_equip_value", "weapon_fauxitemid", "weapon_itemid",
        "weapon_originalowner_xuid", "wipe", "attacker_steamid", "victim_steamid",
        "dominated", "revenge", "t_team_clan_name", "t_side", "ct_team_clan_name",
        "victim_side", "attacker_side", "assister_side"
    ],
    "damages": [
        "armor", "attacker_current_equip_value", "ct_side",
        "t_team_clan_name", "ct_team_clan_name", "t_side",
        "victim_current_equip_value", "attacker_steamid", "victim_steamid"
    ],
    "infernos": [
        "thrower_current_equip_value", "thrower_health", "entity_id", "thrower_steamid"
    ],
    "smokes": [
        "thrower_current_equip_value", "thrower_health", "thrower_steamid"
    ],
    "bomb_events": [
        "steamid"
    ]
}