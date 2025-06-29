from awpy import Demo
import polars as pl
from pathlib import Path
from features import table_processing
from demoparser2 import DemoParser
import utils

def main():
    db_path = 'walle-database.duckdb'
    demo_dir = Path("D:/walle-demos/7902/")
    demo_files = list(demo_dir.glob("*.dem"))
    files_quantity = len(demo_files)

    for id, demo_path in enumerate(demo_files):
        try:
            print(f'[{id+1}/{files_quantity}] parsing {demo_path}...')
            dfs = utils.process_match(demo_path)
            utils.save_to_duckdb(dfs, db_path)
        except Exception as e:
            print(f"Error processing {demo_path.name}: {e}")

if __name__ == '__main__':
    main()