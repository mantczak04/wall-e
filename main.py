import concurrent.futures
from pathlib import Path

import duckdb

import config
from src.wall_e import load, pipeline


def main():
    db_path = config.DATABASE_PATH
    demo_dir = Path(config.DEMO_DIRECTORY_PATH)
    demo_files = list(demo_dir.glob("*.dem"))
    files_quantity = len(demo_files)

    print(f"Found {files_quantity} demo files to process.")

    con = duckdb.connect(db_path)
    try:
        with concurrent.futures.ProcessPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
            # Submit all demo processing tasks to the pool
            future_to_demo = {
                executor.submit(pipeline.process_demo, demo_path): demo_path
                for demo_path in demo_files
            }

            for i, future in enumerate(concurrent.futures.as_completed(future_to_demo)):
                demo_path = future_to_demo[future]
                try:
                    print(f"[{i+1}/{files_quantity}] Processing {demo_path.name}...")
                    processed_dfs = future.result()

                    print(f"[{i+1}/{files_quantity}] Saving data for {demo_path.name} to DuckDB...")
                    load.save_to_duckdb(processed_dfs, con)
                    print(f"[{i+1}/{files_quantity}] Successfully saved {demo_path.name}.")

                except Exception as e:
                    print(f"Error processing {demo_path.name}: {e}. Skipping this file.")
    finally:
        con.close()
        print("\nDatabase connection closed. Pipeline finished.")


if __name__ == "__main__":
    main()