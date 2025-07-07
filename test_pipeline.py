import os
from pathlib import Path

import duckdb

import config
from src.wall_e import load, pipeline


def run_test():
    """
    Runs a limited test of the ETL pipeline on a few demo files
    and saves the result to a temporary database.
    """
    TEST_DB_PATH = "test_pipeline.duckdb"
    DEMO_LIMIT = 10

    # Clean up old test database if it exists to ensure a fresh run
    if os.path.exists(TEST_DB_PATH):
        os.remove(TEST_DB_PATH)
        print(f"Removed old test database: {TEST_DB_PATH}")

    # Get demo files from the configured directory
    demo_dir = Path(config.DEMO_DIRECTORY_PATH)
    demo_files = list(demo_dir.glob("*.dem"))

    if not demo_files:
        print(f"No demo files found in {demo_dir}. Cannot run test.")
        return

    # Limit the number of files for the test run
    files_to_process = demo_files[:DEMO_LIMIT]
    files_quantity = len(files_to_process)
    print(f"Found {len(demo_files)} demos. Processing the first {files_quantity} for this test.")
    print(f"Test database will be created at: {TEST_DB_PATH}")

    # Open a single connection for the entire test run
    con = duckdb.connect(TEST_DB_PATH)
    try:
        for i, demo_path in enumerate(files_to_process):
            try:
                print(f"[{i+1}/{files_quantity}] Processing {demo_path.name}...")
                processed_dfs = pipeline.process_demo(demo_path)

                print(f"[{i+1}/{files_quantity}] Saving data for {demo_path.name}...")
                load.save_to_duckdb(processed_dfs, con)
                print(f"[{i+1}/{files_quantity}] Successfully saved {demo_path.name}.")

            except Exception as e:
                print(f"Error processing {demo_path.name}: {e}. Skipping this file.")
    finally:
        # Ensure the connection is closed
        con.close()
        print("\nDatabase connection closed. Test finished.")
        print(f"You can now inspect the database file: '{TEST_DB_PATH}' with DBeaver.")


if __name__ == "__main__":
    run_test()