import duckdb
import polars as pl

def save_to_duckdb(dfs: dict[str, pl.DataFrame], con: duckdb.DuckDBPyConnection) -> None:
    for name, df in dfs.items():
        # Use a more unique name for the temporary view to avoid potential conflicts
        temp_view_name = f"temp_{name}"
        con.register(temp_view_name, df)
        
        # Create the table if it doesn't exist based on the DataFrame's schema
        con.execute(f"CREATE TABLE IF NOT EXISTS {name} AS SELECT * FROM {temp_view_name} LIMIT 0")
        
        # Insert the data from the temporary view into the main table
        con.execute(f"INSERT INTO {name} SELECT * FROM {temp_view_name}")
        
        # Clean up the temporary view
        con.unregister(temp_view_name)