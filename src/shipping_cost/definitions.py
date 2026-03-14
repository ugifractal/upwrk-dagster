import dagster as dg
from pathlib import Path
from dagster_duckdb import DuckDBResource

defs = dg.Definitions.merge(
    dg.load_from_defs_folder(path_within_project=Path(__file__).parent),
    dg.Definitions(
        resources={
            "duckdb_resource": DuckDBResource(database="data/local_database.duckdb")
        }
    )
)