import dagster as dg
from pathlib import Path
from dagster_duckdb import DuckDBResource

fetch_api_job = dg.define_asset_job(
    name="fetch_api_costs_job",
    selection=dg.AssetSelection.groups("shipping")
)

defs = dg.Definitions.merge(
    dg.load_from_defs_folder(path_within_project=Path(__file__).parent),
    dg.Definitions(
        resources={
            "duckdb_resource": DuckDBResource(database="data/local_database.duckdb")
        },
        jobs=[fetch_api_job]
    )
)