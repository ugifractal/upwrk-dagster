import dagster as dg
import duckdb
from dagster_duckdb import DuckDBResource
from pathlib import Path

@dg.asset(
  group_name="ingestion",
  compute_kind="duckdb",
  description="Indonesian Provinces"
)
def provinces(context: dg.AssetExecutionContext, duckdb_resource: DuckDBResource) -> dg.MaterializeResult:
    file_path = Path("/Users/sugiarto/my_apps/sugi/emperid/address_line/provinces.jsonl")
    context.log.info(f"Reading data from {file_path}")

    with duckdb_resource.get_connection() as conn:

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS provinces AS
            SELECT * FROM read_json_auto('{file_path.as_posix()}')
        """)

        row_count = conn.execute("SELECT count(*) FROM provinces").fetchone()[0]

        preview_df = conn.execute("SELECT * FROM provinces LIMIT 5").df()
        records = [
            dg.TableRecord(record) for record in preview_df.to_dict(orient="records")
        ]

    return dg.MaterializeResult(
        metadata={
            "table_name": "provinces",
            "row_count": row_count,
            "preview": dg.MetadataValue.table(records=records)
        }
    )
