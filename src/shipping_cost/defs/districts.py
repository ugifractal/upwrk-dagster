import dagster as dg
import duckdb
from dagster_duckdb import DuckDBResource
from pathlib import Path
from .regencies import regencies

@dg.asset(
    group_name="shipping",
    compute_kind="duckdb",
    deps=["regencies"]
)
def districts(context: dg.AssetExecutionContext, duckdb_resource: DuckDBResource) -> dg.MaterializeResult:

    root_dir = Path(__file__).parent.parent.parent.parent
    file_path = Path("/Users/sugiarto/my_apps/sugi/emperid/address_line/districts.jsonl")

    with duckdb_resource.get_connection() as conn:

        conn.execute(f"""
            CREATE OR REPLACE TABLE districts AS
            SELECT * FROM read_json_auto('{file_path.as_posix()}')
        """)

        preview_query = """
            SELECT
                d.id as district_id,
                d.name as district_name,
                r.name as regency_name
            FROM districts d
            LEFT JOIN regencies r ON d.regency_id = r.id
            LIMIT 10
        """
        preview_df = conn.execute(preview_query).df()
        row_count = conn.execute("SELECT count(*) FROM districts").fetchone()[0]

    records = [dg.TableRecord(r) for r in preview_df.to_dict(orient="records")]

    return dg.MaterializeResult(
        metadata={
            "table_name": "districts",
            "row_count": row_count,
            "preview_list": dg.MetadataValue.table(records=records)
        }
    )