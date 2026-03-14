import dagster as dg
import duckdb
from dagster_duckdb import DuckDBResource
from pathlib import Path
from .provinces import provinces

@dg.asset(
    group_name="ingestion",
    compute_kind="duckdb",
    deps=[provinces]
)
def regencies(
    context: dg.AssetExecutionContext,
    duckdb_resource: DuckDBResource
) -> dg.MaterializeResult:

    root_dir = Path(__file__).parent.parent.parent.parent

    file_path = Path("/Users/sugiarto/my_apps/sugi/emperid/address_line/regencies.jsonl")

    with duckdb_resource.get_connection() as conn:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS regencies AS
            SELECT * FROM read_json_auto('{file_path.as_posix()}')
        """)

        preview_query = """
            SELECT
                r.id as regency_id,
                r.name as regency_name,
                p.name as province_name
            FROM regencies r
            LEFT JOIN provinces p ON r.province_id = p.id
            LIMIT 10
        """
        preview_df = conn.execute(preview_query).df()
        row_count = conn.execute("SELECT count(*) FROM regencies").fetchone()[0]

    records = [dg.TableRecord(r) for r in preview_df.to_dict(orient="records")]

    return dg.MaterializeResult(
        metadata={
            "table_name": "regencies",
            "row_count": row_count,
            "top_provinces_sample": dg.MetadataValue.table(records=records)
        }
    )