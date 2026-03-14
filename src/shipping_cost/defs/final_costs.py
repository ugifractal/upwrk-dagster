import dagster as dg
from dagster_duckdb import DuckDBResource
from pathlib import Path

@dg.asset(
    group_name="shipping",
    compute_kind="duckdb",
    deps=["shipping_costs", "districts", "regencies", "provinces"]
)
def final_costs(context: dg.AssetExecutionContext, duckdb_resource: DuckDBResource) -> dg.MaterializeResult:
    root_dir = Path(__file__).parent.parent.parent.parent
    output_dir = root_dir / "outputs"
    output_path = output_dir / "shipping_rates_final.csv"

    output_dir.mkdir(parents=True, exist_ok=True)

    with duckdb_resource.get_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE TABLE final_shipping_data AS
            SELECT
                s.rate_key,
                p.name as province_name,
                r.name as regency_name,
                d.name as district_name,
                s.cost as shipping_cost,
                s.courier_code,
                s.updated_at
            FROM shipping_costs s
            JOIN districts d ON s.destination_id = d.id
            JOIN regencies r ON d.regency_id = r.id
            JOIN provinces p ON r.province_id = p.id
        """)

        conn.execute(f"""
            COPY final_shipping_data
            TO '{output_path.as_posix()}'
            (HEADER, DELIMITER ',')
        """)

        total_rows = conn.execute("SELECT count(*) FROM final_shipping_data").fetchone()[0]
        preview_df = conn.execute("SELECT * FROM final_shipping_data LIMIT 5").df()

    return dg.MaterializeResult(
        metadata={
            "status": "Exported to CSV",
            "file_path": str(output_path),
            "total_final_records": total_rows,
            "preview": dg.MetadataValue.table(
                records=[dg.TableRecord(r) for r in preview_df.astype(str).to_dict(orient="records")]
            )
        }
    )