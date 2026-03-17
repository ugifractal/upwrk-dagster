import dagster as dg
import duckdb
import subprocess
from dagster_duckdb import DuckDBResource
from pathlib import Path

@dg.asset(
    group_name="shipping",
    compute_kind="ruby",
    description="Menjalankan rake task untuk mengambil data biaya kirim"
)
def fetch_api_costs(context: dg.AssetExecutionContext):
    working_dir = "/Users/sugiarto/my_apps/sugi/emperid"
    command = ["bundle", "exec", "rake", "address:fetch_cost"]

    try:
        # 3. Eksekusi perintah
        result = subprocess.run(
            command,
            cwd=working_dir,  # Berpindah ke folder project Ruby sebelum eksekusi
            check=True,             # Akan memicu error di Dagster jika rake task gagal
            capture_output=True,    # Menangkap stdout/stderr agar bisa dibaca di Dagster
            text=True               # Mengubah output byte menjadi string
        )

        # 4. Kirim output dari Ruby ke log Dagster
        if result.stdout:
            context.log.info(f"Ruby Output: {result.stdout}")

        return dg.MaterializeResult(
            metadata={
                "status": "success",
                "command": " ".join(command)
            }
        )

    except subprocess.CalledProcessError as e:
        # Jika rake task error, tampilkan detail errornya di Dagster
        context.log.error(f"Rake task gagal! Error: {e.stderr}")
        raise e

@dg.asset(
    group_name="shipping",
    compute_kind="duckdb",
    deps=["districts", fetch_api_costs]
)
def shipping_costs(context: dg.AssetExecutionContext, duckdb_resource: DuckDBResource) -> dg.MaterializeResult:
    root_dir = Path(__file__).parent.parent.parent.parent
    file_path = Path("/Users/sugiarto/my_apps/sugi/emperid/address_line/shipping_costs.jsonl")

    with duckdb_resource.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS shipping_costs (
                rate_key VARCHAR PRIMARY KEY,
                origin_id INTEGER,
                destination_id INTEGER,
                rjo_origin_id INTEGER,
                rjo_destination_id INTEGER,
                cost DOUBLE,
                courier_code VARCHAR(10),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.execute(f"""
            INSERT OR REPLACE INTO shipping_costs (
                rate_key,
                origin_id,
                destination_id,
                rjo_origin_id,
                rjo_destination_id,
                cost,
                updated_at
            )
            SELECT
                CAST(origin_id AS VARCHAR) || '-' || CAST(destination_id AS VARCHAR) || '-' || 'jne_reg',
                origin_id,
                destination_id,
                rjo_origin_id,
                rjo_destination_id,
                cost,
                now()
            FROM read_json_auto('{file_path.as_posix()}')
        """)

        latest_query = """
            SELECT
                d.name as district_name,
                s.cost
            FROM shipping_costs s
            JOIN districts d ON s.destination_id = d.id
            ORDER BY s.updated_at DESC
            LIMIT 10
        """
        latest_df = conn.execute(latest_query).df()

        coverage_query = """
            SELECT
                p.name as province,
                count(DISTINCT s.destination_id) as districts_covered
            FROM provinces p
            JOIN regencies r ON r.province_id = p.id
            JOIN districts d ON d.regency_id = r.id
            LEFT JOIN shipping_costs s ON s.destination_id = d.id
            GROUP BY 1
            ORDER BY 2 DESC
        """
        coverage_df = conn.execute(coverage_query).df()

        row_count = conn.execute("SELECT count(*) FROM shipping_costs").fetchone()[0]

    return dg.MaterializeResult(
        metadata={
            "table_name": "shipping_costs",
            "total_collected_rows": row_count,
            "note": "Incremental load: New data from JSONL added/updated to existing DB.",
            "latest_entries": dg.MetadataValue.table(
                records=[dg.TableRecord(r) for r in latest_df.to_dict(orient="records")]
            ),
            "coverage_by_province": dg.MetadataValue.table(
                records=[dg.TableRecord(r) for r in coverage_df.to_dict(orient="records")]
            )
        }
    )
