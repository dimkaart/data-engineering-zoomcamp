from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("dtc-dez-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"./")
    return Path(f"./{gcs_path}")


@task(log_prints=True)
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to Big Query"""

    gcp_credentials_block = GcpCredentials.load("dtc-dez-service-acct")

    df.to_gbq(
        destination_table="dtc_dez_ny_taxi.rides_yellow",
        project_id="warm-ring-374916",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500000,
        if_exists="append",
    )


@flow(log_prints=True)
def etl_gcs_to_bq(month: int, year: int, color: str):
    """Main ETL flow to load data into Big Query"""
    path = extract_from_gcs(color, year, month)
    df = pd.read_parquet(path)
    print(f"rows: {len(df)}")
    write_bq(df)
    return len(df)


@flow(log_prints=True)
def etl_parent_flow(
    months: list[int] = [2, 3], year: int = 2019, color: str = "yellow"
):
    for month in months:
        total_rows_processed = etl_gcs_to_bq(month, year, color)
        print(f"Month: {month} rows: {total_rows_processed}")


if __name__ == "__main__":
    months = [2, 3]
    year = 2019
    color = "yellow"

    etl_parent_flow(months, year, color)
