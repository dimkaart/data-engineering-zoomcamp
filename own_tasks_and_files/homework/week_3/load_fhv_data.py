from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""

    df = pd.read_csv(dataset_url)
    print(df.dtypes)
    col = df.columns
    for elm in col:
        df[col] = df[col].astype(str)
    print(df.dtypes)
    return df

# test gcs

@task(log_prints=True)
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locallz as parquet file"""
    path = Path(f"{dataset_file}.parquet")
    df.to_parquet(path)
    return path


@task(log_prints=True)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("dtc-dez-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs(year: int, month: int) -> None:
    """The main ETL function"""
    dataset_file = f"fhv_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"

    
    df = fetch(dataset_url)    
    path = write_local(df, dataset_file)
    write_gcs(path)


@flow()
def etl_parent_flow(
    year: int = 2019, months: list[int] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
):
    for month in months:
        etl_web_to_gcs(year, month)

if __name__ == "__main__":
    etl_parent_flow()
