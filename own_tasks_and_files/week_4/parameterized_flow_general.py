from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect.tasks import task_input_hash
from datetime import timedelta


# Read data from web
@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df


# Read data from gcs
@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("dtc-dez-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"./")
    return Path(f"./{gcs_path}")

# Transform data local
@task(log_prints=True)
def clean_yellow(df=pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task(log_prints=True)
def clean_green(df=pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task(log_prints=True)
def clean_fhv(df=pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["Pickup_datetime"] = pd.to_datetime(df["Pickup_datetime"])
    df["DropOff_datetime"] = pd.to_datetime(df["DropOff_datetime"])
    
    df["Dispatching_base_num"] = df["Dispatching_base_num"].astype('int')
    df["PULocationID"] = df["PULocationID"].astype('int')
    df["DOLocationID"] = df["DOLocationID"].astype('int')
    df["SR_Flag"] = df["SR_Flag"].astype("int")
    df["SR_Flag"] = df["SR_Flag"].fillna(0)
    
    return df

# Transform data gcs
@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


# Write files

@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locallz as parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task(log_prints=True)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("dtc-dez-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return

@task(log_prints=True)
def write_bq(df: pd.DataFrame, color:str) -> None:
    """Write DataFrame to Big Query"""

    gcp_credentials_block = GcpCredentials.load("dtc-dez-service-acct")

    df.to_gbq(
        destination_table=f"dtc_dez_ny_taxi.rides_{color}",
        project_id="warm-ring-374916",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500000,
        if_exists="append",
    )

@flow()
def etl_web_to_gcs(color: str, year: int, month: int) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    if color == "yellow":
        df_clean = clean_yellow(df)
    elif color == "green":
        df_clean = clean_green(df)
    elif color == "fhv":
        df_clean = clean_fhv(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)


@flow()
def etl_gcs_to_bq(color: str, year: int, month: int) -> None:
    """Main ETL flow to load data into Big Query"""
    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df, color)


@flow()
def etl_parent_flow(
    colors: list[str] = ["green", "yellow"], years: list[int] = [2019, 2020], months: list[int] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
):
    for color in colors:
        for year in years:
            for month in months:
                print(color, year, month)
                #etl_web_to_gcs(color, year, month)
                etl_gcs_to_bq(color, year, month)









if __name__ == "__main__":
    etl_parent_flow()
