CREATE OR REPLACE EXTERNAL TABLE `warm-ring-374916.dtc_dez_ny_taxi.external_fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://prefect-dtc-dez/data/fhv/fhv_tripdata_2019-*.csv.gz']
);

-- Question 1
SELECT COUNT(*) 
FROM warm-ring-374916.dtc_dez_ny_taxi.external_fhv_tripdata;

-- Question 2
SELECT COUNT( DISTINCT Affiliated_base_number) 
FROM warm-ring-374916.dtc_dez_ny_taxi.external_fhv_tripdata;

SELECT COUNT( DISTINCT Affiliated_base_number)
FROM warm-ring-374916.dtc_dez_ny_taxi.fhv_tripdata;

-- Question 3
SELECT COUNT(*)
FROM warm-ring-374916.dtc_dez_ny_taxi.fhv_tripdata
WHERE PUlocationID IS NULL
AND DOlocationID IS NULL;

-- Question 4

-- Question 5
-- Creation of Tables
CREATE OR REPLACE TABLE warm-ring-374916.dtc_dez_ny_taxi.fhv_tripdata_partitioned 
PARTITION BY
  DATE(pickup_datetime) AS
SELECT * FROM warm-ring-374916.dtc_dez_ny_taxi.external_fhv_tripdata;

CREATE OR REPLACE TABLE warm-ring-374916.dtc_dez_ny_taxi.fhv_tripdata_partitioned_clustered
PARTITION BY
  DATE(pickup_datetime)
CLUSTER BY
  Affiliated_base_number AS
SELECT * FROM warm-ring-374916.dtc_dez_ny_taxi.external_fhv_tripdata;


-- Query for Execution
SELECT DISTINCT Affiliated_base_number
FROM warm-ring-374916.dtc_dez_ny_taxi.fhv_tripdata
WHERE pickup_datetime BETWEEN TIMESTAMP("2019-03-01") AND TIMESTAMP("2019-03-31");

SELECT DISTINCT Affiliated_base_number
FROM warm-ring-374916.dtc_dez_ny_taxi.fhv_tripdata_partitioned
WHERE pickup_datetime BETWEEN TIMESTAMP("2019-03-01") AND TIMESTAMP("2019-03-31");

SELECT DISTINCT Affiliated_base_number
FROM warm-ring-374916.dtc_dez_ny_taxi.fhv_tripdata_partitioned_clustered
WHERE pickup_datetime BETWEEN TIMESTAMP("2019-03-01") AND TIMESTAMP("2019-03-31");

-- Question 6

-- Question 7

-- Question 8