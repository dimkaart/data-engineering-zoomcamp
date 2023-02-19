## Week 4 Homework 

In this homework, we'll use the models developed during the week 4 videos and enhance the already presented dbt project using the already loaded Taxi data for fhv vehicles for year 2019 in our DWH.

We will use the data loaded for:

* Building a source table: `stg_fhv_tripdata`
* Building a fact table: `fact_fhv_trips`
* Create a dashboard 

If you don't have access to GCP, you can do this locally using the ingested data from your Postgres database
instead. If you have access to GCP, you don't need to do it for local Postgres -
only if you want to.

> **Note**: if your answer doesn't match exactly, select the closest option 

### Question 1: 

**What is the count of records in the model fact_trips after running all models with the test run variable disabled and filtering for 2019 and 2020 data only (pickup datetime)** 

You'll need to have completed the "Build the first dbt models" video and have been able to run the models via the CLI. 
You should find the views and models for querying in your DWH.

- 41648442
- 51648442
- 61648442
- 71648442

### Anwer 1.
The count of records in the model fact_trips for the years 2019 and 2020 is nearly `61,648,442` (more precise it is `61,505,432`) as can be seen in the image below.

![alt text](../../images/dbt_question1_1.png)
![alt text](../../images/dbt_question1_2.png)

### Question 2: 

**What is the distribution between service type filtering by years 2019 and 2020 data as done in the videos**

You will need to complete "Visualising the data" videos, either using data studio or metabase. 

- 89.9/10.1
- 94/6
- 76.3/23.7
- 99.1/0.9

### Answer 2.
The distributioin between service type for the years 2019 and 2020 is nearly `89.9/10.1` (more precise `89.8/10.2` as can be seein in the image below).

![alt text](../../images/dbt_question2.png)

### Question 3: 

**What is the count of records in the model stg_fhv_tripdata after running all models with the test run variable disabled (:false)**  

Create a staging model for the fhv data for 2019 and do not add a deduplication step. Run it via the CLI without limits (is_test_run: false).
Filter records with pickup time in year 2019.

- 33244696
- 43244696
- 53244696
- 63244696


### Answer 3.
The count of records in the model stg_fhv_tripdata is nearly `43,244,696` (more precise `43,183,729` as can be seen in the image below)

![alt text](../../images/dbt_question3_1.png)
![alt textt](../../images/dbt_question3_2.png) 


If <b>only records with known pickup and dropoff locations entries</b> are considered, the number of records drops to `41,155,234`.
![alt text](../../images/dbt_question3_3.png)
![alt textt](../../images/dbt_question3_4.png)


### Question 4: 

**What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)**  

Create a core model for the stg_fhv_tripdata joining with dim_zones.
Similar to what we've done in fact_trips, keep only records with known pickup and dropoff locations entries for pickup and dropoff locations. 
Run it via the CLI without limits (is_test_run: false) and filter records with pickup time in year 2019.

- 12998722
- 22998722
- 32998722
- 42998722

### Answer 4.
The number of records in the model fact_fhv_trips is nearly `22,998,722` (more precise `22,989,750` as can be seen in the image below).

![alt text](../../images/dbt_question4.png)


### Question 5: 

**What is the month with the biggest amount of rides after building a tile for the fact_fhv_trips table**
Create a dashboard with some tiles that you find interesting to explore the data. One tile should show the amount of trips per month, as done in the videos for fact_trips, based on the fact_fhv_trips table.

- March
- April
- January
- December

### Answer 5.
The month with the most trips for the model fact_fhv_trips is `January` as can be seen in the image below. Furthermore, for additional information, the second image contains a dashboard for trips take not in January due to the large discrepancy in numbers between January and the rest of the year.

![alt text](../../images/dbt_question5_1.png)
![alt text](../../images/dbt_question5_2.png)



## Submitting the solutions

* Form for submitting: https://forms.gle/6A94GPutZJTuT5Y16
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 25 February (Saturday), 22:00 CET


## Solution

We will publish the solution here
