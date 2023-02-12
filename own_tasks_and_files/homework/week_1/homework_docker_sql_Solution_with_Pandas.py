import pandas as pd

green_taxi_2019_01 = pd.read_csv(
    "D:\Courses\Coding Courses\git\data-engineering-zoomcamp\dtc_de_data\green_tripdata\green_tripdata_2019-01.csv"
)
taxi_zone_lookup = pd.read_csv(
    "D:\Courses\Coding Courses\git\data-engineering-zoomcamp\dtc_de_data\\taxi_zone_lookup.csv"
)

# Question 3. Count records
green_taxi_2019_01_15 = green_taxi_2019_01[
    (green_taxi_2019_01.lpep_pickup_datetime.between("2019-01-15", "2019-01-16"))
    & (green_taxi_2019_01.lpep_dropoff_datetime.between("2019-01-15", "2019-01-16"))
]

print("Answer 3.")
print(
    f"Earliest pickup: {min(green_taxi_2019_01_15.lpep_pickup_datetime)} --> Latest pickup: {max(green_taxi_2019_01_15.lpep_pickup_datetime)}"
)
print(
    f"Earliest dropoff: {min(green_taxi_2019_01_15.lpep_dropoff_datetime)} --> Latest dropoff: {max(green_taxi_2019_01_15.lpep_dropoff_datetime)}"
)
print(
    f"There are {green_taxi_2019_01_15.shape[0]} records of trips with a green taxi starting and ending on 2019-01-15"
)

# Question 4. Largest trip for each day
green_taxi_2019_01_10 = green_taxi_2019_01[
    green_taxi_2019_01.lpep_pickup_datetime.between("2019-01-10", "2019-01-11")
]
green_taxi_2019_01_15 = green_taxi_2019_01[
    green_taxi_2019_01.lpep_pickup_datetime.between("2019-01-15", "2019-01-16")
]
green_taxi_2019_01_18 = green_taxi_2019_01[
    green_taxi_2019_01.lpep_pickup_datetime.between("2019-01-18", "2019-01-19")
]
green_taxi_2019_01_28 = green_taxi_2019_01[
    green_taxi_2019_01.lpep_pickup_datetime.between("2019-01-28", "2019-01-29")
]

print("Answer 4.")
print(f"Longest distance on 2019-01-10: {max(green_taxi_2019_01_10.trip_distance)}")
print(f"Longest distance on 2019-01-15: {max(green_taxi_2019_01_15.trip_distance)}")
print(f"Longest distance on 2019-01-18: {max(green_taxi_2019_01_18.trip_distance)}")
print(f"Longest distance on 2019-01-28: {max(green_taxi_2019_01_28.trip_distance)}")

# Question 5. The number of passengers
green_taxi_2019_01_01 = green_taxi_2019_01[
    green_taxi_2019_01.lpep_pickup_datetime.between("2019-01-01", "2019-01-02")
]

green_taxi_2019_01_01_2passengers = green_taxi_2019_01_01[
    green_taxi_2019_01_01.passenger_count == 2
]
green_taxi_2019_01_01_3passengers = green_taxi_2019_01_01[
    green_taxi_2019_01_01.passenger_count == 3
]

print("Answer 5.")
print(
    f"On 2019-01-01 were {green_taxi_2019_01_01_2passengers.shape[0]} trips with 2 passengers"
)
print(
    f"On 2019-01-01 were {green_taxi_2019_01_01_3passengers.shape[0]} trips with 3 passengers"
)

# Question 6. Largest tip

# Find Zone ID of "Astoria"
astoria_zone_id = taxi_zone_lookup.LocationID[
    taxi_zone_lookup.Zone == "Astoria"
].values[0]
# Select only trips starting at "Astoria"
green_taxi_2019_01_start_astoria = green_taxi_2019_01[
    green_taxi_2019_01.PULocationID == astoria_zone_id
]
# Find largest tip
max_tip_astoria = max(green_taxi_2019_01_start_astoria.tip_amount)
max_tip_DOLocationID = green_taxi_2019_01_start_astoria[
    green_taxi_2019_01_start_astoria.tip_amount == max_tip_astoria
].DOLocationID.values[0]
# Find Zone Name of Zone ID "146"
max_tip_zone_name = taxi_zone_lookup.Zone[
    taxi_zone_lookup.LocationID == max_tip_DOLocationID
].values[0]

print("Answer 6.")
print(
    f"The largest tip was {max_tip_astoria} £ for a trip starting at Astoria and ending at {max_tip_zone_name}"
)
