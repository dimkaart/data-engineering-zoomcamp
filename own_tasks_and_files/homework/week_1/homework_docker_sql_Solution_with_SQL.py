# Question 3. Count records
sql_Q3 = """
SELECT Count(1)
FROM green_taxi_data
WHERE lpep_pickup_datetime BETWEEN '2019-01-15 00:00:00' AND '2019-01-15 23:59:59'
AND lpep_dropoff_datetime BETWEEN '2019-01-15 00:00:00' AND '2019-01-15 23:59:59'
"""

# Question 4. Largest trip for each day
sql_Q4 = """
SELECT CAST(lpep_pickup_datetime AS Date) AS date_var, MAX(trip_distance)
FROM green_taxi_data
WHERE CAST(lpep_pickup_datetime AS Date) IN ('2019-01-10','2019-01-15','2019-01-18','2019-01-28')
GROUP BY date_var
ORDER BY date_var
"""

# Question 5. The number of passengers
sql_Q5 = """
SELECT passenger_count, Count(1)
FROM green_taxi_data
WHERE CAST(lpep_pickup_datetime AS DATE) IN ('2019-01-01')
AND passenger_count IN (2,3)
GROUP BY passenger_count
ORDER BY passenger_count
"""

# Question 6. Largest tip
sql_Q6 = """
SELECT MAX(gtd.tip_amount) as max_tip_amount, z_DO."Zone" as end_zone
FROM green_taxi_data as gtd
LEFT JOIN zones as z_PU ON gtd."PULocationID" = z_PU."LocationID"
LEFT JOIN zones as z_DO ON gtd."DOLocationID" = z_DO."LocationID"
WHERE z_PU."Zone" = 'Astoria'
GROUP BY z_DO."Zone"
ORDER BY max_tip_amount DESC
LIMIT 10
"""
