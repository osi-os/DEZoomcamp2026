# de-camp-docker-workshop

## First Module & Homework in the DE Zoomcamp Series

Answers below -

1. 
osiosman@MacBookPro ~/DEZoomcamp2026 [125]> docker run -it --entrypoint=bash --rm python:3.13
root@2544286ca0c7:/# pip --version
pip 25.3 from /usr/local/lib/python3.13/site-packages/pip (python 3.13)

2.
The answer is postgres:5432 because the name of the container pgadmin needs to connect to is
"postgres", and the port that belongs to it is identified by the 2nd number in the ports section, 5432:

services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data


3.
8007

SELECT COUNT(*)
FROM green_trip_data
WHERE green_trip_data."lpep_pickup_datetime" >= '2025-11-01'
	AND green_trip_data."lpep_pickup_datetime" < '2025-12-01'
	AND green_trip_data.trip_distance <= 1

4.
2025-11-14

SELECT * 
FROM green_trip_data
WHERE green_trip_data.trip_distance < 100
ORDER BY green_trip_data.trip_distance DESC;

5.
East Harlem North

SELECT taxi_zone_data."Zone", SUM(green_trip_data.total_amount) AS total_amount
FROM green_trip_data
INNER JOIN taxi_zone_data ON green_trip_data."PULocationID" = taxi_zone_data."LocationID"
WHERE green_trip_data.lpep_pickup_datetime >= '2025-11-18'
	AND green_trip_data.lpep_pickup_datetime < '2025-11-19'
GROUP BY taxi_zone_data."Zone"
ORDER BY total_amount DESC;

6.
Yorkville West

SELECT MAX(green_trip_data.tip_amount) as big_tip, green_trip_data."DOLocationID"
FROM green_trip_data
INNER JOIN taxi_zone_data ON green_trip_data."PULocationID" = taxi_zone_data."LocationID"
WHERE green_trip_data."PULocationID" = 74
	AND green_trip_data.lpep_pickup_datetime >= '2025-11-01'
	AND green_trip_data.lpep_pickup_datetime < '2025-12-01'
GROUP BY green_trip_data."DOLocationID"
ORDER BY big_tip DESC;

select * from taxi_zone_data
where taxi_zone_data."LocationID" = 263;

7.
Please see the main.tf in the terrademo1 folder

terraform init, terraform apply -auto-approve, terraform destroy



## Second Module & Homework in the DE Zoomcamp Series


*Backfilling the data for the specified time period: From 1/1/2021 to 7/31/2021 for the yellow and green taxi data:
<br>
<br>
<img width="1154" height="699" alt="image" src="https://github.com/user-attachments/assets/83775356-86c5-40e1-a1e3-aa482c10f806" />
<img width="1160" height="332" alt="image" src="https://github.com/user-attachments/assets/c48c186b-9036-4e3e-8880-b4d09fc1d136" />
<br>
<br>
<br>
1. **134.5MB**
   <img width="1326" height="116" alt="image" src="https://github.com/user-attachments/assets/5ab33953-a7aa-4011-a483-ce48050803ed" />
<br>
<br>
<br>
2. green_tripdata_2020-04.csv
   <img width="1114" height="67" alt="image" src="https://github.com/user-attachments/assets/7b10be19-ba88-4820-b178-ce96fcae5846" />
<br>
<br>
<br>
3. 24,648,499
<br>
   --Rows for the Yellow Taxi data in 2020
<br>
	SELECT COUNT(*) FROM `kestra-learning-485617.zoomcamp.yellow_tripdata`
	WHERE filename >= "yellow_tripdata_2020-01.csv"
	AND filename < "yellow_tripdata_2021-01.csv"
<br>
<br>
<br>
4. 1734051
<br>
--Rows for the Green Taxi data in 2020
<br>
SELECT COUNT(*) FROM `kestra-learning-485617.zoomcamp.green_tripdata`
WHERE filename >= "green_tripdata_2020-01.csv"
AND filename < "green_tripdata_2021-01.csv"
<br>
<br>
<br>
5. 1925152
<br>
<img width="621" height="553" alt="image" src="https://github.com/user-attachments/assets/b73ad18a-3e8a-48ff-9fa8-d27fd1d9ba8d" />
<br>
<br>
<br>
6. Add a timezone property set to America/New_York in the Schedule trigger configuration
<br>
triggers:
<br>
<br>
<p>- id: green_schedule</p>  
<p>type: io.kestra.plugin.core.trigger.Schedule</p>  
<p>cron: "0 9 1 * *"</p>  
<p>inputs:</p>  
<p>taxi: green</p>
<br>
<br>
<p>- id: yellow_schedule</p>
<p>type: io.kestra.plugin.core.trigger.Schedule</p>
<p>cron: "0 10 1 * *"</p>
<p>inputs:</p>
<p>taxi: yellow</p>
<br>
<br>
<p>- id: new_york_schedule</p>
<p>type: io.kestra.plugin.core.trigger.Schedule</p>
<p>cron: "0 11 1 * *"</p>
<p>timezone: America/New_York</p>



