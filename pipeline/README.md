# DE-Camp-Homework
First Module & Homework in the DE Zoomcamp series

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