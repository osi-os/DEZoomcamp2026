#!/usr/bin/env python
# coding: utf-8

# In[1]:

import argparse

import pyspark
from pyspark.sql import SparkSession
import sys
import pandas as pd

parser = argparse.ArgumentParser()

parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output


# In[2]:


pyspark.__file__


# In[3]:


spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()


# In[4]:


from glob import glob


# In[6]:


#green_paths = glob('data/pq/green_tripdata_*/*/')
#yellow_paths = glob('data/pq/yellow_tripdata_*/*/')

df_green = spark.read.parquet(input_green)
df_yellow = spark.read.parquet(input_yellow)


# In[7]:


df_green = df_green.withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
                   .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')


# In[8]:


df_yellow = df_yellow.withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
                   .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')


# In[9]:


common_columns = []

yellow_columns = set(df_yellow.columns)

for col in df_green.columns:
    if col in yellow_columns:
        common_columns.append(col)


# In[11]:


common_columns #the above cell preserved the order of the columns


# In[10]:


from pyspark.sql import functions as F


# In[12]:


common_columns = ['VendorID',
     'pickup_datetime',
     'dropoff_datetime',
     'store_and_fwd_flag',
     'RatecodeID',
     'PULocationID',
     'DOLocationID',
     'passenger_count',
     'trip_distance',
     'fare_amount',
     'extra',
     'mta_tax',
     'tip_amount',
     'tolls_amount',
     'improvement_surcharge',
     'total_amount',
     'payment_type',
     'congestion_surcharge'
]


# In[13]:


df_green_sel = df_green \
    .select(common_columns) \
    .withColumn('service_type', F.lit('green'))  #labeling the green table data before creating the combined table


# In[14]:


df_yellow_sel = df_yellow \
    .select(common_columns) \
    .withColumn('service_type', F.lit('yellow')) #labeling the yellow table data before creating the combined table


# In[15]:


df_trips_data = df_green_sel.unionAll(df_yellow_sel)


# In[16]:


df_trips_data.groupBy('service_type').count().show()


# In[17]:


df_trips_data.createOrReplaceTempView('trips_data')


# In[18]:


df_result = spark.sql("""
SELECT 
    -- Revenue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_monthly_passenger_count,
    AVG(trip_distance) AS avg_monthly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
""")


# In[ ]:


df_result.coalesce(1).write.parquet(output, mode='overwrite')

