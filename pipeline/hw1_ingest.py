#!/usr/bin/env python
# coding: utf-8

# In[4]:


import pandas as pd

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet"
url2 = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"

df = pd.read_parquet(url)
dc = pd.read_csv(url2)


# In[3]:


df.head()


# In[5]:


dc.head()


# In[7]:


df.dtypes


# In[9]:


dc.dtypes


# In[10]:


get_ipython().system('uv add sqlalchemy psycopg2-binary')


# In[11]:


from sqlalchemy import create_engine
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# In[12]:


print(pd.io.sql.get_schema(df, name='green_trip_data', con=engine))


# In[13]:


print(pd.io.sql.get_schema(dc, name='taxi_zone_data', con=engine))


# In[14]:


df.head(n=0).to_sql(name='green_trip_data', con=engine, if_exists='replace')


# In[15]:


dc.head(n=0).to_sql(name='taxi_zone_data', con=engine, if_exists='replace')


# In[23]:


parse_dates = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime"
]


# In[22]:


get_ipython().system('pip3 install pyarrow')


# In[26]:


df = pd.read_parquet(
    url
)


# In[27]:


dc = pd.read_csv(
    url2
)


# In[28]:


df.to_sql(name='green_trip_data', con=engine, if_exists='append')


# In[29]:


dc.to_sql(name='taxi_zone_data', con=engine, if_exists='append')


# In[ ]:




