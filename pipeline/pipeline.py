import sys
import pandas as pd

#Scenario - We have a year's worth of data, but we want to process December (month 12)
#Also, to save the item to a parquet file (that's what line 17 is saying) we need the pyarrow module
#We're going to set up a virtual environment (for teaching purposes) and download the pyarrow module there
#uv is what we're going to download for the virtual environment (a tool that manages virtual envs)
#We will also create a docker image from the virtual environment of python 3.13 and pyarrow installed and it'll run the below script

#Year's worth data scenario is below -

print("arguments", sys.argv)

month = int(sys.argv[1])

print(f"Running pipeline for month {month}")

df = pd.DataFrame({"day": [1,2], "number_of_passengers": [3,4]})
df['month'] = month 
print(df.head())

df.to_parquet(f"output_{month}.parquet")