#!/usr/bin/env python
# coding: utf-8

# ## bronze_earthquake
# 
# New notebook

# In[ ]:


import requests
import json

url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_date}&endtime={end_date}"

response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    data = data['features']

    file_path = f'/lakehouse/default/Files/{start_date}_earthquake_data.json'

    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)

    print(f"Data successfully save to {file_path}")
else:
    print("Failed to fetch data. Status code:", response.status_code)


# In[ ]:


df = spark.read.option("multiline", "true").json(f"Files/{start_date}_earthquake_data.json")
# df now is a Spark DataFrame containing JSON data from "Files/2025-07-29_earthquake_data.json".
display(df)

