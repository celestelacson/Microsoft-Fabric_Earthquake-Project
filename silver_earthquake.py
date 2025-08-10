#!/usr/bin/env python
# coding: utf-8

# ## silver_earthquake
# 
# New notebook

# ```
# # Worldwide Earthquake Events API - Silver Layer Processing
# ```

# In[ ]:


from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType


# In[4]:


# df now is a Spark DataFrame containing JSON data
df = spark.read.option("multiline", "true").json(f"Files/{start_date}_earthquake_data.json")

display(df)


# In[9]:


display(df.select(col('geometry.coordinates').getItem(0)))


# In[10]:


# Reshape earthquake data by extracting and renaming key attributes for further analysis.
df = \
df.\
    select(
        'id',
        col('geometry.coordinates').getItem(0).alias('longitude'),
        col('geometry.coordinates').getItem(1).alias('latitude'),
        col('geometry.coordinates').getItem(2).alias('elevation'),
        col('properties.title').alias('title'),
        col('properties.place').alias('place_description'),
        col('properties.sig').alias('sig'),
        col('properties.mag').alias('mag'),
        col('properties.magType').alias('magType'),
        col('properties.time').alias('time'),
        col('properties.updated').alias('updated')
        )


# In[11]:


display(df)


# In[12]:


# Convert 'time' and 'updated' columns from milliseconds to timestamp format for clearer datetime representation.
df = df.\
    withColumn('time', col('time')/1000).\
    withColumn('updated', col('updated')/1000).\
    withColumn('time', col('time').cast(TimestampType())).\
    withColumn('updated', col('updated').cast(TimestampType()))


# In[14]:


# appending the data to the gold table
df.write.mode('append').saveAsTable('earthquake_events_silver')

