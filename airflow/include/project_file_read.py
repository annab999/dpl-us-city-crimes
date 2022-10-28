#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext


# In[2]:


from pyspark.sql import types
from pyspark.sql import functions as F


# In[3]:


import os
import pandas as pd
import pendulum as pdl


# # inputs
# - city
# - fname

# In[4]:


city = 'san_francisco'
fname = ''


# In[30]:


# for city-specific data
cities = ['Chicago', 'San Francisco', 'Los Angeles', 'Austin']
f_cities = [c.replace(' ', '_').lower() for c in cities]


# In[5]:


gcs_bkt = os.getenv('GCP_GCS_BUCKET')


# In[6]:


jar_path = os.getenv('JAR_FILE_LOC')
creds_path = '/.google/credentials/' + os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

conf = SparkConf()     .setMaster('local[*]')     .setAppName('proj_file_read')     .set("spark.jars", jar_path)     .set("spark.hadoop.google.cloud.auth.service.account.enable", "true")     .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", creds_path)


# ### Only if an existing one already runs:
# `sc.stop()`

# In[7]:


sc = SparkContext(conf=conf)


# In[8]:


hconf = sc._jsc.hadoopConfiguration()

hconf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hconf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hconf.set("fs.gs.auth.service.account.json.keyfile", creds_path)
hconf.set("fs.gs.auth.service.account.enable", "true")


# In[9]:


spark = SparkSession.builder     .config(conf=sc.getConf())     .getOrCreate()


# ***

# ### first, open dataset page and check data dictionary on columns

# ### 1-time sample download for pandas reading to infer schema for everything else:
# 
# command:
# `!wget https://data.cityofchicago.org/api/views/hx8q-mf9v/rows.csv?accessType=DOWNLOAD`
# 
# note: file is `Crimes_-_2012.csv`
# 
# output:
# ```
# --2022-10-22 08:53:42--  https://data.cityofchicago.org/api/views/hx8q-mf9v/rows.csv?accessType=DOWNLOAD
# Resolving data.cityofchicago.org (data.cityofchicago.org)... 52.206.140.205, 52.206.68.26, 52.206.140.199
# Connecting to data.cityofchicago.org (data.cityofchicago.org)|52.206.140.205|:443... connected.
# HTTP request sent, awaiting response... 200 OK
# Length: unspecified [text/csv]
# Saving to: ‘rows.csv?accessType=DOWNLOAD’
# 
# rows.csv?accessType     [          <=>       ]  75.99M  2.54MB/s    in 28s     
# 
# 2022-10-22 08:54:11 (2.68 MB/s) - ‘rows.csv?accessType=DOWNLOAD’ saved [79677853]
# ```

# In[79]:


get_ipython().system('wget https://data.sfgov.org/api/views/tmnf-yvry/rows.csv?accessType=DOWNLOAD')


# ### check count here: raw csv file
# command:
# `!wc -l rows.csv?accessType=DOWNLOAD`
# 
# chicago1: `485854 rows.csv?accessType=DOWNLOAD`
# chicago12: `336247 rows.csv?accessType=DOWNLOAD`
# austin: `35098 rows.csv?accessType=DOWNLOAD.1`
# los angeles: ` `
# san francisco: `2129526 rows.csv?accessType=DOWNLOAD.2`

# In[80]:


get_ipython().system('wc -l rows.csv?accessType=DOWNLOAD.2')


# In[82]:


df_pd = pd.read_csv('rows.csv?accessType=DOWNLOAD.2', nrows=1000)
df_pd.columns


# ### see sample of data
# Command: `df_pd`

# In[83]:


pd.set_option('max_colwidth', None)
print(df_pd.head())


# ### initial commands for all (1 cell each)
# Commands:
# ```
# df_pd = pd.read_csv('rows.csv?accessType=DOWNLOAD.1', nrows=1000)
# df_pd.columns
# ```
# get output, then:
# ```
# spark.createDataFrame(df_pd).schema
# ```
# if with error, then:
# ```
# cols = <PASTE COLUMN LIST HERE, REMOVE PROBLEMATIC COL>
# 
# df_pd = pd.read_csv('<EDIT FILENAME>', nrows=1000, usecols=cols)
# ```
# 
# ### for Chicago because of `TypeError: Can not merge type (pandas string to spark double) for 'Location Description', 'location' fields`
# ```
# cols = ['Case Number', 'Date', 'Block', 'IUCR', 'Primary Type', 'Description', 'Arrest', 'Domestic', 'Beat', 'Ward', 'FBI Code', 'X Coordinate', 'Y Coordinate', 'Year', 'Latitude', 'Longitude']
# ```
# 
# ### for Austin because of `TypeError: Can not merge type (pandas string to spark double) for 'Clearance Status', 'Clearance Date', 'GO Location' fields`
# Commands:
# ```
# cols = ['GO Primary Key', 'Council District', 'GO Highest Offense Desc', 'Highest NIBRS/UCR Offense Description', 'GO Report Date', 'GO District', 'GO Location Zip', 'GO Census Tract', 'GO X Coordinate', 'GO Y Coordinate']
# ```

# In[84]:


spark.createDataFrame(df_pd).schema


# ### modify schema output above and removed columns, based on sample output before, then add template below

# In[50]:


# modified pattern from pandas schema
if city == f_cities[0]:
    schema_template = types.StructType([
        types.StructField('Case Number', types.StringType(), True),
        types.StructField('Date', types.StringType(), True),
        types.StructField('Block', types.StringType(), True),
        types.StructField('IUCR', types.StringType(), True),
        types.StructField('Primary Type', types.StringType(), True),
        types.StructField('Description', types.StringType(), True),
        types.StructField('Location Description', types.StringType(), True),
        types.StructField('Arrest', types.BooleanType(), True),
        types.StructField('Domestic', types.BooleanType(), True),
        types.StructField('Beat', types.StringType(), True),
        types.StructField('Ward', types.IntegerType(), True),
        types.StructField('FBI Code', types.StringType(), True),
        types.StructField('X Coordinate', types.FloatType(), True),
        types.StructField('Y Coordinate', types.FloatType(), True),
        types.StructField('Year', types.IntegerType(), True),
        types.StructField('Latitude', types.FloatType(), True),
        types.StructField('Longitude', types.FloatType(), True),
        types.StructField('Location', types.StringType(), True)
    ])
elif city == f_cities[1]:
    pass
elif city == f_cities[3]:
    schema_template = types.StructType([
    types.StructField('GO Primary Key', types.IntegerType(), True),
    types.StructField('Council District', types.IntegerType(), True),
    types.StructField('GO Highest Offense Desc', types.StringType(), True),
    types.StructField('Highest NIBRS/UCR Offense Description', types.StringType(), True),
    types.StructField('GO Report Date', types.StringType(), True),
    types.StructField('GO Location', types.StringType(), True),
    types.StructField('Clearance Status', types.StringType(), True),
    types.StructField('Clearance Date', types.StringType(), True),
    types.StructField('GO District', types.StringType(), True),
    types.StructField('GO Location Zip', types.IntegerType(), True),
    types.StructField('GO Census Tract', types.FloatType(), True),
    types.StructField('GO X Coordinate', types.IntegerType(), True),
    types.StructField('GO Y Coordinate', types.IntegerType(), True)
])


# ### Replace below with me:
# ```
# df_csv = spark.read \
#     .option("header", "true") \
#     .schema(schema_template) \
#     .csv(f'{gcs_bkt}/raw/{city}/{fname}')
# ```

# In[51]:


df_csv = spark.read     .option("header", "true")     .schema(schema_template)     .csv(f'{gcs_bkt}/raw/{city}/' + '2017_Annual_Crime.csv')


# In[81]:


df_csv.count()


# ### check count here: original df
# Command:
# `df_csv.count()`
# 
# Output:
# `485853`

# ### inspect data
# Command:
# ```
# df_csv.head(10)
# ```

# In[63]:


def parse_dt(dt_str):
    """
    parse datetime object from given date string of specific format
    """
    if city == f_cities[0]:
        fmt = 'MM/DD/YYYY HH:mm:ss A'
    elif city == f_cities[3]:
        fmt = 'D-MMM-YY'
    return pdl.from_format(dt_str, fmt)

parse_dt_udf = F.udf(parse_dt, returnType=types.TimestampType())


# In[75]:


# parse datetime out of provided date column
dt_str_col = {f_cities[0]: 'Date', f_cities[3]: 'GO Report Date'}
df_time = df_csv.withColumn('Timestamp', parse_dt_udf(F.col(dt_str_col[city])))

if city == f_cities[0]:
    years_rows = df_time         .select('Year')
elif city == f_cities[3]:
    years_rows = df_time         .select(F.year('Timestamp').alias('Year'))


# In[76]:


years_rows = years_rows     .dropna()     .dropDuplicates(['Year'])     .collect()

years = [row.Year for row in years_rows]


# ### check parsed years
# Command:
# `print(years)`
# 
# Output:
# `[2001]`

# ### check count Jan: csv df, strdate col
# Command: Chicago
# ```
# df_time \
#     .filter(F.month('Timestamp') == 1) \
#     .count()
# ```
# Chicago: `38114` Austin: `3098`

# In[87]:


df_time     .filter(F.month('Timestamp') == 1)     .filter(F.dayofmonth('Timestamp') == 1)     .count()


# ### check date Jan1: csv df, strdate col
# Command: Chicago
# ```
# df_time \
#     .filter(F.month('Timestamp') == 1) \
#     .filter(F.dayofmonth('Timestamp') == 1) \
#     .count()
# ```
# Chicago: `1825` Austin: `97`

# In[78]:


o_cols = df_time.columns
cols = [col.lower().replace(' ', '_') for col in o_cols]

for year in years:
    df = df_time.filter(F.year('Timestamp') == year)
    for month in range(1, 13):
        df_month = df.filter(F.month('Timestamp') == month)
        for i in range(len(o_cols)):
            df_month = df_month.withColumnRenamed(o_cols[i], cols[i])
        df_month             .drop('Timestamp', dt_str_col[city])             .write.parquet(f'{gcs_bkt}/pq/{city}/{year}/{month}', mode='overwrite')


# In[ ]:


sc.stop()

