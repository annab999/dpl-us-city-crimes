#!/usr/bin/env python
# coding: utf-8

import pyspark

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

from pyspark.sql import types
from pyspark.sql import functions as F

import os
import pandas as pd
import pendulum as pdl

# # inputs
# - city
# - fname

city = 'chicago'
fname = ''

gcs_bkt = os.getenv('GCP_GCS_BUCKET')

jar_path = os.getenv('JAR_FILE_LOC')
creds_path = '/.google/credentials/' + os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

conf = SparkConf()     .setMaster('local[*]')     .setAppName('proj_file_read')     .set("spark.jars", jar_path)     .set("spark.hadoop.google.cloud.auth.service.account.enable", "true")     .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", creds_path)

sc = SparkContext(conf=conf)

hconf = sc._jsc.hadoopConfiguration()

hconf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hconf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hconf.set("fs.gs.auth.service.account.json.keyfile", creds_path)
hconf.set("fs.gs.auth.service.account.enable", "true")

spark = SparkSession.builder     .config(conf=sc.getConf())     .getOrCreate()

# ### 1-time sample download for pandas reading to infer schema for everything else:
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

# ### check count here: raw csv file
# command:
# `!wc -l rows.csv?accessType=DOWNLOAD`
# 
# output:
# `336247 rows.csv?accessType=DOWNLOAD`

# ### Because of `TypeError: Can not merge type (pandas string to spark double) for 'Location Description', 'location' fields`
# Commands:
# ```
# cols = ['Case Number', 'Date', 'Block', 'IUCR', 'Primary Type', 'Description', 'Arrest', 'Domestic', 'Beat', 'Ward', 'FBI Code', 'X Coordinate', 'Y Coordinate', 'Year', 'Latitude', 'Longitude']
# df_pandas = pd.read_csv('rows.csv?accessType=DOWNLOAD', nrows=100, usecols=cols)
# ```

# Command:
# `spark.createDataFrame(df_pandas).schema`
# 
# Output:
# ```
# StructType([StructField('Case Number', StringType(), True), StructField('Date', StringType(), True), StructField('Block', StringType(), True), StructField('IUCR', StringType(), True), StructField('Primary Type', StringType(), True), StructField('Description', StringType(), True), StructField('Arrest', BooleanType(), True), StructField('Domestic', BooleanType(), True), StructField('Beat', LongType(), True), StructField('Ward', LongType(), True), StructField('FBI Code', StringType(), True), StructField('X Coordinate', DoubleType(), True), StructField('Y Coordinate', DoubleType(), True), StructField('Year', LongType(), True), StructField('Latitude', DoubleType(), True), StructField('Longitude', DoubleType(), True)])
# ```

# In[11]:


# modified pattern from pandas schema
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


# ### Replace below with me:
# ```
# df_csv = spark.read \
#     .option("header", "true") \
#     .schema(schema_template) \
#     .csv(f'{gcs_bkt}/raw/{city}/{fname}')
# ```

# In[12]:


df_csv = spark.read     .option("header", "true")     .schema(schema_template)     .csv(f'{gcs_bkt}/raw/{city}/' + 'Crimes_-_2001.csv')


# ### check count here: original df
# Command:
# `df_csv.count()`
# 
# Output:
# `485853`

# ### check count Jan: csv df, strdate col
# Command:
# ```
# df_csv \
#     .filter(F.split('Date', ' ').getItem(0).startswith('01')) \
#     .count()
# ```
# Output:
# `38114`

# ### check date Jan1: csv df, strdate col
# Command:
# ```
# df_csv \
#     .filter(F.split('Date', ' ').getItem(0) == '01/01/2001') \
#     .count()
# ```
# Output:
# `1825`

years_rows = df_csv     .select('Year')     .dropna()     .dropDuplicates(['Year'])     .collect()

years = [row.Year for row in years_rows]


# ### check parsed years
# Command:
# `print(years)`
# 
# Output:
# `[2001]`

cols = [col.lower().replace(' ', '_') for col in df_csv.columns]

for year in years:
    df = df_csv.filter(F.col('Year') == year)
    for month in range(1, 13):
        df_month = df.filter(F.split('Date', ' ').getItem(0).startswith(f'{month:02}/'))
        for i in range(len(df_csv.columns)):
            df_month = df_month.withColumnRenamed(df_csv.columns[i], cols[i])
        df_month             .write.parquet(f'{gcs_bkt}/pq/{city}/{year}/{month}', mode='overwrite')


# In[ ]:


sc.stop()

