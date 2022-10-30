####### project_file_read.py
## CLI usage:
## spark-submit --master <URL> \
#       --name <app-name> \
#       --jars </abs/path/to/connector.jar> \
#       --py-files city_vars.py \
#       project_file_read.py \
#           <city> \
#           <fname> \
#           $GCP_GCS_BUCKET

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql import functions as F

import os
import argparse
import pendulum as pdl
import logging

from city_vars import dict_cities

# inputs
parser = argparse.ArgumentParser(description = 'Read CSV file into Spark, divide into years, and write to Parquets.')
parser.add_argument('city',
    choices = ['Chicago', 'San Francisco', 'Los Angeles', 'Austin'],
    help = 'specify 1 of the 4 city templates',
    required = True)
parser.add_argument('gs', help = 'GCS bucket URL in gs:// format', required = True)
parser.add_argument('fname', help = 'CSV file name', required = True)
args = parser.parse_args()

# parsed inputs
city = args.city
gs_bkt = args.gs
fname = args.fname

# for city-specific data
dict_city = dict_cities[city]
creds_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

def parse_dt(dt_str):
    """
    parse datetime object from given date string of specific format
    """
    return pdl.from_format(dt_str, dict_city['date_format'])
parse_dt_udf = F.udf(parse_dt, returnType=types.TimestampType())

# connect to GCS
sc = SparkContext(conf=SparkConf())
hconf = sc._jsc.hadoopConfiguration()
hconf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hconf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hconf.set("fs.gs.auth.service.account.json.keyfile", creds_path)
hconf.set("fs.gs.auth.service.account.enable", "true")

spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

df_csv = spark.read \
    .option("header", "true") \
    .schema(dict_city['schema_template']) \
    .csv(f"{gs_bkt}/raw/{dict_city['formatted']}/{fname}")

####### CHECKER ############################
logging.info('----------------- creds path is' + creds_path)
print('----------------- creds path is' + creds_path)
df_csv.head(10)
df_csv.count()

# parse datetime out of provided date column
df_time = df_csv.withColumn('Timestamp', parse_dt_udf(F.col(dict_city['date_string_col'])))

# parse year list
if dict_city['with_year_col']:
    years_rows = df_time \
        .select('Year')
else:
    years_rows = df_time \
        .select(F.year('Timestamp').alias('Year'))
years_rows = years_rows \
    .dropna() \
    .dropDuplicates(['Year']) \
    .collect()
years = [row.Year for row in years_rows]
years.sort()

# write to parquet
o_cols = df_time.columns
cols = [col.lower().replace(' ', '_') for col in o_cols]
for year in years:
    df = df_time.filter(F.year('Timestamp') == year)
    for month in range(1, 13):
        df_month = df.filter(F.month('Timestamp') == month)
        for i in range(len(o_cols)):
            df_month = df_month.withColumnRenamed(o_cols[i], cols[i])
        if dict_city['partitions'] > 1:
            df_month = df_month.repartition(dict_city['partitions'])
        df_month \
            .drop('Timestamp') \
            .write.parquet(f"{gs_bkt}/pq/from_raw/{dict_city['formatted']}/{year}/{month}", mode='overwrite')