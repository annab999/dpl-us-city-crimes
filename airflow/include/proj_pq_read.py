## CLI usage:
## spark-submit --master <URL> \
#       --name <app-name> \
#       --jars </abs/path/to/connector.jar> \
#       --py-files city_vars.py \
#       <this_file>.py \
#           <args>

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql import functions as F

import os
import argparse
import pendulum as pdl

from city_vars import dict_cities

# inputs
parser = argparse.ArgumentParser(description = 'Read Parquet file into Spark, clean rows, add standard timestamps, divide into months.')
parser.add_argument('csv_fpath', help = 'CSV file name and path prefix (if any), e.g. <dir1>/<subdir>/<fname>.<ext>')
parser.add_argument('pq_dir', help = 'Parquet path prefix replacing old prefix in csv_fpath')
args = parser.parse_args()

# parsed inputs
csv_fpath = args.csv_fpath
pq_dir = args.pq_dir
city_proper = os.getenv('CITY_PROPER')
gs_bkt = os.getenv('GCP_GCS_BUCKET')
creds_path = os.getenv('SPARK_CREDENTIALS')

# for city-specific data
dict_city = dict_cities[city_proper]
in_path = csv_fpath.replace(os.getenv("PREFIX_RAW"), pq_dir).replace(os.getenv('IN_FMT'), '')
out_path = f"{os.getenv('PREFIX_ORGANIZED')}/{dict_city['formatted']}"

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

df_in = spark.read.parquet(f'{gs_bkt}/{in_path}/*')

# filter out rows with null values in important columns
# parse datetime from provided date column of specific format
# drop old date col
p_func = lambda s: pdl.from_format(s, dict_city['date_format'])
parse_dt_udf = F.udf(p_func, returnType=types.TimestampType())

df_time = df_in \
    .filter(F.col(dict_city['date_string_col']).isNotNull()) \
    .withColumn('Timestamp', parse_dt_udf(F.col(dict_city['date_string_col']))) \
    .drop(dict_city['date_string_col'])

# parse year list
if dict_city['with_year_col']:
    years_rows = df_time \
        .select('Year')
    df_time.drop('Year')
else:
    years_rows = df_time \
        .select(F.year('Timestamp').alias('Year'))
years_rows = years_rows \
    .dropna() \
    .dropDuplicates(['Year']) \
    .collect()
years = [row.Year for row in years_rows]
years.sort()

# clean col names
# repartition if needed
# write to per-month parquet
o_cols = df_time.columns
cols = [col.lower().replace(' ', '_') for col in o_cols]
for year in years:
    df = df_time.filter(F.year('Timestamp') == year)
    for month in range(1, 13):
        df_month = df.filter(F.month('Timestamp') == month)
        for i in range(len(o_cols)):
            df_month = df_month.withColumnRenamed(o_cols[i], cols[i])
        if dict_city['pq_parts'] > 1:
            df_month = df_month.repartition(dict_city['pq_parts'])
        df_month.write \
            .parquet(f"{gs_bkt}/{out_path}/{year}/{month:02}", mode='overwrite')
