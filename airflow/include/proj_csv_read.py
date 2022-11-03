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
parser = argparse.ArgumentParser(description = 'Read CSV file into Spark, divide into years, add standard timestamps, and write to Parquets.')
parser.add_argument('city_proper',
    choices = ['Chicago', 'San Francisco', 'Los Angeles', 'Austin'],
    help = 'specify 1 of the 4 cities for its corresponding template')
parser.add_argument('fpath', help = 'CSV file name and path prefix (if any), e.g. <dir1>/<subdir>/<fname>.<ext>')
args = parser.parse_args()

# parsed inputs
city_proper = args.city_proper
fpath = args.fpath
gs_bkt = os.getenv('GCP_GCS_BUCKET')
creds_path = os.getenv('SPARK_CREDENTIALS')

# for city-specific data
dict_city = dict_cities[city_proper]

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
    .csv(f'{gs_bkt}/{fpath}')

# parse datetime from provided date column of specific format
p_func = lambda s: pdl.from_format(s, dict_city['date_format'])
parse_dt_udf = F.udf(p_func, returnType=types.TimestampType())
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
        df_month.write \
            .parquet(f"{gs_bkt}/pq/from_raw/{dict_city['formatted']}/{year}/{month:02}", mode='overwrite')
