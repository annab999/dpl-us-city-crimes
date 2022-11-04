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
parser.add_argument('fpath', help = 'CSV file name and path prefix (if any), e.g. <dir1>/<subdir>/<fname>.<ext>')
parser.add_argument('outdir', help = 'output path prefix replacing old prefix in fpath')
args = parser.parse_args()

# parsed inputs
fpath = args.fpath
outdir = args.outdir
city_proper = os.getenv('CITY_PROPER')
gs_bkt = os.getenv('GCP_GCS_BUCKET')
creds_path = os.getenv('SPARK_CREDENTIALS')

opath = fpath.replace('raw/', outdir).replace(os.getenv('IN_FMT'), os.getenv('OUT_FMT'))

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

# repartitioning for large files
if dict_city['csv_parts'] > 1 and 'Present' not in fpath:
    df_csv = df_csv.repartition(dict_city['csv_parts'])

df_csv.write \
    .parquet(f'{gs_bkt}/{opath}', mode='overwrite')
