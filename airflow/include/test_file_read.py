####### project_file_read.py
## CLI usage:
## spark-submit --master <URL> \
#       --name <app-name> \
#       --jars </abs/path/to/connector.jar> \
#       --py-files city_vars.py \
#       project_file_read.py \
#           <fpath>
#
# optional args:
#           --city=<city> \
#           --gs=${GCP_GCS_BUCKET}

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
parser = argparse.ArgumentParser(
    description = 'Read CSV file into Spark, divide into years, and write to Parquets.',
    epilog = "Provide optional args only if not available as env vars.")
parser.add_argument('fpath', help = 'CSV file name and path prefix, e.g. <dir1>/<subdir>/<fname>.<ext>')
parser.add_argument('--city',
    choices = ['Chicago', 'San Francisco', 'Los Angeles', 'Austin'],
    help = 'specify 1 of the 4 city templates')
parser.add_argument('--gs', help = 'GCS bucket URL in gs:// format')
args = parser.parse_args()

# parsed input
fpath = args.fpath
# env vars or parsed inputs
city = os.environ.get('city_name', args.city)
gs_bkt = os.environ.get('gcs_bkt', args.gs)
creds_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

# for city-specific data
dict_city = dict_cities[city]

def parse_dt(dt_str):
    """
    parse datetime object from given date string of specific format
    """
    return pdl.from_format(dt_str, dict_city['date_format'])
parse_dt_udf = F.udf(parse_dt, returnType=types.TimestampType())

logging.info('----------------- creds path is' + creds_path)
print('----------------- creds path is' + creds_path)
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
    .csv(f"{gs_bkt}/{fpath}")

####### CHECKER ############################
logging.info('----------------- df_csv.head(10) is' + df_csv.head(10))
logging.info('----------------- df_csv.count() is' + df_csv.count())

# parse datetime out of provided date column
df_time = df_csv.withColumn('Timestamp', parse_dt_udf(F.col(dict_city['date_string_col'])))

