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
from pyspark.sql import functions as F

import os
import argparse

from city_vars import dict_common, dict_cities

# inputs
parser = argparse.ArgumentParser(description = 'Read monthly Parquets, clean and choose columns, combine cities per month.')
parser.add_argument('city_proper',
    choices = ['Chicago', 'San Francisco', 'Los Angeles', 'Austin'],
    help = 'specify 1 of the 4 cities for its corresponding template')
parser.add_argument('year', help = 'year of data to use, in YYYY format')
parser.add_argument('zmonth', help = 'zero-padded month of data to use, in 0m format')
args = parser.parse_args()

# parsed inputs
city_proper = args.city_proper
year = args.year
zmonth = args.zmonth
gs_bkt = os.getenv('GCP_GCS_BUCKET')
creds_path = os.getenv('SPARK_CREDENTIALS')

# for city-specific data
dict_city = dict_cities[city_proper]
in_path = f"{os.getenv('PREFIX_ORGANIZED')}/{dict_city['formatted']}"
out_path = f"{os.getenv('PREFIX_CLEAN')}/{dict_city['formatted']}"

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

df_pq = spark.read.parquet(f"{gs_bkt}/{in_path}/{year}/{zmonth}")

# filter out duplicates
# select columns to remove any similarly named columns
# standardize column names
o_cols = dict_city['ordered_cols']
cols = dict_city['renamed_cols']
df_ordered = df_pq \
    .distinct() \
    .select(o_cols)
for i in range(len(o_cols)):
    df_ordered = df_ordered \
        .withColumnRenamed(o_cols[i], cols[i])

# parse relevant columns
parser_udf = F.udf(dict_city['parser'], returnType=dict_common['p_ret_type'])

# parse some columns
# add common columns
# pick out final columns
#   .withColumn(dict_common['str_col'], F.lit('UNKNOWN')) \
df_select = df_ordered \
    .fillna('UNKNOWN', dict_common['p_col']) \
    .withColumn(dict_common['p_col'], parser_udf(F.col(dict_common['p_col']))) \
    .withColumn('city', F.lit(city_proper)) \
    .withColumn(dict_common['int_col'], F.lit(9999)) \
    .withColumn(dict_city['new_col'], F.col(dict_city['new_val_from'])) \
    .select(dict_common['minimal'])

df_select.write.parquet(f"{gs_bkt}/{out_path}/{year}/{zmonth}", mode='overwrite')
