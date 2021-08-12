# Transformation code to convert JSON to Parquet, remove records we think are invalid
# and add other transformations we think will make our lives easier.

import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

config = configparser.ConfigParser()
config.read('sts.ini')

spark = SparkSession.builder.appName('STS Prep').getOrCreate()

input = spark.read.json(config['DEFAULT']['data_file_directory'] + "/*")

flattened_input = input.select('event.*')
filtered_input = flattened_input.where(col('chose_seed') == False).where(col('playtime') > 60)

# Output to Parquet
filtered_input.write.format("parquet").save(config['DEFAULT']['processed_data_directory'])
