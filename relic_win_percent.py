# Win percentage for each relic 
# Uses pure python transformations.

import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

config = configparser.ConfigParser()
config.read('sts.ini')

spark = SparkSession.builder.appName('STS').getOrCreate()

input = spark.read.parquet(config['DEFAULT']['processed_data_directory'] + "/*")

# Casting victory to integer gives 1 for true, 0 for false, which lets us use it in sum to get the win count.
win_rates = (input.select(col('victory').cast('integer'), explode(col('relics')).alias('relic'))
    .groupBy('relic')
    .agg(sum('victory').alias('wins'),
         count('victory').alias('total'),
         (sum('victory') / count('victory') * 100).alias('pct')))

# Show the "best" and "worst"
win_rates.orderBy('pct').show(20)
win_rates.orderBy('pct', ascending=False).show(20)
