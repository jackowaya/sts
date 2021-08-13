# Win percentage for each relic 
# Uses SQL transformations.

import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

config = configparser.ConfigParser()
config.read('sts.ini')

spark = SparkSession.builder.appName('STS').getOrCreate()

input = spark.read.parquet(config['DEFAULT']['processed_data_directory'] + "/*")

# Casting victory to integer gives 1 for true, 0 for false, which lets us use it in sum to get the win count.
# There doesn't seem to be an array with the final deck, so we'll use card_choices for now.
# Start by exploding the array of card choices
cards = input.select('victory', explode(col('card_choices')).alias('card_choices'))

cards.createOrReplaceTempView("runs")
win_rates = spark.sql("""SELECT card_choices.picked, sum(cast(victory as integer)) as wins, 
    count(*) as total, sum(cast(victory as integer)) / count(*) * 100 as pct
    from runs where card_choices.picked is not null
    group by card_choices.picked""")

# Show the "best" and "worst"
win_rates.orderBy('pct').show(20)
win_rates.orderBy('pct', ascending=False).show(20)
