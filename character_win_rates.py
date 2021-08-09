# Proof of concept code that just gives the win/lose counts per class

import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

config = configparser.ConfigParser()
config.read('sts.ini')

spark = SparkSession.builder.appName('STS').getOrCreate()

input = spark.read.json(config['DEFAULT']['data_file_directory'] + "/*")

# Pure python version
(input.select('event.character_chosen', 'event.victory')
    .groupBy('character_chosen', 'victory')
    .agg(count('victory'))
    .orderBy('character_chosen', 'victory')).show()

# Spark SQL Version
input.createOrReplaceTempView("runs")
spark.sql("""SELECT event.character_chosen, event.victory, count(*) from runs
    group by character_chosen, victory
    order by character_chosen, victory""").show()