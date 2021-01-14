import json
import time

from pyspark.sql import SparkSession

from processing import TaxiDataProcessor


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option") \
    .getOrCreate()
sc = spark.sparkContext
data_file = "data.txt"
df = sc.textFile(data_file, minPartitions=100).map(lambda x: eval(x))
td = TaxiDataProcessor(df)


n = 0
result_dict = {}
for hour, rides in td.most_intensive_timeframe():
   result_dict[n] = {}
   result_dict[n]['start'] = hour
   result_dict[n]['end'] = hour + 1
   result_dict[n]['qty_rides'] = rides
   n += 1
with open("results/timeframes.json", "w") as f:
   json.dump(result_dict, f, indent=4, sort_keys=True)

