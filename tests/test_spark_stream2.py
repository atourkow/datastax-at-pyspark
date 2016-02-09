from __future__ import print_function
import os

from pyspark.sql import SQLContext, Row
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext


sc = SparkContext(appName="Ratings Stream Test", master="spark://127.0.0.1:7077")
ssc = StreamingContext(sc, 2)
ssc.checkpoint(os.getcwd())

lines = ssc.socketTextStream("127.0.0.1", 6000)

# Persists the counts of the movies
def updateCounts(new_values, last_sum):
    return sum(new_values) + (last_sum or 0)

lines_split = lines.map(lambda line: line.split("::"))

rating_count = lines_split\
    .map(lambda (user_id, movie_id, rating, ts): ((movie_id), 1))\
    .updateStateByKey(updateCounts)

rating_count.pprint()

ssc.start()
ssc.awaitTermination()
