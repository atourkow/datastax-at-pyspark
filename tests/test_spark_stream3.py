from __future__ import print_function

import sys
import os

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

'''
 To run this on your local machine, you need to first run a Netcat server
    `$ nc -lk 9999`
'''

if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreamingNetworkWordCount")
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint(os.getcwd())

    def updateFunc(new_values, last_sum):
        return sum(new_values) + (last_sum or 0)

    lines = ssc.socketTextStream("127.0.0.1", 6000)
    counts = lines.flatMap(lambda line: line.split(" "))\
                  .map(lambda word: (word, 1))\
                  .updateStateByKey(updateFunc)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()