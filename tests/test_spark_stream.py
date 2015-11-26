## This just tests that it can get a stream
# CREATE TABLE movie_ratings_time_series ( movie_id int, ts timeuuid, rating float, primary key (movie_id, ts) );


from pyspark.sql import SQLContext, Row
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from uuid import uuid1


conf = SparkConf() \
    .setAppName("Ratings Stream") \
    .setMaster("spark://127.0.0.1:7077") \

# set up our contexts
sc = SparkContext(conf=conf)
sql = SQLContext(sc)
stream = StreamingContext(sc, 1) # 1 second window
lines = stream.socketTextStream("127.0.0.1", 6000)



# Lazily instantiated global instance of SQLContext
def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']


def process_ratings(time, rdd):
    if (rdd.isEmpty()):
        print "============== RDD Is Empty. Give it a few to get the stream.  You started the stream right?"
        return

    print "============== %s ============" % str(time)

    # Test the RDD Coming in
    for s in rdd.collect():
        print s

    # Test the map
    ratings = rdd.map(lambda line: line.split("::"))
    for s in ratings.collect():
        print s

    # Test the rows
    row_rdd = ratings.map(
        lambda (user_id, movie_id, rating, ts):
        Row(movie_id=int(movie_id), user_id=int(user_id), rating=float(rating), timestamp=int(ts))
        )
    for s in row_rdd.collect():
        print s

    '''
    local_sql = getSqlContextInstance(rdd.context)
    ratings = local_sql.createDataFrame(row_rdd, samplingRatio=1)

    from pyspark.sql import functions as F
    avg_ratings = ratings.groupBy("movie_id", "timestamp").agg(F.avg(ratings.rating).alias('rating'))
    for s in avg_ratings.collect():
        print s

    '''
    print "============== DONE WRITING ============"


# https://spark.apache.org/docs/latest/api/python/pyspark.streaming.html#pyspark.streaming.StreamingContext.socketTextStream
lines.foreachRDD(process_ratings)

stream.start()
stream.awaitTermination()
