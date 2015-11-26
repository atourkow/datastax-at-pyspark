from pyspark.sql import SQLContext, Row
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from uuid import uuid1



conf = SparkConf() \
    .setAppName("ratings stream") \
    .setMaster("spark://127.0.0.1:7077") \
    .set("spark.cassandra.connection.host", "127.0.0.1")

# set up our contexts
sc = SparkContext(conf=conf)
sql = SQLContext(sc)
stream = StreamingContext(sc, 1) # 1 second window

def create_writer(sql, keyspace, mode="append"):
    def writer(table, df):
        df.write.format("org.apache.spark.sql.cassandra").\
                 options(table=table, keyspace=keyspace).save(mode="append")
    return writer


# Lazily instantiated global instance of SQLContext
def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']



writer = create_writer(sql, "at_pyspark")
# https://spark.apache.org/docs/latest/api/python/pyspark.streaming.html#pyspark.streaming.StreamingContext.socketTextStream
lines = stream.socketTextStream("127.0.0.1", 6000)

def process_ratings(time, rdd):
    if (rdd.isEmpty()):
        print "============== RDD Is Empty. Give it a few moments to get the stream.  You started the stream right?"
        return

    print "============== %s ============" % str(time)

    import time
    ts = time.time()

    ratings = rdd.map(lambda line: line.split("::"))
    row_rdd = ratings.map(
        lambda (user_id, movie_id, rating, timestamp):
        Row( movie_id=int(movie_id), user_id=int(user_id), rating=float(rating), timestamp=int(timestamp) )
        )


    local_sql = getSqlContextInstance(rdd.context)
    ratings = local_sql.createDataFrame(row_rdd, samplingRatio=1)
    ratings.show()

    # Save dataFrame to rating_by_movie
    #writer('rating_by_movie', ratings)

    #ratings.registerTempTable("ratings")

    # I want to get the average rating, and count of the number of ratings for each movie and persist it to cassandra
    #movie_ids = ratings.select("movie_id").distinct()
    #movie_ids.show()

    # create table movie_ratings_time_series ( movie_id int, ts timeuuid, rating float, primary key (movie_id, ts) );

    from pyspark.sql import functions as F
    avg_ratings = ratings.groupBy("movie_id", "timestamp").agg(F.avg(ratings.rating).alias('rating'))
    avg_ratings.show()

    #avg_ratings.write.format("org.apache.spark.sql.cassandra").\
    #            options(table="movie_ratings_time_series", keyspace="training").\
    #            save(mode="append")

    # writer("movie_ratings_time_series", avg_ratings)

    # movie_to_ts = local_sql.sql("select distinct movie_id, ts from ratings")
    # movie_to_ts.registerTempTable("movie_ts")

    # going to join this against itself
    # agg = local_sql.sql("SELECT movie_id, avg(rating) as a, count(rating) as c from ratings group by movie_id")
    # agg.registerTempTable("movie_aggregates")

    # matched = local_sql.sql("select a.movie_id, b.ts, a.a, a.c from movie_aggregates a join movie_ts b on a.movie_id = b.movie_id  ")

    # writer(matched, "movie_stream_ratings")

    print "============== DONE WRITING ============== "


lines.foreachRDD(process_ratings)

stream.start()
stream.awaitTermination()
