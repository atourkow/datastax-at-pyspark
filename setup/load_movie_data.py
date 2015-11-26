# Load Movies into Cassandra
import time
import sys
import logging
from cassandra.cluster import Cluster

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    filename='load_movie_data.log',
    filemode='w'
)

print "Connecting to Cassandra"
cluster = Cluster(['127.0.0.1']) # It's just on the local machine
session = cluster.connect('at_pyspark')

stmt = session.prepare("INSERT INTO movie (movie_id, name, genre) VALUES (?, ?, ?)")

#sys.exit()

start = time.time()
counter = 0
movie_file = "ml-10M100K/movies.dat"
total = sum(1 for line in open(movie_file))


def handle_success(results):
    print "hi"
    sys.stdout.write("\rTest Wrote {} of {} Movies".format(counter, total))
    sys.stdout.flush()

def handle_failure(reason):
    print str(reason)

print "Loading {} Movies".format(total)
with open(movie_file, 'r') as fp:

    # Send the data
    for line in fp:
        counter += 1
        (movie_id, movie_title, movie_tags) = line.split("::")
        movie_tags = movie_tags.replace("\n", "")
        # TODO - Add lables as a set
        #e.g. session.execute(my_statement, [set(1, 2, 3)])

        '''
        future = session.execute_async(stmt, (int(movie_id), movie_title, movie_tags)) \
                        .add_callback(handle_success) \
                        .add_errback(handle_failure)
        '''

        future = session.execute_async(stmt, (int(movie_id), movie_title, movie_tags))

        try:
            future.result()
        except OperationTimedOut:
            sys.stdout.write(" TIMED OUT: Ugh...")
        
        sys.stdout.write("\rWrote {} of {} Movies".format(counter, total))
        sys.stdout.flush()
'''
while True:
    i = input("Enter text (or Enter to quit): ")
    if not i:
        break
    print("Your input:", i)
'''

total_time = time.time() - start
print "\ntotal time: {}".format(total_time)
print "{}/s".format(float(counter) / total_time)


