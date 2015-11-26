drop keyspace if exists at_pyspark;

create KEYSPACE at_pyspark WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
use at_pyspark;


/*  Movie Stuff  */
create table movie (
    movie_id int primary key,
    name text,
    genre text,
    tags set<text>
);

create table rating_by_movie (
    movie_id int,
    user_id int,
    rating decimal,
    timestamp int,
    primary key(movie_id, user_id)
);

create table average_rating (
    movie_id int primary key,
    rating float
);

create table movies_by_tag (
    tag text,
    movie_id int,
    name text,
    primary key(tag, movie_id)
);
