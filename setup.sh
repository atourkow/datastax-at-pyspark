#!/bin/bash
# Download and unzip the Movie lens library
if [ ! -d "ml-10M100K" ]; then
    wget http://files.grouplens.org/datasets/movielens/ml-10m.zip
    unzip ml-10m.zip
    rm ml-10m.zip
fi

# Makr a 100k subset of ratings
if [ ! -f "ml-10M100K/ratings.subset.dat" ]; then
    (cd ml-10M100K;
    head -n 100000 ratings.dat > ratings.subset.dat)
fi

# Install Python Requirements
pip install -r setup/requirements.txt

# Setup Keyspaces and Tables
cqlsh -f setup/setup.cql

# Load Movie Data

