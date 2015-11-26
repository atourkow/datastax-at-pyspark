# PySpark Demo with Search

This is based off of [Jon Haddad's Spark Training](https://github.com/rustyrazorblade/spark-training) in Notebook form.  It's an awesome training guide and I highly recommend using it

This demo is built to work with DataStax Entereprise 4.8+ (relies on Spark 1.3 Dataframes)

### Virtual Env ###
These steps are based on using [virtualenv](http://docs.python-guide.org/en/latest/dev/virtualenvs/). 
It is highly recommended to use virtualenv as it keeps packages separate between apps. 
[Here's a good intro](http://www.dabapps.com/blog/introduction-to-pip-and-virtualenv-python/).  
Install virtualenv
```bash
    sudo pip install virtualenv
```
    
Enter the directory of your application and create the virtualenv in the app directory (the name env is the standard)
```bash
    mkdir /var/www/at_pyspark
    cd /var/www/at_pyspark
    git clone someURL .
    virtualenv env
    source env/bin/activate
        # If you're using fish shell (like I am): 
        source env/bin/activate.fish
```
    
You are now in the virtualenv, your prompt should reflect this, and are ready to install other python packages.  
Type `deactivate` to exit the active virtualenv.


### Setup

* Start DSE in SearchAnalytics mode 
    * `dse cassandra -s -k`
    * Spark Web UI: http://localhost:7080/
    * SOLR Web UI: http://localhost:8983/solr/

Run `./setup.sh`

This will:
* Download and unzip the movie lens dataset.
* Install the Python Requirements
* Create the required keyspace, tables, and load movie data for the exercises

If you want to search the data:
* `dsetool create_core at_pyspark.movie generateResources=true reindex=true`
* Enable <rt>
    * Get the current solrconfig.xml
    *  `curl -o solrconfig.xml http://localhost:8983/solr/at_pyspark.movie/admin/file?file=solrconfig.xml&contentType=text/xml;charset=utf-8`
    *  `dsetool reload_core at_pyspark.movie reindex=true solrconfig=/Users/adamtourkow/ds/demos/at-pyspark/solrconfig.xml`


### Running the demo

* Run the Ratings Generator Feed
    * `ipython 01.ratings_generator.py`
* --- The below is incomplete --- 
* Run the Spark Submit Stream 
    * `dse spark-submit 02.spark_stream.py`


#### TODO
* Add recommendations
* Add Faceted Search
* Add GeoSpatial
