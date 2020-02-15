# MediDash
A dashboard that diplays, for a given state, zip code, or medical practice, counts and percentages of medical procedures performed and drugs deployed, as well as most common medical specialties and medical schools.


# Data Source
 The Center for Medicare and Medicaid Services (CMS) publishes for public use aggregated data on medical procedures and individual data on medical practitioners. I join this data and explode it to simulate event-level data (480GB), which is how it exists in practice but is not publicly available at that granularity.

# Pipeline

 ![Pipeline](https://github.com/diego-astu/physician-specialties-and-procedures/blob/master/images/pipeline_overview.png)

# Requirements

* AWS CLI
* [SPARK standalone cluster](https://github.com/InsightDataScience/pegasus) with 6 EC2 instance workers (EC2 t2.xlarge instances)
* [POSTGRESQL 9.5](https://blog.insightdatascience.com/simply-install-postgresql-58c1e4ebf252) installed on a t2.large EC2 instance
* [Flask app](https://flask.palletsprojects.com/en/1.1.x/quickstart/) hosting a Tableau Story on a t2.micro EC2 instance

# The Spark cluster requires
* Python 3.5.2, with the same subversion installed on all cluster nodes
* The master node must have the following .jar file and environment variables
	* `postgresql-42.2.9.jar`
	* `PYSPARK_DRIVER_PYTHON=python3`
	* `PYSPARK_DRIVER_PYTHON=python3`
	* `export POSTGRES_USER=<user>`
	* `export POSTGRES_PASSWORD=<pw>`
	* `export POSTGRES_IP=<LOCAL_IP_Database_EC2>`
* the `spark-defaults.conf` file (in $SPARK_HOME/conf) must specify the IP address of the master node, following the format specified in `spark-defaults.conf.template`


# Methodology

Stage 1, meant to be run only once, reads seed data from CMS are stored in an Amazon S3 bucket and read in into a child class of Spark DataFrame (DiegoDF). Helper methods written for that class check for duplication and deduplicate/aggregate to the appropriate primary key. Deduplicated seed data are merged and "exploded" to simulate event-level data. For exapmle, a seed data entry indicating 50 procedures, the row is repeated 50 times. This data is read out to S3 in parquet format.

Stage 2, can be extended to add additional features, more datasets, updates as they come in.
Read in and clean auxiliary physician quality data, merge with exploded seed data, aggregate, and output to Postgres

# Helper Methods
* Only StandardizeAllColumns is used in production. Others were used during development for data cleaning and quality checks
* StandardizeAllColumns()
	* Reads in a spark data frame, loops through columns, renames each column to be lower-case and to replace spaces with underscores
* LookForDups():
	* Reads in a Spark dataframe and a list of hypothesized key variables
	* If unique count at key != number of rows, prints out diagnostic information about duplicates
* CountMissings():
	* Loops through columns in Spark dataframe and counts nulls and NAs.
	* Returns a dataframe with that information
* DatasetInfo()
	* Augments Spark's describe() method do include CountMissings() and a count of distinct values
	* In development, this was used while merging to make sure all merging and non-merging rows were accounted for



# Deployment

Stages 1 and 2 of the Spark pipeline can be run via command-line from the master node of the Spark cluster:

	
~~~~
nohup spark-submit \
--executor-memory 5500M \
--total-executor-cores 60 \
--executor-cores 5 \
src/01_seed-to-s3.py &
	
~~~~


	
~~~~
nohup spark-submit \
--executor-memory 5500M \
--total-executor-cores 60 \
--executor-cores 5 \
--packages org.postgresql:postgresql:42.2.9 \
src/02_spark-process-postgres.py &
	
~~~~

The frontend can be deployed via command line. If your python binary is not in /usr/bin, you must replace python with the output from `$which python`
`screen sudo python frontend/flask_app.py`


# Dashboard
The dashboard is accessible at http://www.datifica.me, as long as /frontend/flask_app.py is running.

Alternatively, it is available on [Tableau Public](https://public.tableau.com/profile/diego.astudillo#!/vizhome/Practitioner_Dashboard/Story1)


![Landing Page](https://github.com/diego-astu/physician-specialties-and-procedures/blob/master/images/dashboard_statezip.png)

![Medical Practice Lookup](https://github.com/diego-astu/physician-specialties-and-procedures/blob/master/images/dashboard_practice.png)