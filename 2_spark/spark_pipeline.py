from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark import SparkConf
from pyspark.sql import SparkSession

from pyspark.sql.functions import *
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql.types import LongType

import pyspark.sql.functions as f
import re
from pyspark.sql.functions import broadcast

import pandas


sc = SparkContext(conf=SparkConf().setAppName("dphys"))
spark = SparkSession.builder.appName("dphys").getOrCreate()
#SparkConf().set("spark.jars.packages","org.apache.hadoop:hadoop-aws:3.0.0-alpha3")
#spark = SparkContext.getOrCreate()

df = spark.read.parquet("s3a://diego-bucket0/try0/test0/remote_exploded1_notsubsetted.parquet")
df_providers = df.dropDuplicates(['npi','frst_nm','lst_nm','gndr','med_sch']).select('npi','frst_nm','lst_nm','gndr','med_sch').withColumn('npi',col('npi').cast(LongType()))




df_providers.write.format("jdbc") \
    .option("url", "jdbc:postgresql://ec2-52-21-106-54.compute-1.amazonaws.com/diego_db") \
    .option("dbtable", "mvp_schema.physicians") \
    .option("user", "postgres") \
    .option("password", "") \
    .option("driver","org.postgresql.Driver") \
    .mode("append").save()


    #spark-submit --packages org.postgresql:postgresql:42.2.9 --jars /usr/local/spark/jars/postgresql-42.2.9.jar tp.py
