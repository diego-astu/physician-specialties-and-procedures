
"""
This script reads in simulated medical events,
Merges it with physician quality data
Aggregates it
Outputs to Postgres
"""
import boto3
from botocore.exceptions import NoCredentialsError
from fastparquet import write

import numpy as np

import os 

from psycopg2 import connect, extensions, sql

import pyspark
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.functions import *
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql.types import LongType

import pyspark.sql.functions as F
from pyspark.sql.functions import broadcast

import re
import s3fs
import time

from SparkMethods_Diego import DiegoDF, duplicate_function

def ReadInExtraDataCleanMerge():
    """
    Read in exploded data simulated from CMS seeds 
    """
    exploded_df_in = DiegoDF(spark.read.parquet(s3_exploded_data_in))\
        .withColumnRenamed('organization_legal_name','private_practice_name')
    ##################################################
    # Read in  Physician quality info                #
    ##################################################
    #Primary key NPI = provider ID
    qual_key_list = ['npi']
    #Convert columns to lowercase and whitespace to underscore
    df_qual0 = DiegoDF(spark.read.csv(s3_path_physician_quality, header=True))\
        .StandardizeAllColumns()

    ##################################################
    # Clean Physician quality info                   #
    ##################################################
    # Development versions of this code showed via LookForDups method that
    # 16% of cases were duplicates (same values in each column)
    # Drop duplciates based on expected primary key
    # Keep only Provider ID and quality score
    df_qual = df_qual0\
        .dropDuplicates(qual_key_list)\
        .select(['npi', 'final_mips_score'])

    # join exploded data (large)
    # with Physician quality data (small)
    exploded_data_with_quality = exploded_df_in\
        .join(df_qual,how='left',on=['npi'])
    return(exploded_data_with_quality)

def CalculateProcedureCounts(df_in):
    # Register the DataFrame as a SQL temporary view
    df_in.createOrReplaceTempView("visits")

    df_out = spark.sql("""


    SELECT npi,group_practice_pac_id,hcpcs_code, -- Primary Key

    -- Physician level auxiliary attributes that could be normalized
    medical_school_name, graduation_year, primary_specialty, 
    secondary_specialty_1, secondary_specialty_2, secondary_specialty_3, secondary_specialty_4, all_secondary_specialties, 
    --final_mips_score
    hospital_affiliation_ccn_1, hospital_affiliation_ccn_2, 
    hospital_affiliation_ccn_3, hospital_affiliation_ccn_4, hospital_affiliation_ccn_5,

    hospital_affiliation_lbn_1, hospital_affiliation_lbn_2, 
    hospital_affiliation_lbn_3, hospital_affiliation_lbn_4, hospital_affiliation_lbn_5,

    -- Practice level auxiliary attributes that could be normalized
    private_practice_name, number_of_group_practice_members, line_1_street_address, line_2_street_address, city, state, substr(zip_code,1,5) as zip_code, phone_number, 

    --Procedure level attributes that could be normalized
    hcpcs_description, hcpcs_drug_indicator, 

    -- Aggregation
    count(number_of_services) as number_procedures
     FROM visits

    
     Group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 
     13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
     27,28,29,30,31


     """)

    return(df_out)

def WriteToPostgres(spark_df, 
    PostgresIP, 
    DB_name,
    Namespace,
    Table,
    PG_USER = os.environ["POSTGRES_USER"],
    PG_PW = os.environ["POSTGRES_PASSWORD"]):
    spark_df.write.format("jdbc") \
        .option("url", "jdbc:postgresql://"+PostgresIP+":5432/"+DB_name) \
        .option("dbtable", Namespace+"."+Table) \
        .option("user", PG_USER) \
        .option("password", PG_PW) \
        .option("driver","org.postgresql.Driver") \
        .mode("overwrite").save()


if __name__ == '__main__':
    
    # Initialize Spark Session 
    sc = SparkContext(conf=SparkConf())
    spark = SparkSession.builder.appName("s3-to-postgres")\
        .getOrCreate()

    s3_bucket = 's3a://dphys-data/'
    s3_path_physician_quality = s3_bucket + 'seed-data/' + 'Physician_Compare_2017_Individual_EC_Public_Reporting_-_Overall_MIPS_Performance' + '.csv'
    s3_exploded_data_in = s3_bucket + 'seed-data/exploded-data/feb2_exploded_seed_data'

    exploded_data_with_quality = ReadInExtraDataCleanMerge()

    DF_forPostgres = CalculateProcedureCounts(exploded_data_with_quality).persist()

    WriteToPostgres(spark_df = DF_forPostgres, 
        PostgresIP = os.environ["POSTGRES_IP"],
        DB_name = "test_db",
        Namespace = "denorm_schema",
        Table = "summary",
        PG_USER = os.environ["POSTGRES_USER"],
        PG_PW = os.environ["POSTGRES_PASSWORD"])
