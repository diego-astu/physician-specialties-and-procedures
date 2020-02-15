>>>>>>> Stashed changes
import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark import SparkConf
from pyspark.sql import SparkSession

from pyspark.sql.functions import *
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql.types import LongType

import pyspark.sql.functions as f
from pyspark.sql.functions import broadcast

import numpy as np
import re

import boto3
from botocore.exceptions import NoCredentialsError
from fastparquet import write
import s3fs
import tabulate
import time

<<<<<<< Updated upstream
=======
<<<<<<< Updated upstream
>>>>>>> Stashed changes
s3_bucket = 's3a://dphys-data/'


#####################################################
###### CREATE SPARK SESSION AND SPARK CONTEXT #######
#####################################################
# add a description here about what this is doing

sc = SparkContext(conf=SparkConf())
spark = SparkSession.builder.getOrCreate()

spark.conf.set("spark.executor.memory", "6g")
spark.conf.set("spark.driver.memory", "6g")
spark.conf.set("spark.cores.max", "6")


#####################################################
###### IMPORT SEED CSVs FROM AMAZON S3 BUCKET #######
#####################################################
# phys_util = spark.read.parquet(s3_bucket + 'seed-data/' + '' + 'parquet')
# seed_util = spark.read.parquet(s3_bucket + 'seed-data/' + '' + 'parquet')
# qual_util = spark.read.parquet(s3_bucket + 'seed-data/' + '' + 'parquet')

df_prov_in = spark.read.csv(s3_bucket + 'seed-data/' + 'Physician_Compare_National_Downloadable_File' + '.csv', header=True)
df_util_in = spark.read.csv(s3_bucket + 'seed-data/' + 'Medicare_Provider_Utilization_and_Payment_Data__Physician_and_Other_Supplier_PUF_CY2017' + '.csv', header=True)
df_qual_in = spark.read.csv(s3_bucket + 'seed-data/' + 'Physician_Compare_2017_Individual_EC_Public_Reporting_-_Overall_MIPS_Performance' + '.csv', header=True)

def standarize_all_columns(spark_df):
    std_data = spark_df
    for c in spark_df.columns:
        std_data = std_data.withColumnRenamed(c,c.replace(" ", "_").lower())
    return(std_data)

df_prov0 = standarize_all_columns(df_prov_in)
df_util0 = standarize_all_columns(df_util_in).withColumnRenamed('national_provider_identifier','npi')
df_qual0 = standarize_all_columns(df_qual_in)

#####################################################
########  SHOW FILE SCHEMAS, FOR REFERENCE  #########
#####################################################
# df_prov0.printSchema()
# df_util0.printSchema()
# df_qual0.printSchema()

keep_prov_vars = ['npi', 'pac_id', 'professional_enrollment_id', 
'last_name', 'first_name', 'middle_name', 'suffix', 
'gender', 'credential', 'medical_school_name', 'graduation_year', 
'primary_specialty', 'secondary_specialty_1', 'secondary_specialty_2', 'secondary_specialty_3', 'secondary_specialty_4', 'all_secondary_specialties', 
'organization_legal_name', 'group_practice_pac_id', 'number_of_group_practice_members', 
'line_1_street_address', 'line_2_street_address', 'marker_of_address_line_2_suppression', 
'city', 'state', 'zip_code', 'phone_number', 
'hospital_affiliation_ccn_1', 'hospital_affiliation_lbn_1', 'hospital_affiliation_ccn_2', 'hospital_affiliation_lbn_2', 
'hospital_affiliation_ccn_3', 'hospital_affiliation_lbn_3', 'hospital_affiliation_ccn_4', 'hospital_affiliation_lbn_4', 
'hospital_affiliation_ccn_5', 'hospital_affiliation_lbn_5', 'professional_accepts_medicare_assignment']

# ... df_prov0.printSchema()
# root
#  |-- npi: string (nullable = true)
#  |-- pac_id: string (nullable = true)
#  |-- professional_enrollment_id: string (nullable = true)
#  |-- last_name: string (nullable = true)
#  |-- first_name: string (nullable = true)
#  |-- middle_name: string (nullable = true)
#  |-- suffix: string (nullable = true)
#  |-- gender: string (nullable = true)
#  |-- credential: string (nullable = true)
#  |-- medical_school_name: string (nullable = true)
#  |-- graduation_year: string (nullable = true)
#  |-- primary_specialty: string (nullable = true)
#  |-- secondary_specialty_1: string (nullable = true)
#  |-- secondary_specialty_2: string (nullable = true)
#  |-- secondary_specialty_3: string (nullable = true)
#  |-- secondary_specialty_4: string (nullable = true)
#  |-- all_secondary_specialties: string (nullable = true)
#  |-- organization_legal_name: string (nullable = true)
#  |-- group_practice_pac_id: string (nullable = true)
#  |-- number_of_group_practice_members: string (nullable = true)
#  |-- line_1_street_address: string (nullable = true)
#  |-- line_2_street_address: string (nullable = true)
#  |-- marker_of_address_line_2_suppression: string (nullable = true)
#  |-- city: string (nullable = true)
#  |-- state: string (nullable = true)
#  |-- zip_code: string (nullable = true)
#  |-- phone_number: string (nullable = true)
#  |-- hospital_affiliation_ccn_1: string (nullable = true)
#  |-- hospital_affiliation_lbn_1: string (nullable = true)
#  |-- hospital_affiliation_ccn_2: string (nullable = true)
#  |-- hospital_affiliation_lbn_2: string (nullable = true)
#  |-- hospital_affiliation_ccn_3: string (nullable = true)
#  |-- hospital_affiliation_lbn_3: string (nullable = true)
#  |-- hospital_affiliation_ccn_4: string (nullable = true)
#  |-- hospital_affiliation_lbn_4: string (nullable = true)
#  |-- hospital_affiliation_ccn_5: string (nullable = true)
#  |-- hospital_affiliation_lbn_5: string (nullable = true)
#  |-- professional_accepts_medicare_assignment: string (nullable = true)

keep_util_vars = ['npi',  'place_of_service', 
'hcpcs_code', 'hcpcs_description', 'hcpcs_drug_indicator',
 'number_of_services', 'number_of_medicare_beneficiaries', 
 'average_medicare_allowed_amount', 'average_submitted_charge_amount', 
 'average_medicare_payment_amount', 'average_medicare_standardized_amount']
# >>> df_util0.printSchema()
# root
#  |-- national_provider_identifier: string (nullable = true)
#  |-- last_name/organization_name_of_the_provider: string (nullable = true)
#  |-- first_name_of_the_provider: string (nullable = true)
#  |-- middle_initial_of_the_provider: string (nullable = true)
#  |-- credentials_of_the_provider: string (nullable = true)
#  |-- gender_of_the_provider: string (nullable = true)
#  |-- entity_type_of_the_provider: string (nullable = true)
#  |-- street_address_1_of_the_provider: string (nullable = true)
#  |-- street_address_2_of_the_provider: string (nullable = true)
#  |-- city_of_the_provider: string (nullable = true)
#  |-- zip_code_of_the_provider: string (nullable = true)
#  |-- state_code_of_the_provider: string (nullable = true)
#  |-- country_code_of_the_provider: string (nullable = true)
#  |-- provider_type: string (nullable = true)
#  |-- medicare_participation_indicator: string (nullable = true)
#  |-- place_of_service: string (nullable = true)
#  |-- hcpcs_code: string (nullable = true)
#  |-- hcpcs_description: string (nullable = true)
#  |-- hcpcs_drug_indicator: string (nullable = true)
#  |-- number_of_services: string (nullable = true)
#  |-- number_of_medicare_beneficiaries: string (nullable = true)
#  |-- number_of_distinct_medicare_beneficiary/per_day_services: string (nullable = true)
#  |-- average_medicare_allowed_amount: string (nullable = true)
#  |-- average_submitted_charge_amount: string (nullable = true)
#  |-- average_medicare_payment_amount: string (nullable = true)
#  |-- average_medicare_standardized_amount: string (nullable = true)
keep_qual_vars = ['npi', 'final_mips_score']
# >>> df_qual0.printSchema()
# root
#  |-- npi: string (nullable = true)
#  |-- pac_id: string (nullable = true)
#  |-- last_name: string (nullable = true)
#  |-- first_name: string (nullable = true)
#  |-- source_of_scores: string (nullable = true)
#  |-- quality_category_score: string (nullable = true)
#  |-- aci_category_score: string (nullable = true)
#  |-- ia_category_score: string (nullable = true)
#  |-- final_mips_score: string (nullable = true)

#####################################################
###### CONFIRM UNIQUE KEYS INVESTIGATE DUPES ########
#####################################################
# Expected Unique Identifiers according to data documentation
prov_key_list = ['npi','group_practice_pac_id']
util_key_list = ['npi', 'hcpcs_code', 'place_of_service']
qual_key_list = ['npi']


def LookForDups(spark_df, expected_unique_key):
    ##Determine if hypothesized unique key is unique
    unique_bool = spark_df.select(expected_unique_key).distinct().count() == spark_df.count()
    if (unique_bool):
        print("Unique Key Found")
        print(expected_unique_key)
        return('')
        
    else:
        print("Duplication found")
        print("Printing first 20 duplicate cases, descending order...")
        print(spark_df.groupBy(expected_unique_key).count().filter('count>1').sort(desc("count")).show(20));
        return('')

print("Look for Duplicates")
#Look for dups
# LookForDups(df_prov0,prov_key_list)
#df_prov0.filter('npi == 1326534264 and group_practice_pac_id == 8820360795').show(vertical=True)
#there is substantial duplication due to multiple addresses for the same org_pac_id
#Will hard-deduplicate, even though organization PAC ID will have one of possibly many addresses

# LookForDups(df_util0,util_key_list)
#None found!

# LookForDups(df_qual0, qual_key_list)
# df_qual0.filter('npi == 1568411072').show()
# df_qual0.filter('npi == 1912058561').show()
# Conclusion: This duplication is in error, not due to additional keys... Hard-deduplicate

#####################################
###### DEDUPLICATE SEED DATA ########
#####################################

# df = spark.read.parquet("s3a://diego-bucket0/try0/test0/remote_exploded1_notsubsetted.parquet")
df_prov = df_prov0.dropDuplicates(prov_key_list).select(keep_prov_vars)
df_util = df_util0.dropDuplicates(util_key_list).select(keep_util_vars)
df_qual = df_qual0.dropDuplicates(qual_key_list).select(keep_qual_vars)


print("Confirm that deduplication worked")
# LookForDups(df_prov,prov_key_list)
# LookForDups(df_util,util_key_list)
# LookForDups(df_qual,qual_key_list)

###############################
###### MERGE SEED DATA ########
###############################

#merge
#COUNTS
#df_prov has 1,297,037 rows, 38 columns
#df_util has 9,847,443 rows, 26 columns


phys_util_join = df_prov.join(df_util, ['npi'], how = 'inner')
#phys_util_join has 10,958,107 rows, 
merged_df = phys_util_join.join(df_qual, on = ['npi'], how = 'left')
#merged_df has 10,958,107 rows

#################################
###### EXPLODE SEED DATA ########
#################################

def duplicate_function(row):
    data = []  # list of rows to return
    to_duplicate = float(row["number_of_services"])
    i = 0
    while i < to_duplicate:
        row_dict = row.asDict()  # convert a Spark Row object to a Python dictionary
        row_dict["SERIAL_NO"] = str(i)
        new_row = pyspark.sql.Row(**row_dict)  # create a Spark Row object based on a Python dictionary
        data.append(new_row)  # adds this Row to the list
        i += 1
    return(data)  # returns the final list


print("Flatmap")
df_flatmap = merged_df.rdd.flatMap(duplicate_function).toDF(merged_df.schema)
df_flatmap.persist()
#df_flatmap.show(100)
#df_flatmap.count()
#####################################################
###### WRITE OUT TO CSV IN AMAZON S3 BUCKET #########
#####################################################
<<<<<<< Updated upstream
=======
=======
from psycopg2 import connect, extensions, sql

import SparkMethods_Diego


def UtilizationData_ReadAndClean():
    # Medicare provider utilization Dataset code fs4p-t5eq
    df_util0 = DiegoDF(spark.read.csv(s3_path_utilization_data, header=True))\
        .standarize_all_columns()\
        .withColumnRenamed('national_provider_identifier','npi')
    
    keep_util_vars = ['npi',  'place_of_service', 
    'hcpcs_code', 'hcpcs_description', 'hcpcs_drug_indicator',
     'number_of_services', 'number_of_medicare_beneficiaries']

    #####################################################
    # Unique Identifiers according to data documentation
    # were npi*hcpcs_code*location(inpatient/outpatient)
    #####################################################
    # Development versions of this code showed 
    # based on the LookForDups method that
    # The provider data was unique at the specified pkey
    # However, I do not care about inpatient/outpatient distinction
    # Will sum across desired pkey
    df_util0.createOrReplaceTempView("util")
    df_util_out = spark.sql(""" 
    select -- keys
    npi, 
    hcpcs_code,

    --variables
    max(hcpcs_description) as hcpcs_description, 
    max(hcpcs_drug_indicator) as hcpcs_drug_indicator,
    sum(number_of_services) as number_of_services,
    sum(number_of_medicare_beneficiaries) as number_of_medicare_beneficiaries

    from util 

    group by 1,2
     """)

    return(df_util_out)



def ProviderData_ReadAndClean():
    """
    Import seed CSVs from Medicare/Medicaid Data, stored in S3
    Standardize columns (convert to lowercase and replace whitespace with underscore)
    Create
    """
    # Physician Compare Dataset Code mj5m-pzi6
    # each line is unique at the clinician/enrollment record/group/address level.
    df_prov0 = DiegoDF(spark.read.csv(s3_path_physician_info, header=True))\
        .standarize_all_columns()
    

    keep_prov_vars = ['npi', 'professional_enrollment_id', 
    #Doctor personal info
    'last_name', 'first_name', 'middle_name', 'suffix', 
    'gender', 'credential', 'medical_school_name', 'graduation_year', 
    #Specialty info
    'primary_specialty', 'secondary_specialty_1', 'secondary_specialty_2', 'secondary_specialty_3', 'secondary_specialty_4', 'all_secondary_specialties', 
    #Practice Affiliation
    'organization_legal_name', 'group_practice_pac_id', 'number_of_group_practice_members', 
    'line_1_street_address', 'line_2_street_address', #'marker_of_address_line_2_suppression', 
    'city', 'state', 'zip_code', 'phone_number', 
    #Hospital Affiliation
    'hospital_affiliation_ccn_1', 'hospital_affiliation_lbn_1', 'hospital_affiliation_ccn_2', 'hospital_affiliation_lbn_2', 
    'hospital_affiliation_ccn_3', 'hospital_affiliation_lbn_3', 'hospital_affiliation_ccn_4', 'hospital_affiliation_lbn_4', 
    'hospital_affiliation_ccn_5', 'hospital_affiliation_lbn_5']

    
    #####################################################
    # Expected primary key according to documentation was:
    #####################################################
    # prov_key_list = ['npi','group_practice_pac_id',
    # 'line_1_street_address', 'line_2_street_address','phone_number',
    # 'city', 'state', 'zip_code']
    #####################################################
    # However, development versions of this code showed 
    # based on the LookForDups method that
    # The provider data was NOT unique at the specified pkey
    # The source of non-uniqueness was duplicate records
    # at the address level
    # There were two kinds of duplication:
    # # 1) One entry with a phone number, one entry w/o
    # # 2) One entry with a suite #, one entry w/o
    #### SOLUTION
    # # Group by expected primary key, taking max() of every other variable

    df_prov00.createOrReplaceTempView("prov")
    df_prov_out = spark.sql(""" 
    select -- keys
    npi, 
    group_practice_pac_id, 
    line_1_street_address, 
    city, 
    state, 
    zip_code,

    --variables
    max(last_name) as last_name, 
    max(first_name) as first_name, 
    max(middle_name) as middle_name, 
    max(suffix) as suffix, 
    max(gender) as gender, 
    max(credential) as credential,
    max(medical_school_name) as medical_school_name, 
    max(graduation_year) as graduation_year, 
    max(primary_specialty) as primary_specialty, 
    max(secondary_specialty_1) as secondary_specialty_1, 
    max(secondary_specialty_2) as secondary_specialty_2, 
    max(secondary_specialty_3) as secondary_specialty_3, 
    max(secondary_specialty_4) as secondary_specialty_4, 
    max(organization_legal_name) as organization_legal_name, 
    max(number_of_group_practice_members) as number_of_group_practice_members, 
    max(line_2_street_address) as line_2_street_address, 
    max(phone_number) as phone_number,
    max(hospital_affiliation_ccn_1) as hospital_affiliation_ccn_1, 
    max(hospital_affiliation_ccn_2) as hospital_affiliation_ccn_2, 
    max(hospital_affiliation_ccn_3) as hospital_affiliation_ccn_3, 
    max(hospital_affiliation_ccn_4) as hospital_affiliation_ccn_4, 
    max(hospital_affiliation_ccn_5) as hospital_affiliation_ccn_5,
    max(hospital_affiliation_lbn_1) as hospital_affiliation_lbn_1, 
    max(hospital_affiliation_lbn_2) as hospital_affiliation_lbn_2, 
    max(hospital_affiliation_lbn_3) as hospital_affiliation_lbn_3, 
    max(hospital_affiliation_lbn_4) as hospital_affiliation_lbn_4, 
    max(hospital_affiliation_lbn_5) as hospital_affiliation_lbn_5

    from prov 

    group by npi, group_practice_pac_id, 
    line_1_street_address, city, state, zip_code
     """)

    return(df_prov_out)


def MergeExplodeParquet():

    #Left merge, and impute some non-merging values as zeros
    merged_df = df_prov.join(df_util, 'npi','left').fillna(
        {
         'hcpcs_code': 'none',
         'hcpcs_description' : 'none',
         'hcpcs_drug_indicator': 'none',
         'number_of_services': '0',
         'number_of_medicare_beneficiaries': '0'
         })

    def duplicate_function(row):
    """
    This function is applied via flatMap to an RDD
    It creates N duplicates of that row and returns an exploded RDD
    N = the value set in to_duplicate, because if a procedure is performed N times,
    I nee to return N rows

    Returns a list of Spark RDD Rows
    """
    # list of rows to return
    data = []  
    to_duplicate = float(row["number_of_services"])
    i = 0
    while i < to_duplicate:
        # convert a Spark Row object to a Python dictionary
        row_dict = row.asDict()  
        #assign it an ID number (temporary)
        row_dict["SERIAL_NO"] = str(i)
        # create a Spark Row object based on a Python dictionary
        new_row = pyspark.sql.Row(**row_dict)  
        # adds this Row to the list
        data.append(new_row)  
        i += 1
    # returns the final list
    return(data)  


    return(merged_df.rdd.flatMap(duplicate_function).toDF(merged_df.schema))
    








if __name__ == '__main__':

    # Initialize Spark Session   
    sc = SparkContext(conf=SparkConf())
    spark = SparkSession.builder.appName("seed-to-s3")\
        .getOrCreate()


    s3_bucket = 's3a://dphys-data/'
    s3_path_physician_info = s3_bucket + 'seed-data/' + 'Physician_Compare_National_Downloadable_File' + '.csv'
    s3_path_utilization_data = s3_bucket + 'seed-data/' + 'Medicare_Provider_Utilization_and_Payment_Data__Physician_and_Other_Supplier_PUF_CY2017' + '.csv'
    out_to_parquet = s3_bucket + 'seed-data/exploded-data/' + 'feb11_exploded_seed_data' 


    df_util = UtilizationData_ReadAndClean()

    df_prov = ProviderData_ReadAndClean()

    df_tos3 = MergeExplodeParquet().cache()
    #####################################################
    ######## WRITE OUT TO PARQUET IN S3 BUCKET ##########
    #####################################################
    df_tos3\
        .write\
        .parquet(out_to_parquet, 
            partitionBy = 'zip_code',
            compression='snappy', 
            mode='overwrite')


>>>>>>> Stashed changes
>>>>>>> Stashed changes


#merged_df.write.parquet(s3_bucket + 'exploded-data/' + 'test_exploded_seed_data' , compression='snappy', mode = 'overwrite')
df_flatmap.write.parquet(s3_bucket + 'seed-data/' + 'exploded_seed_data' , compression='snappy', mode='overwrite')
