"""
This script takes in seed data from CMS (stored in S3)
Performs clean, merge, explode
Outputs to Parquet
"""

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

from psycopg2 import connect, extensions, sql

from SparkMethods_Diego import DiegoDF


def ProviderData_ReadAndClean():
    """
    Import seed CSVs from Medicare/Medicaid Data, stored in S3
    Standardize columns (convert to lowercase and replace whitespace with underscore)
    Create
    """
    # Physician Compare Dataset Code mj5m-pzi6
    # each line is unique at the clinician/enrollment record/group/address level.
    df_prov0 = DiegoDF(spark.read.csv(s3_path_physician_info, header=True))\
        .StandardizeAllColumns()
    

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

def UtilizationData_ReadAndClean():
    # Medicare provider utilization Dataset code fs4p-t5eq
    df_util0 = DiegoDF(spark.read.csv(s3_path_utilization_data, header=True))\
        .StandardizeAllColumns()\
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

def MergeExplode():

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

    df_prov = ProviderData_ReadAndClean()


    df_util = UtilizationData_ReadAndClean()

    df_tos3 = MergeExplode().cache()
    #####################################################
    ######## WRITE OUT TO PARQUET IN S3 BUCKET ##########
    #####################################################
    df_tos3\
        .write\
        .parquet(out_to_parquet, 
            compression='snappy', 
            mode='overwrite')

