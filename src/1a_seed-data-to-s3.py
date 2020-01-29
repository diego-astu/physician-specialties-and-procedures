#!/usr/bin/env python3

import os
#from dotenv import load_dotenv
import pandas as pd
import numpy as np
from sodapy import Socrata
import boto3
from botocore.exceptions import NoCredentialsError
from fastparquet import write
import s3fs
import tabulate
import time

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark import SparkConf
from pyspark.sql import SparkSession

from pyspark.sql.functions import *
from pyspark.sql.functions import col
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.types import LongType

import pyspark.sql.functions as f
import re
from pyspark.sql.functions import broadcast




sc = SparkContext(conf=SparkConf())
spark = SparkSession.builder.getOrCreate()



s3_bucket = 's3a://diego-bucket0/try0/test0/'


#thisfile_path = Path(__file__)
#src_directory = thisfile_path.parent
#sodahelpers = src_directory / 'SODAclient_metadata_helpers.py'
#exec(open(sodahelpers).read())


def ExtractColumnNullCounts(md = pd.DataFrame()):
	"""
	This function takes in a list of dicts that is the output of a Socrata.get_metadata() method
	It outputs a dataframe with schema: fieldName, null, non_null representing counts for each column

	"""	
	df = pd.DataFrame(md['columns'])[['fieldName','cachedContents']]
	df["non_null"] = [int(d.get('non_null')) for d in df["cachedContents"]]
	df["null"] = [int(d.get('null')) for d in df["cachedContents"]]
	df_metadata = pd.DataFrame(df[['fieldName','null','non_null']])
	
	return(df_metadata)


def RowCountFromMetadata(md = pd.DataFrame()):
	nullsAndNonNulls = list(md.loc[0,['null','non_null']])
	total = 0
	for s in nullsAndNonNulls:
		total += s
	return(total)

def CountNulls(cc = pd.DataFrame(), varnamelist = list()):
	for v in varnamelist:
		numNulls = cc.loc[lambda l: l.fieldName == v,'null'].values
		print(v+' has '+ str(numNulls) + ' nulls')




#load_dotenv()
cms_client = Socrata(
	domain = 'data.cms.gov',
	app_token = os.getenv('SOCRATA_API_KEY'),
	username=os.getenv('SOCRATA_USERNAME'),
	password=os.getenv('SOCRATA_PASSWORD'))
medicare_client = Socrata(
	domain = 'data.medicare.gov',
	app_token = os.getenv('SOCRATA_API_KEY'),
	username=os.getenv('SOCRATA_USERNAME'),
	password=os.getenv('SOCRATA_PASSWORD'))


metadata_util = cms_client.get_metadata(dataset_identifier = "fs4p-t5eq", content_type="json")
metadata_phys = medicare_client.get_metadata(dataset_identifier = "j79y-bz9t", content_type="json")
metadata_quality = medicare_client.get_metadata(dataset_identifier = "mj5m-pzi6", content_type="json")



#print(metadata_util)
util_columncounts = ExtractColumnNullCounts(metadata_util)
phys_columncounts = ExtractColumnNullCounts(metadata_phys)
quality_columncounts = ExtractColumnNullCounts(metadata_quality)


print("SEED DATASETS ROW COUNTS, FROM METADATA")
print("Physician Utilization")
print(RowCountFromMetadata(util_columncounts))
print("Physician Quality")
print(RowCountFromMetadata(quality_columncounts))
print("Physician Quality")
print(RowCountFromMetadata(phys_columncounts))



print("starting import of utilization data")
print(time.ctime())
#util_readin = spark.createDataFrame(Row(**x) for x in cms_client.get("fs4p-t5eq", limit=5000)).show(truncate=False)
#RowCountFromMetadata(util_columncounts)))\
util_readin = sc.parallelize(cms_client.get("fs4p-t5eq", limit= RowCountFromMetadata(util_columncounts))).toDF()#RowCountFromMetadata(util_columncounts)))
#util_readin = sc.parallelize(cms_client.get("fs4p-t5eq", limit= 3000))#RowCountFromMetadata(util_columncounts)))
#util_readin = spark.read.json(cms_client.get("fs4p-t5eq", limit= 100))
#util_readin = spark.read.json(sc.parallelize(cms_client.get("fs4p-t5eq", limit= RowCountFromMetadata(util_columncounts))))

print("Imported Utilization Data")
print(time.ctime())
#util = pd.DataFrame.from_records(util_readin).drop_duplicates(subset=['npi', 'hcpcs_code','place_of_service']) #no duplicates were found in sample, but deduplicating for safety
util = util_readin.dropDuplicates(['npi', 'hcpcs_code','place_of_service'])

s3_url = s3_bucket + 'util_seed_sample_whole' + '.parquet'
print("Attempt to write" + s3_url)
print(time.ctime())
#util.to_parquet(s3_url, compression='gzip')
util.write.parquet(s3_url)
print("Exported Utilization Data to S3")
print(time.ctime())