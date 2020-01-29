#!/usr/bin/env python3



# make sure to install these packages before running:
# pip3 install pandas
# pip3 install sodapy;
# pip3 install numpy;
# pip3 install boto3;
# pip3 install fastparquet;
# pip3 install s3fs;
#pip install python-dotenv
import os
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from sodapy import Socrata
import boto3
from botocore.exceptions import NoCredentialsError
from fastparquet import write
import tabulate
from sodapy import Socrata




load_dotenv()
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



print("Expected row counts incoming")
print(RowCountFromMetadata(util_columncounts))
print(RowCountFromMetadata(quality_columncounts))
print(RowCountFromMetadata(phys_columncounts))
###read in raw data (lists of dicts) via get method of SODA API
#The Physician Information dataset is available at
#https://data.medicare.gov/Physician-Compare/Physician-Compare-National-Downloadable-File/mj5m-pzi6

util_readin = cms_client.get("fs4p-t5eq", limit=RowCountFromMetadata(util_columncounts))
print("Imported Utilization Data")


quality_score = medicare_client.get("j79y-bz9t", limit = RowCountFromMetadata(quality_columncounts))
print("Imported Quality Data")


phys_readin = medicare_client.get("mj5m-pzi6", limit = RowCountFromMetadata(phys_columncounts))
print("Imported Utilization Data")



keep_util_columns = [
'npi',
'hcpcs_code', #medical service code
'hcpcs_description', #service description
'place_of_service',
'line_srvc_cnt',
]
keep_qual_columns = [
'npi',
'final_mips_score']
keep_phys_columns = [
'npi', #Unique professional ID assigned by NPPES
'org_pac_id', #private medical practice ID
'frst_nm',
'mid_nm',
'lst_nm',
'med_sch',
'grd_yr',
'cred', #Medical Credential (eg MD, DO, DPM)
'pri_spec', #Primary Specialty
'sec_spec_1',
'sec_spec_2',
'sec_spec_3',
'sec_spec_4',

'hosp_afl_1', #Primary Hospital ID (CCN number)
'hosp_lbn_1', #Primary hospital Name
'hosp_afl_2',
'hosp_lbn_2',

'hosp_afl_3',
'hosp_lbn_3',
'hosp_afl_4',
'hosp_lbn_4',
'hosp_afl_5',
'hosp_lbn_5',
]

pd.options.display.max_columns = None
# Convert to pandas DataFrame
phys = pd.DataFrame.from_records(phys_readin).drop_duplicates(subset=['npi','org_pac_id']) #there is some duplication due to multiple addresses for the same org_pac_id

qual = pd.DataFrame.from_records(quality_score).drop_duplicates(subset=['npi']) #a small number of exact duplicates were found
#print(qual.groupby(["npi"]).size().to_frame().T)
print("Finished Reading In")

#check unique IDs
print(phys.set_index(['npi','org_pac_id']).index.is_unique)
print(util.set_index(['npi', 'hcpcs_code','place_of_service']).index.is_unique)
print(qual.set_index(['npi']).index.is_unique)

#merge
merged_df0 = pd.merge(phys,util,on="npi",how='inner')
merged_df = pd.merge(merged_df0,qual,how='left',on='npi')

#CHECK ROW AND COLUMN COUNTS
CountNulls(cc = util_columncounts, varnamelist = ['npi','nppes_provider_street2'])

print(phys.shape)
print(util.shape)
print(qual.shape)
print(merged_df0.shape)
print(merged_df.shape)

print(merged_df.set_index(["npi", "org_pac_id","hcpcs_code", "place_of_service"]).index.is_unique)

# print(merged_df.groupby(["npi", "org_pac_id","hcpcs_code", "place_of_service"]).size().to_frame().T)
# print(merged_df.query("npi=='1003013129'"))

#list_of_counts = [i * 1 for i in  list(merged_df['line_srvc_cnt'].tolist())]

# #exploded_df = pd.DataFrame(np.repeat(merged_df.values,list_of_counts,axis=0))

# #exploded_df = pd.DataFrame(merged_df.values.repeat(merged_df.line_srvc_cnt, axis=0), columns=merged_df.columns)
# exploded_df = merged_df.reindex(merged_df.index.repeat(merged_df.line_srvc_cnt))

# exploded_df.to_parquet(s3_url, compression='gzip')
# print("Finished")
