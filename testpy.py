#!/usr/bin/env python3



# make sure to install these packages before running:
# pip3 install pandas
# pip3 install sodapy;
# pip3 install numpy;
# pip3 install boto3;
# pip3 install fastparquet;
# pip3 install s3fs;

import pandas as pd
import numpy as np
from sodapy import Socrata
import boto3
from botocore.exceptions import NoCredentialsError
from fastparquet import write
from tabulate import *



# Unauthenticated client only works with public data sets. Note 'None'
# in place of application token, and no username or password:
medicare_client = Socrata("data.medicare.gov", None)
cms_client = Socrata("data.cms.gov", None)


####################################################################
## TODO: CONVERT TO SPARK AND READ IN ENTIRE DATASET
## WILL HAVE TO AUTHENTICATE TO PREVENT THROTTLING
####################################################################
## # MyAppToken = 'FakeCMSToken'
# FakePW = '235536'
# medicare_client = Socrata("data.medicare.gov",
#                  MyAppToken,
#                  username="jdiego@umich.edu",
#                  password="FakePW")
# cms_client = Socrata("data.cms.gov",
#                  MyAppToken,
#                  username="jdiego@umich.edu",
#                  password="FakePW")



###read in raw data (lists of dicts) via get method of SODA API
#The Physician Information dataset is available at
#https://data.medicare.gov/Physician-Compare/Physician-Compare-National-Downloadable-File/mj5m-pzi6
util_results = cms_client.get("fs4p-t5eq", limit=2*1000)
#The Physician Utilization dataset is available at
#https://dev.socrata.com/foundry/data.cms.gov/fs4p-t5eq
phys_results = medicare_client.get("mj5m-pzi6", limit = 2*1000)



# Convert to pandas DataFrame AND MERGE
phys = pd.DataFrame.from_records(phys_results)
util = pd.DataFrame.from_records(util_results)
merged_df = pd.merge(phys,util,on="npi")

#list_of_counts = [i * 1 for i in  list(merged_df['line_srvc_cnt'].tolist())]

#exploded_df = pd.DataFrame(np.repeat(merged_df.values,list_of_counts,axis=0))

#exploded_df = pd.DataFrame(merged_df.values.repeat(merged_df.line_srvc_cnt, axis=0), columns=merged_df.columns)
exploded_df = merged_df.reindex(merged_df.index.repeat(merged_df.line_srvc_cnt))



print(tabulate(exploded_df.head(),headers='keys'))
