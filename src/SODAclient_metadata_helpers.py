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
	return(sum(nullsAndNonNulls))


def CountNulls(cc = pd.DataFrame(), varnamelist = list()):
	for v in varnamelist:
		numNulls = cc.loc[lambda l: l.fieldName == v,'null'].values
		print(v+' has '+ str(numNulls) + ' nulls')
