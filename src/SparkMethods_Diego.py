"""This script contains helper functions, mostly custom methods for Spark DFs
Only one of these will be used in production (standardize_all_columns)
The others were quality checks implemented during pipeline processing
"""
from pyspark.sql import DataFrame

## Create a custom class that inherits from Spark DataFrame
class DiegoDF(DataFrame):
    def __init__(self,df):
      super().__init__(df._jdf, df.sql_ctx)
      self._df = df
    def LookForDups(self, expected_unique_key):
        """
        Determine if hypothesized unique key is unique

        Use this function when reading in a new schema to verify its primary key

        1) Check with expected_unique_key = self.columns:
           If dups found, a primary key does not exist
        2) Check with expected primary key, or any other key
           Make sure you understand primary of data before working with any dataset
        """
        #A key is unique if count at key == number of rows in dataframe
        is_unique = self.select(expected_unique_key)\
            .distinct().count() == self.count()
        if (is_unique):
            print("Unique Key Found")
            print(expected_unique_key)
        else:
            print("Duplication found")
            # First order duplication check is to make sure that a primary key exists
            # In that case, expected_unique_key = self.columns
            if (expected_unique_key == self.columns):
                print("There is no primary key")
                print("Some rows have all columns with same value")
            # Print out diagnostics to identify extent of duplication
            count_by_key = self.groupBy(expected_unique_key).count().persist()
            total_row_count = count_by_key\
                .select(sum('count')).collect()[0][0]
            nonunique_row_count = count_by_key\
                .filter('count>1')\
                .select(sum('count')).collect()[0][0]
            unique_row_values = count_by_key.count()
            print("=================")
            print(nonunique_row_count,"non-unique rows out of",total_row_count)
            percent_duplicates = 100.0 * float(nonunique_row_count) / float(total_row_count)
            print("{0:.1f}% of rows are non-unique".format(percent_duplicates))
            print(unique_row_values,"unique values at key",expected_unique_key)
            # Print out non-unique IDs to manually determine reasons for duplication
            if (expected_unique_key != self.columns):
                print("Printing duplicate cases, descending order...")
                print(count_by_key.filter('count>1').sort(desc("count")).show(20))
    
    def StandardizeAllColumns(self):
        """
        Column names often have spaces and capitalization
        Replace spaces with underscore, convert all to lowercase
        """
        std_data = self
        for c in self.columns:
            std_data = std_data.withColumnRenamed(c,c.replace(" ", "_").lower())
        return(std_data)
    #
    def CountMissings(self,sort=False):
        """
        Counts number of nulls and nans in each column
        """
        df = self.select([F.count(F.when(F.isnan(c) | F.isnull(c), c)).alias(c) for (c,c_type) in self.dtypes if c_type not in ('timestamp', 'date')])
        if sort:
            return(df.rename(index={0: 'count'}).T.sort_values("count",ascending=False))
        return(df)
    #
    def DatasetInfo(self, varlist_uv = [None]):
        """
        This method supplements Spark DataFrame's describe() method by adding distinct count & missing count
        INPUTS: Spark dataframe, character list of column names
        OUTPUTS: Spark dataframe with :
        Number of rows
        Number of distinct values
        Number of missing values
        """
        intab = self
        if varlist_uv == [None]:
            varlist_uv = list(intab.columns)
        default_describe = intab.select(*varlist_uv)\
            .describe().filter("summary in ('min','max', 'count')")
        cnt_distincts = intab.agg(*
            (countDistinct(col(c)).alias(c) for c in varlist_uv)
            )\
            .withColumn('summary',lit('count_distinct'))
        missings_cnt = CountMissings(intab.select(*varlist_uv))\
            .withColumn('summary',lit('missing_cnt'))
        out_summary = default_describe.\
            unionByName(cnt_distincts)\
            .unionByName(missings_cnt)
        print("SUMMARY INFO")
        out_summary.show(vertical=True)
        return(out_summary)


