from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

#we get the data types schema from bulk_transactions
df = spark.sql("select * from banking.bronze.bulk_transactions")
transactions_schema = df.schema

#we create the SDP streaming table
dp.create_streaming_table("staging_transactions")


#To append bulk table in the SDP streaming table which is staging_transactions 
@dp.append_flow(
  target = "staging_transactions"
)
def transactions_initialLoad():
    df = spark.readStream.table("bulk_transactions")

#transform the column "transaction_timestamp" into a timestamp data type cause it's a String type
    df = df.withColumn("transaction_timestamp", col("transaction_timestamp").cast("timestamp"))
    return df



#To append streaming data table in the SDP streaming table  
@dp.append_flow(
  target = "staging_transactions"
)
def transactions_streams():
    df = spark.readStream.table("transactions_raw")

    df_parsed = df.withColumn("parsed_transactions", from_json(col("transactions"), transactions_schema)).select("parsed_transactions.*")

#transform the column "transaction_timestamp" into a timestamp 
    df_parsed = df_parsed.withColumn("transaction_timestamp", col("transaction_timestamp").cast("timestamp"))

#the returned dataframe is the one that gets appended to "staging_transactions" table
    return df_parsed

#we will add a column "parsed_transactions" to transactions_raw dataframe and this column will have the parsed "transactions" column meaning that in this column we will have data not in a string json format, but in a pure json format with keys referencing the schema keys

#then we added the select("parsed_transactions.*") to get only each key as column from the json in the "parsed_transactions" column 

#So by that we will get the json in "parsed_transactions" column in a dataframe with each key being a column with the data type in the schema "transactions_schema" and the values in json of each record 