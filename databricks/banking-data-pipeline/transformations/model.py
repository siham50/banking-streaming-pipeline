from pyspark import pipelines as dp

#This is the source (view)
@dp.view
def dim_customers_view():
  df = spark.readStream.table("OBT_silver")
  df = df.select("customer_id", "customer_name", "customer_email", "customer_phone")
  df = df.dropDuplicates(subset = ["customer_id"])
  return df


#Creating the Dim customers
dp.create_streaming_table("dim_customers")
dp.create_auto_cdc_flow(
  target = "dim_customers",
#whenever we wanna create streaming tables we create streaming views, so we will create a view that selects the columns we wanna have on our customer dimension table (so the view will be the source)
  source = "dim_customers_view",
#we need to deduplicate the table cause we can have a customer making multiple transactions
  keys =["customer_id"],
  sequence_by = "customer_id",
  stored_as_scd_type = 1
)

#Creating the Dim payee
@dp.view
def dim_payee_view():
  df = spark.readStream.table("OBT_silver")
  df = df.select("payee_id", "payee_name", "payee_phone", "payee_tax_id", "payee_risk_score")
  df = df.dropDuplicates(subset = ["payee_id"])
  return df

dp.create_streaming_table("dim_payee")
dp.create_auto_cdc_flow(
  target = "dim_payee",
  source = "dim_payee_view",
  keys =["payee_id"],
  sequence_by = "payee_id",
  stored_as_scd_type = 1
)

#Creating the Dim account
@dp.view
def dim_account_view():
  df = spark.readStream.table("OBT_silver")
  df = df.select("account_id", "card_network_id", "channel_id", "card_tier", "account_number", "account_type", "card_network", "channel")
  df = df.dropDuplicates(subset = ["account_id"])
  return df

dp.create_streaming_table("dim_account")
dp.create_auto_cdc_flow(
  target = "dim_account",
  source = "dim_account_view",
  keys =["account_id"],
  sequence_by = "account_id",
  stored_as_scd_type = 1
)

#Creating the Dim payment
@dp.view
def dim_payment_view():
  df = spark.readStream.table("OBT_silver")
  df = df.select("payment_method_id", "payment_method", "is_card", "requires_auth")
  df = df.dropDuplicates(subset = ["payment_method_id"])
  return df

dp.create_streaming_table("dim_payment")
dp.create_auto_cdc_flow(
  target = "dim_payment",
  source = "dim_payment_view",
  keys =["payment_method_id"],
  sequence_by = "payment_method_id",
  stored_as_scd_type = 1
)


#Creating the Dim transcations
#this is one of the most important dimensions
@dp.view
def dim_transactions_view():
  df = spark.readStream.table("OBT_silver")
  df = df.select("transaction_id", "transaction_reference", "source_location_id", "customer_address", "customer_latitude", "customer_longitude", "destination_location_id", "payee_address", "payee_latitude", "payee_longitude","transaction_status_id", "beneficiary_branch_id", "decline_reason_id", "initiation_timestamp", "settlement_timestamp", "transaction_timestamp")
  df = df.dropDuplicates(subset = ["transaction_id"])
  return df

dp.create_streaming_table("dim_transactions")
dp.create_auto_cdc_flow(
  target = "dim_transactions",
  source = "dim_transactions_view",
  keys =["transaction_id"],
  sequence_by = "transaction_id",
  stored_as_scd_type = 1
)


#Creating the Dim source_location
#here i converted the view into table to monitor the code better and query the table
@dp.table
def dim_sourceLocation_view():
  df = spark.readStream.table("OBT_silver")
  df = df.select("branch_id", "customer_branch", "customer_region", "customer_city", "customer_updated_at")
  df = df.dropDuplicates(subset = ["branch_id", "customer_updated_at"])
  return df
  #we added "customer_updated_at" to subset because it will prevents us from having multiple changes before processing our data

dp.create_streaming_table("dim_sourceLocation")
dp.create_auto_cdc_flow(
  target = "dim_sourceLocation",
  source = "dim_sourceLocation_view",
  keys =["branch_id"],
  sequence_by = "customer_updated_at",
  stored_as_scd_type = 2
)


#Creating the fact table
@dp.view
def fact_view():
  df = spark.readStream.table("OBT_silver")
  df = df.select("transaction_id", "customer_id", "payee_id", "account_id", "payment_method_id", "branch_id", "beneficiary_branch_id", "transaction_status_id", "transaction_amount", "processing_duration", "base_fee", "variable_fee", "processing_fee", "risk_multiplier", "total_amount", "feedback_score", "base_rate", "percentage_fee", "per_minute", "transaction_timestamp"
)
  return df

dp.create_streaming_table("fact")
dp.create_auto_cdc_flow(
  target = "fact",
  source = "fact_view",
  keys =["transaction_id", "customer_id", "payee_id", "account_id", "payment_method_id", "branch_id", "beneficiary_branch_id", "transaction_status_id"],
  sequence_by = "transaction_id",
  stored_as_scd_type = 1
)