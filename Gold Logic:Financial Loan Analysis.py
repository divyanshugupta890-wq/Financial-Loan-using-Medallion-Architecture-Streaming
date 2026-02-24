# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ALWAYS REMEMBER BEFORE CREATING GOLD LAYER
# MAGIC 1)Read Silver as stream
# MAGIC
# MAGIC 2)Aggregate micro-batch
# MAGIC
# MAGIC 3)Merge into Gold

# COMMAND ----------

# MAGIC %md
# MAGIC featureLoanTrx

# COMMAND ----------

from pyspark.sql.functions import col, expr
from delta.tables import DeltaTable

def create_featureLoanTrx_stream():
   
    
    silver_transactions_path = "/mnt/certazuresa/silver-finloan1/transactions"
    silver_customerdriver_path = "/mnt/certazuresa/silver-finloan1/customerdriver1"
    gold_path = "/mnt/certazuresa/gold-finloan1/featureLoanTrx"
    checkpoint_path = "/mnt/certazuresa/checkpoint-gold2/featureLoanTrx"
    
    # Read stream from silver_transactions

    

    trx_stream = (
        spark.readStream
        .format("delta")
        .load(silver_transactions_path)
        
    )
    
    
    # Read customerdriver as static snapshot
    customerdriver_static = (
        spark.read
        .format("delta")
        .load(silver_customerdriver_path)
    )
    
    # Perform stream-static join
    feature_stream = (trx_stream.alias("t")
    .join(customerdriver_static.alias("d"),
          ["customer_id", "date"],
          "inner")
    .select(
        "t.date",
        "t.customer_id",
        "t.payment_period",
        F.col("t.loan_amount").cast("double"),
        "t.currency_type",
        "t.evaluation_channel",
        F.col("t.interest_rate").cast("double"),
        F.col("d.monthly_salary").cast("double"),
        F.col("d.health_score").cast("int"),
        F.col("d.current_debt").cast("double"),
        "d.category",
        F.col("d.is_risky").alias("is_risk_customer"),
        
    )
    )
    
    
    
    # Define merge function for foreachBatch to handle deduplication
    def merge_to_gold(batch_df, batch_id):
        if not batch_df.isEmpty(): 
            delta_table = DeltaTable.forPath(spark, gold_path)
            delta_table.alias("target").merge(
                batch_df.alias("source"),
                "target.date = source.date AND target.customer_id = source.customer_id"
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
            print(f"Batch {batch_id}: Merged {batch_df.count()} records to featureLoanTrx")
    
    # Write stream with foreachBatch for merge logic
    query = (
        feature_stream.writeStream
        .foreachBatch(merge_to_gold)
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .toTable("FeatureLoanTrx")
    )
    
    
    
    
    print(f"Streaming pipeline started for featureLoanTrx")
    return query




# COMMAND ----------

feature_query=create_featureLoanTrx_stream

# COMMAND ----------

# MAGIC %md
# MAGIC AggLoanTrx

# COMMAND ----------

from pyspark.sql.functions import *
from delta.tables import *

def create_aggLoanTrx_stream():

    
    
    gold_feature_path = "/mnt/certazuresa/gold-finloan1/featureLoanTrx"
    gold_agg_path = "/mnt/certazuresa/gold-finloan1/aggLoanTrx"
    checkpoint_path = "/mnt/certazuresa/checkpoint-gold2/aggLoanTrx"
    
    # Read stream from featureLoanTrx
    feature_stream = (
        spark.readStream
        .format("delta")
        .load(gold_feature_path)
    )
    
    # Create aggregations
    agg_stream = feature_stream.groupBy(
        "date",
        "payment_period",
        "currency_type",
        "evaluation_channel",
        "category"
    ).agg(
        sum("loan_amount").alias("total_loan_amount"),
        avg("loan_amount").alias("avg_loan_amount"),
        sum("current_debt").alias("sum_current_debt"),
        avg("interest_rate").alias("avg_interest_rate"),
        max("interest_rate").alias("max_interest_rate"),
        min("interest_rate").alias("min_interest_rate"),
        avg("health_score").alias("avg_score"),
        avg("monthly_salary").alias("avg_monthly_salary")
    )
    def merge_agg_to_gold(batch_df, batch_id):
            if not DeltaTable.isDeltaTable(spark, gold_agg_path):
            # First time — create Delta table
                batch_df.write.format("delta").mode("overwrite").save(gold_agg_path)
                print(f"Batch {batch_id}: Created aggLoanTrx table")
            else:
                delta_table = DeltaTable.forPath(spark, gold_agg_path)

                delta_table.alias("target").merge(
                    batch_df.alias("source"),
                    """
                    target.date = source.date AND 
                    target.payment_period = source.payment_period AND 
                    target.currency_type = source.currency_type AND 
                    target.evaluation_channel = source.evaluation_channel AND 
                    target.category = source.category
                    """
                ).whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()

                print(f"Batch {batch_id}: Merged {batch_df.count()} records")

    
    
    # Write stream with foreachBatch for merge logic
    query = (
        agg_stream.writeStream
        .foreachBatch(merge_agg_to_gold)
        .option("checkpointLocation", checkpoint_path)
        .outputMode("complete")  # Complete mode for aggregations
        .start()
    )
    
    
    
    
    
    
    print(f"Streaming pipeline started for aggLoanTrx. Processing aggregations with deduplication.")
    return query




# COMMAND ----------

agg_query = create_aggLoanTrx_stream()

# COMMAND ----------

# MAGIC %sql
# MAGIC create table aggLoanTrx using delta location "/mnt/certazuresa/gold-finloan1/aggLoanTrx"
