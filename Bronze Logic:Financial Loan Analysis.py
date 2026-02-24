# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
def ingest_to_bronze(entity:str):
    """
    Reusable function to move data from Landing to Bronze.
    
    """
    base_data_dir=f"/mnt/certazuresa/landing-finloan1/{entity}"
    check_point_dir=f"/mnt/certazuresa/checkpoint-bronze2/{entity}"
    target_path=f"/mnt/certazuresa/bronze-finloan1/{entity}"
    #target_table = f"bronze_finloan1.{entity}"

    static_df = spark.read.csv(f"/mnt/certazuresa/landing-finloan1/{entity}", header=True)
    schema = static_df.schema
    
    df_stream = spark.readStream.format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load(base_data_dir).withColumn("InputFile",col("_metadata.file_path"))

    def clean_column_names(df):
       return df.toDF(*[
        c.strip()
         .replace(" ", "_")
         .replace("-", "_")
         .lower()
        for c in df.columns
    ])

    df_stream = clean_column_names(df_stream)




    df_stream=df_stream.withColumn("_ingest_timestamp",current_timestamp())

    df_stream.writeStream.queryName("ingest_to_bronze_n") \
    .format("delta") \
    .option("checkpointLocation", check_point_dir) \
    .outputMode("append") \
    .start(target_path)
    
           
           


     
    print(f"Data ingested from {base_data_dir} to {target_path}")




# COMMAND ----------

ingest_to_bronze("customer")


# COMMAND ----------

ingest_to_bronze("transactions")

# COMMAND ----------

ingest_to_bronze("customerdriver")
