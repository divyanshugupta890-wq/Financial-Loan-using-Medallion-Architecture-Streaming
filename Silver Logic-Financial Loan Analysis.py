# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

from pyspark.sql.functions import (
    current_timestamp,
    col,
    when,
    coalesce,
    lit,
    date_format,
    to_date
)

#Silver Stream

def ingest_to_silver_stream(entity: str):

    bronze_path = f"/mnt/certazuresa/bronze-finloan1/{entity}"
    silver_path = f"/mnt/certazuresa/silver-finloan1/{entity}"
    checkpoint_path = f"/mnt/certazuresa/checkpoint-silver2/{entity}"
    #table_name = f"silver_{entity}"

    # 1️⃣ Read Bronze as stream (readStream will only process new data after checkpoint)
    df_stream = (
        spark.readStream
        .format("delta")
        .load(bronze_path)
    )

    # 2️⃣ Rename mappings (per entity)
    rename_mappings = {
        "transactions": {
            "customerid": "customer_id",
            "paymentperiod": "payment_period",
            "loanamount": "loan_amount",
            "currencytype": "currency_type",
            "evaluationchannel": "evaluation_channel"
        },
        "customer": {
            "customerid": "customer_id",
            "firstname": "first_name",
            "lastname": "last_name",
            "phone": "phone_number",
            "address": "customer_address",
            "is_active": "active_status"
        },
        "customerdriver1": {
            "customerid": "customer_id"
        }
    }

    # Apply renaming if entity exists
    if entity in rename_mappings:
        entity_mapping = rename_mappings[entity]

        df_stream = df_stream.select([
            col(original_col).alias(
                entity_mapping.get(original_col.lower(), original_col)
            )
            for original_col in df_stream.columns
        ])

    # File Specific Transformations

    if entity == "customerdriver1":
        df_stream = (
            df_stream
            # Null handling
            .withColumn("monthly_salary",
                        coalesce(col("monthly_salary"), lit(1500)))
            .withColumn("health_score",
                        coalesce(col("health_score"), lit(100)))
            .withColumn("current_debt",
                        coalesce(col("current_debt"), lit(0)))
            .withColumn("category",
                        coalesce(col("category"), lit("OTHERS")))
            # Parse date in required format
            .withColumn("date", coalesce(
                to_date(col("date"), "dd-MM-yyyy"),
                to_date(col("date"), "dd/MM/yyyy"),
                to_date(col("date"), "yyyy-MM-dd")
            ))
            # Risk flag
            .withColumn(
                "is_risky",
                when(col("health_score") < 100, True).otherwise(False)
            )
        )

    if entity == "transactions":
        df_stream = (
            df_stream
            .withColumn("date", coalesce(
                to_date(col("date"), "dd-MM-yyyy"),
                to_date(col("date"), "dd/MM/yyyy"),
                to_date(col("date"), "yyyy-MM-dd")
            ))
        )

    # 4️⃣ Write to Silver
    query = (
        df_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .start(silver_path)
    )

    print(f"Streaming Silver pipeline started for {entity}. It will process new files arriving in the Bronze path.")

    return query

# COMMAND ----------

ingest_to_silver_stream("customer")

# COMMAND ----------

ingest_to_silver_stream("customerdriver1")

# COMMAND ----------

ingest_to_silver_stream("transactions")
