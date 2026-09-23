from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def create_gold():
    spark = (
        SparkSession.builder.appName("FX Gold").getOrCreate()
    )

    silver_df = spark.read.parquet("../data/silver/")
    # silver_df.show()

    currency_summary = (
        silver_df.groupBy("target_currency").agg(F.count("*").alias("transaction_count"), F.round(F.sum("gross_amount"),2).alias("total_myr_volume"))
    )

    currency_summary.write.mode("overwrite").parquet("../data/gold/currency_summary/")

    settlement_summary = (
        silver_df.groupBy("status").agg(F.count("*").alias("count"))
    )

    settlement_summary.write.mode("overwrite").parquet("../data/gold/status_summary/")

    daily_summary = (
        silver_df.withColumn("date", F.to_date(F.col("ingestion_timestamp"))).groupBy("date").agg(
            F.count("*").alias("transaction_count"),
            F.round(F.sum("gross_amount"),2).alias("total_myr_volume"),
            F.round(F.avg("gross_amount"),2).alias("average_transaction"),
            F.sum(F.when(F.col("status") == "SETTLED",1).otherwise(0)).alias("settlement_count"),
            F.sum(F.when(F.col("status") == "FAILED",1).otherwise(0)).alias("failed_count")
        )
    )

    daily_summary.write.mode("overwrite").parquet("../data/gold/daily_summary/")

    spark.stop()

if __name__ == "__main__":
    create_gold()