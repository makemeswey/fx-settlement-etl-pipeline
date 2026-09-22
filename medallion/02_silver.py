from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def clean_data():
    spark = (
        SparkSession.builder.appName("FX Silver").getOrCreate()
    )

    df = spark.read.json("../data/bronze/")

    silver_df = (
        df
        .dropDuplicates()
        .withColumn("gross_amount", col("gross_amount").cast("double"))
        .withColumn("applied_fx_rate", col("applied_fx_rate").cast("double"))
        .withColumn(
            "converted_target_amount",
            col("converted_target_amount").cast("double")
        )
        .filter(col("gross_amount") > 0)
        .filter(col("applied_fx_rate") > 0)
    )

    silver_df.write.mode("append").parquet("../data/silver/")

    spark.stop()

if __name__ == "__main__":
    clean_data()