from pyspark.sql import SparkSession

def create_gold():
    spark = (
        SparkSession.builder.appName("FX Gold").getOrCreate()
    )

    silver_df = spark.read.parquet("../data/silver/")
    silver_df.show()

if __name__ == "__main__":
    create_gold()