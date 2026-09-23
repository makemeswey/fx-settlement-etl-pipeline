# Read any parquet files

from pyspark.sql import SparkSession

spark = SparkSession.builder .appName("ReadParquetTest").getOrCreate()

df1 = spark.read.parquet("../data/gold/daily_summary/")
df2 = spark.read.parquet("../data/gold/currency_summary/")
df3 = spark.read.parquet("../data/gold/status_summary/")

df1.show()
df2.show()
df3.show()
