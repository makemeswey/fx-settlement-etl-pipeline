# Read any parquet files

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession

from paths import CURRENCY_SUMMARY, STATUS_SUMMARY, DAILY_SUMMARY

spark = SparkSession.builder .appName("ReadParquetTest").getOrCreate()

df1 = spark.read.parquet(str(DAILY_SUMMARY))
df2 = spark.read.parquet(str(CURRENCY_SUMMARY))
df3 = spark.read.parquet(str(STATUS_SUMMARY))

df1.show()
df2.show()
df3.show()
