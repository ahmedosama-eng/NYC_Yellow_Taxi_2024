from pyspark.sql import SparkSession    
from Extract.data_reader import reading_the_files_in_df,reading_the_lookups

spark = SparkSession.builder \
    .appName("Taxi_analysis_2024") \
    .config("spark.ui.port", "4041") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.memory", "1g") \
    .config("spark.jars", r"C:\Program Files\PostgreSQL\17\jdbc\postgresql-42.7.5.jar") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.hadoop.io.native.lib.available", "false")

# reading from local DataLack
data=reading_the_files_in_df(spark)
lookups=reading_the_lookups(spark)