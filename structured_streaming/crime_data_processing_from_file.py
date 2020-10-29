from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":

    # create SparkSession
    spark = SparkSession \
        .builder \
        .appName("File Source Demo") \
        .getOrCreate()

    # schema for the crime data
    schema = StructType([StructField("lsoa", StringType(), True), \
                         StructField("borough", StringType(), True), \
                         StructField("major_category", StringType(), True), \
                         StructField("minor_category", StringType(), True), \
                         StructField("value", IntegerType(), True), \
                         StructField("year", IntegerType(), True), \
                         StructField("month", IntegerType(), True)])
    # Read file stream
    crime_data_df = spark.readStream \
        .csv("hdfs://localhost/user/cloudera/structured_streaming_101/staging", schema=schema)

    # Some information about the stream
    print("isStreaming? ")
    print(crime_data_df.isStreaming)

    print("Schema: ")
    print(crime_data_df.printSchema)

    result_df = crime_data_df.select(
        crime_data_df["borough"],
        crime_data_df["year"],
        crime_data_df["month"],
        crime_data_df["value"]
    ).withColumnRenamed("value", "convictions")

    # output to console in append model
    query = result_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 30) \
        .start() \
        .awaitTermination()








