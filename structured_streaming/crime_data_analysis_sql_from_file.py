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

    # Registering Table
    crime_data_df.createOrReplaceTempView("crime_data")

    # Make aggregation in SQL
    convictions_by_borough = spark.sql("SELECT borough, SUM(value) AS convictions \
                            FROM crime_data \
                            WHERE year = '2016' \
                            GROUP BY borough \
                            ORDER BY convictions DESC ")


    # output to console in append model
    query = convictions_by_borough.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 30) \
        .start() \
        .awaitTermination()
