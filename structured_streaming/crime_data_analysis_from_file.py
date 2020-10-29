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

    # Multiple streaming aggregations (i.e. a chain of aggregations on a streaming DF) are not yet supported
    # on streaming Datasets.
    convictions_by_borough = crime_data_df.groupBy("borough")\
                                      .agg({"value": "sum"})\
                                      .withColumnRenamed("sum(value)", "convictions")\
                                      .orderBy("convictions", ascending=False)

    # output to console in append model
    query = convictions_by_borough.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 30) \
        .start() \
        .awaitTermination()
