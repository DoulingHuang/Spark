from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    # create SparkSession
    spark = SparkSession\
        .builder\
        .appName("Structured Network Wordcount")\
        .getOrCreate()

    # Create DataFrame representing the stream of input lines from connection to localhost:9999
    lines = spark \
        .readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9999) \
        .load()

    # Split the lines into words
    words = lines.select(explode(split(lines["value"], " ")).alias("word"))

    # Generate running word count
    wordCounts = words.groupBy("word").agg(count("*"))


     # Start running the query that prints the running counts to the console
    query = wordCounts \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .trigger(once=True) \
        .start()

    query.awaitTermination()
