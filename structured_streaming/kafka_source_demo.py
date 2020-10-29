from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("Kafka Source Demos")\
        .getOrCreate()

    # connect to Kafka
    input_df = spark.readStream\
                        .format("kafka")\
                        .option("kafka.bootstrap.servers", "localhost:9092,localhost:9093")\
                        .option("subscribe", "logs_stream") \
                        .load()
    # input_df
    # +----+----------------+-----------+---------+-------+-----------------------+-------------+
    # |key |value           |topic      |partition|offset |timestamp              |timestampType|
    # +----+----------------+-----------+---------+-------+-----------------------+-------------+
    # |null|[68 65 6C 6C 6F]|logs_stream|1        |2066677|2018-07-28 03:12:39.902|0            |
    # +----+----------------+-----------+---------+-------+-----------------------+-------------+
    query = input_df.writeStream\
                   .outputMode("append")\
                   .format("console")\
                   .option("truncate", "false")\
                   .start()\
                   .awaitTermination()
