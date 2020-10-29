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

    logs_df = input_df.select(input_df["value"].cast("string"))

    # input_df
    # +-----+
    # |value|
    # +-----+
    # |hello|
    # +-----+

    query = logs_df.writeStream\
                   .outputMode("append")\
                   .format("console")\
                   .option("truncate", "false")\
                   .start()\
                   .awaitTermination()
