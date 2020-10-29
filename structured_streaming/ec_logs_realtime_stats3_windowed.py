from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

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

    logs_df = input_df.select(input_df["value"].cast("string")) \
                      .selectExpr("split(value,',')[0] as timestamp",
                                  "split(value,',')[2] as action",
                                  "split(value,',')[3] as visitor",
                                  "split(value,',')[5] as product")

    visitor_activity = logs_df.select(logs_df["visitor"],
                                      when(logs_df["action"] == "sale", 1).otherwise(0).alias("is_sale"),
                                      when(logs_df["action"] == "add_to_cart", 1).otherwise(0).alias("is_add_to_cart"),
                                      when(logs_df["action"] == "page_view", 1).otherwise(0).alias("is_page_view"),
                                      logs_df["timestamp"])

    user_activity_df = visitor_activity.groupBy(window(visitor_activity["timestamp"], "30 minutes", "30 minutes"),
                                                "visitor") \
                                       .agg(sum("is_sale").alias("number_of_sales"),
                                            sum("is_add_to_cart").alias("number_of_add_to_carts"),
                                            sum("is_page_view").alias("number_of_page_views"),
                                            count("*").alias("number_of_events"))

    user_activity_df_sorted = user_activity_df.orderBy(user_activity_df["window"].desc(),
                                                       user_activity_df["number_of_add_to_carts"].desc())

    query = user_activity_df_sorted.writeStream\
                                   .outputMode("complete")\
                                   .format("console")\
                                   .option("truncate", "false") \
                                   .option("numRows", 100) \
                                   .trigger(processingTime="30 seconds") \
                                   .start()\
                                   .awaitTermination()
