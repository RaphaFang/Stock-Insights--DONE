from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType, TimestampType
from pyspark.sql.functions import from_json, col, window, sum as spark_sum, count as spark_count,avg, last, lit, to_timestamp, current_timestamp
from pyspark.sql.functions import current_timestamp, window, when, lit, coalesce
from pyspark.sql import functions as SF

def main():
    schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("type", StringType(), True),
        StructField("exchange", StringType(), True),
        StructField("market", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("size", IntegerType(), True),
        StructField("bid", DoubleType(), True),
        StructField("ask", DoubleType(), True),
        StructField("volume", IntegerType(), True),
        StructField("isContinuous", BooleanType(), True),
        StructField("time", StringType(), True), 
        StructField("serial", StringType(), True),
        StructField("id", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("yesterday_price", DoubleType(), True),
    ])

    spark = SparkSession.builder \
        .appName("spark_per_sec_data") \
        .master("local[2]") \
        .config("spark.sql.streaming.pipelining.enabled", "true") \
        .config("spark.sql.streaming.pipelining.batchSize", "500") \
        .config("spark.sql.streaming.stateRebalancing.enabled", "true") \
        .config("spark.sql.streaming.offset.management.enabled", "true") \
        .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider") \
        .config("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true") \
        .getOrCreate()
        # .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider") \
        # 確保有開啟 State Rebalancing 和 Enhanced Offset Management
        # 使用 pipelining
        # .config("spark.executor.cores", "2") \

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "10.0.1.138:9092") \
        .option("subscribe", "kafka_raw_data") \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", "2000") \
        .option("failOnDataLoss", "false") \
        .load()
    
    def process_batch(df, epoch_id):
        try:
            df = df.selectExpr("CAST(value AS STRING) as json_data") \
                .select(from_json(col("json_data"), schema).alias("data")) \
                .select("data.*")

            df = df.withColumn("time", to_timestamp(col("time") / 1000000))
            windowed_df = df.groupBy(
                SF.window(col("time"), "1 second", "1 second"),col("symbol")
            ).agg(
                SF.sum(SF.when(col("type") != "heartbeat", col("price") * col("size")).otherwise(0)).alias("price_time_size"),
                SF.sum(SF.when(col("type") != "heartbeat", col("size")).otherwise(0)).alias("size_per_sec"),
                SF.last(SF.when(col("type") != "heartbeat", col("volume")).otherwise(0)).alias("volume_till_now"),
                SF.last("time", ignorenulls=True).alias("last_data_time"),
                SF.count(SF.when(col("type") != "heartbeat", col("symbol"))).alias("real_data_count"),
                SF.count(SF.when(col("type") == "heartbeat", col("symbol"))).alias("filled_data_count"),
                SF.last("yesterday_price", ignorenulls=True).alias("yesterday_price"),
            )#.orderBy("window.start")

            result_df = windowed_df.withColumn(
                "vwap_price_per_sec",
                SF.when(col("size_per_sec") != 0, col("price_time_size") / col("size_per_sec"))
                .otherwise(0)
            )
            result_df = result_df.withColumn(
                "price_change_percentage",
                SF.when(col("yesterday_price") != 0, 
                    SF.round(((col("vwap_price_per_sec") - col("yesterday_price")) / col("yesterday_price")) * 100, 2)
                ).otherwise(0)
            )
            result_df = result_df.withColumn(
                "real_or_filled", SF.when(col("real_data_count") > 0, "real").otherwise("filled")
            )

            result_df = result_df.orderBy("window.start") # 其實這個也沒有很消耗效能，昨天不知道為甚麼策出來很慢？？
            result_df.select(
                "symbol",
                SF.lit("per_sec_data").alias("type"),
                "window.start",
                "window.end",
                SF.current_timestamp().alias("current_time"),
                "last_data_time",
                "real_data_count",
                "filled_data_count",
                "real_or_filled",
                "vwap_price_per_sec",
                "size_per_sec",
                "volume_till_now",
                "yesterday_price",
                "price_change_percentage",
            ).selectExpr(
                "CAST(symbol AS STRING) AS key",
                "to_json(struct(*)) AS value"
            ).write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "10.0.1.138:9092") \
                .option("topic", "kafka_per_sec_data") \
                .save()
            
        except Exception as e:
            print(f"Error processing batch {epoch_id}: {e}")

    query = kafka_df.writeStream \
        .foreachBatch(lambda df, epoch_id: process_batch(df, epoch_id)) \
        .option("checkpointLocation", "/app/tmp/spark_checkpoints/spark_per_sec") \
        .start()
    # # # .trigger(processingTime='1 second') \理論上現在不應該用這個，因為這是每秒驅動一次，但如果資料累積，就會沒辦法每秒都運作，並且我已經有window來處理了
    query.awaitTermination()

if __name__ == "__main__":
    main()