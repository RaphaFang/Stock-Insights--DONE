from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum as spark_sum, avg, last, lit, to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType

def main():
    spark = SparkSession.builder \
        .appName("spark_app_first") \
        .getOrCreate()

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
        StructField("channel", StringType(), True)
    ])
        
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "raw_data") \
        .option("startingOffsets", "latest") \
        .load()

    def process_batch(df, epoch_id):
        print("hihihihih")
        df.printSchema() 
        df.show()
        if df.rdd.isEmpty():
            print(f"Batch {epoch_id} is empty, skipping processing.")
            return


        df = df.selectExpr("CAST(value AS STRING) as json_data") \
            .select(from_json(col("json_data"), schema).alias("data")) \
            .select("data.*")
            
        df = df.withColumn("time", to_timestamp(col("time") / 1000000))

        # df.printSchema()  # 這是檢查用的，

    # 讀取出資料，並且建立計算基本資料，並且使用withWatermark然下面沒辦法用append
        windowed_df = df.groupBy(
            window(col("time"), "1 second"),
            col("symbol")
        ).agg(
            spark_sum(col("price") * col("size")).alias("price_time_size"),  
            spark_sum("size").alias("size_per_sec"),
            last("volume", ignorenulls=True).alias("volume_till_now"),
            last("time", ignorenulls=True).alias("last_data_time"),
            last("isContinuous", ignorenulls=True).alias("isContinuous")
        )
    # 計算每秒資料，並且調整欄位名稱，準備輸出
        result_df = windowed_df.withColumn(
            "vwap_price_per_sec", col("price_time_size") / col("size_per_sec")
        ).select(
            "symbol",
            "vwap_price_per_sec",
            "volume_till_now",
            "size_per_sec",
            "last_data_time",
            "isContinuous",
            "window.start", 
            "window.end",
            current_timestamp().alias("current_time") 
            # "last_serial as serial",  # 這個架構好像不支持這樣重新命名的操作
            # col("last_isClose").alias("isClose"),  # 不然就是要這樣命名
        )

        result_df.selectExpr(
            "CAST(symbol AS STRING) AS key",
            "to_json(struct(*)) AS value"
        ).write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("topic", "processed_data") \
            .save()
            
    query = kafka_df.writeStream \
        .foreachBatch(process_batch) \
        .trigger(processingTime='1 second') \
        .option("checkpointLocation", "/app/tmp/spark_checkpoints") \
        .start()
    query.awaitTermination()

if __name__ == "__main__":
    main()

# ---------------------------------------------------------------------------------------------------

# class SparkHandler:
#     def __init__(self):
#         self.spark = SparkSession.builder \
#             .master("local[*]") \
#             .appName("SparkApp") \
#             .config("spark.executor.cores", "2") \
#             .getOrCreate()
        
#             # .config("spark.executor.instances", "1") \
#             # executor，是設立spark的「節點」，要在多台主機的情況這設置才有意義
#             # core，會用到兩個核心
#             # .config("spark.jars", "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.1.2.jar") \

#         self.schema = StructType([
#             StructField("symbol", StringType(), True),
#             StructField("type", StringType(), True),
#             StructField("exchange", StringType(), True),
#             StructField("market", StringType(), True),
#             StructField("price", DoubleType(), True),
#             StructField("size", IntegerType(), True),
#             StructField("bid", DoubleType(), True),
#             StructField("ask", DoubleType(), True),
#             StructField("volume", IntegerType(), True),
#             StructField("isContinuous", BooleanType(), True),
#             StructField("time", StringType(), True), 
#             StructField("serial", StringType(), True),
#             StructField("id", StringType(), True),
#             StructField("channel", StringType(), True)
#         ])
#         # self.latest_data = {}

#     def process_data(self):
#         kafka_df = self.spark.readStream \
#             .format("kafka") \
#             .option("kafka.bootstrap.servers", "kafka:9092") \
#             .option("subscribe", "raw_data") \
#             .option("startingOffsets", "latest") \
#             .load()
        
#         def process_batch(df, epoch_id):
#             if df.rdd.isEmpty():
#                 print(f"Batch {epoch_id} is empty, skipping processing.")
#                 return

#             df = df.selectExpr("CAST(value AS STRING) as json_data") \
#                 .select(from_json(col("json_data"), self.schema).alias("data")) \
#                 .select("data.*")
            
#             df = df.withColumn("time", to_timestamp(col("time") / 1000000))

#             df.printSchema()

# # 讀取出資料，並且建立計算基本資料，並且使用withWatermark然下面沒辦法用append
#             # .withWatermark("time", "1 minute")
#             windowed_df = df.groupBy(
#                 window(col("time"), "1 second"),
#                 col("symbol")
#             ).agg(
#                 spark_sum(col("price") * col("size")).alias("price_time_size"),  
#                 spark_sum("size").alias("size_per_sec"),
#                 last("volume", ignorenulls=True).alias("volume_till_now"),
#                 last("time", ignorenulls=True).alias("last_data_time"),
#                 last("isContinuous", ignorenulls=True).alias("isContinuous")
#             )
# # 計算每秒資料，並且調整欄位名稱，準備輸出
#             result_df = windowed_df.withColumn(
#                 "vwap_price_per_sec", col("price_time_size") / col("size_per_sec")
#             ).select(
#                 "symbol",
#                 "vwap_price_per_sec",
#                 "volume_till_now",
#                 "size_per_sec",
#                 "last_data_time",
#                 "isContinuous",
#                 "window.start", 
#                 "window.end",
#                 current_timestamp().alias("current_time") 
#                 # "last_serial as serial",  # 這個架構好像不支持這樣重新命名的操作
#                 # col("last_isClose").alias("isClose"),  # 不然就是要這樣命名
#             )

#             result_df.selectExpr(
#                 "CAST(symbol AS STRING) AS key",
#                 "to_json(struct(*)) AS value"
#             ).write \
#                 .format("kafka") \
#                 .option("kafka.bootstrap.servers", "kafka:9092") \
#                 .option("topic", "processed_data") \
#                 .save()
            
#         query = kafka_df.writeStream \
#             .foreachBatch(process_batch) \
#             .trigger(processingTime='1 second') \
#             .option("checkpointLocation", "/app/tmp/spark-checkpoints") \
#             .start()
#         query.awaitTermination()

#     def stop(self):
#         self.spark.stop()