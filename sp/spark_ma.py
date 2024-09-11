from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType, TimestampType
from pyspark.sql.functions import from_json, col, window, sum as spark_sum, count as spark_count,avg, last, lit, to_timestamp, current_timestamp
from pyspark.sql import functions as SF

def main():
    schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("type", StringType(), True),
        StructField("start", TimestampType(), True),
        StructField("end", TimestampType(), True),

        StructField("current_time", TimestampType(), True),
        StructField("last_data_time", TimestampType(), True),
        StructField("real_data_count", IntegerType(), True),
        StructField("filled_data_count", IntegerType(), True),

        StructField("real_or_filled", StringType(), True),
        StructField("vwap_price_per_sec", DoubleType(), True),
        StructField("size_per_sec", IntegerType(), True),
        StructField("volume_till_now", IntegerType(), True),

        StructField("yesterday_price", DoubleType(), True),
        StructField("price_change_percentage", DoubleType(), True),
    ])

    spark = SparkSession.builder \
        .appName("spark_MA_data") \
        .master("local[2]") \
        .config("spark.sql.streaming.pipelining.enabled", "true") \
        .config("spark.sql.streaming.pipelining.batchSize", "500") \
        .config("spark.sql.streaming.stateRebalancing.enabled", "true") \
        .config("spark.sql.streaming.offset.management.enabled", "true") \
        .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider") \
        .config("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true") \
        .getOrCreate()
        # .config("spark.locality.wait", "10s") \
        # 上面這是等待將數據調用過來的時間 
        # 確保有開啟 State Rebalancing 和 Enhanced Offset Management # 使用 pipelining # 發現都沒用...還是會分批次
        # .config("spark.executor.cores", "2") \

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "10.0.1.138:9092") \
        .option("subscribe", "kafka_per_sec_data") \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", "2000") \
        .option("failOnDataLoss", "false") \
        .load()    

    # groupBy，spark的groupBy會需要透過Shuffle來處理資料，而Shuffle很消耗 cpu 以及 io，因為他要把資料傳遞到不同的節點，以及全局運算資料
    def calculate_sma(df, window_duration):
        df_with_watermark = df.withWatermark("start", "15 seconds")
        sma_df = df_with_watermark.groupBy(
            SF.window(col("start"), f"{window_duration} seconds", "1 second"), col("symbol")
        ).agg(
            SF.sum(SF.when(col("size_per_sec") != 0, col("vwap_price_per_sec")).otherwise(0)).alias('sum_of_vwap'),
            SF.count(SF.when(col("size_per_sec") != 0, col("vwap_price_per_sec"))).alias('count_of_vwap'),  # !這邊不應該用otherwise...........
            SF.count("*").alias("window_data_count"),
            SF.collect_list("start").alias("start_times"),
            SF.count(SF.when(col("real_or_filled") == "real", col("symbol"))).alias("real_data_count"),
            SF.count(SF.when(col("real_or_filled") != "real", col("symbol"))).alias("filled_data_count"),
        )#.orderBy("window.start") ##這邊的排序會將聚合前，不同節點資料調動，所以理論上會更消耗效能

        # sma_df = sma_df.withColumn("first_current_time", current_timestamp())
        sma_df = sma_df.orderBy("window.start") # 這邊的排序理論上是已經聚合後的資料，把這邊的資料排序

        ## 切記，會因為算式可能為空，導致輸出出錯，然後numInputRows就一直會是0
        sma_df = sma_df.withColumn(
            "sma_value",
            SF.when(col('count_of_vwap') != 0,
                SF.round(col('sum_of_vwap') / col('count_of_vwap'), 2)
            ).otherwise(0)
        )
        return sma_df
    
    # def merge_overlapping_windows(sma_df):
    #     expanded_df = sma_df.withColumn("start", col("window.start")).withColumn("end", col("window.end"))
    #     merged_df = expanded_df.withWatermark("start", "15 seconds").groupBy(
    #         col("start"),
    #         col("end"),
    #         col("symbol")
    #     ).agg(
    #         SF.sum("sum_of_vwap").alias("m_sum_of_vwap"),
    #         SF.sum("count_of_vwap").alias("m_count_of_vwap"),
    #         SF.sum("window_data_count").alias("m_window_data_count"),
    #         SF.sum("real_data_count").alias("m_real_data_count"),
    #         SF.sum("filled_data_count").alias("m_filled_data_count"),
    #         SF.min("first_current_time").alias("first_current_time")
    #     ).withColumn(
    #         "merged_sma_value",
    #         SF.when(col("m_count_of_vwap") != 0,
    #                 SF.round(col("m_sum_of_vwap") / col("m_count_of_vwap"), 2)
    #         ).otherwise(0)
    #     )
    #     return merged_df

    def send_to_kafka(df, window_duration):
        df.select(            
            col("symbol"),
            lit("MA_data").alias("type"),
            lit(f"{window_duration}_MA_data").alias("MA_type"),
            col(f"window.start").alias("start"),
            col(f"window.end").alias("end"),

            SF.current_timestamp().alias("current_time"),
            SF.element_at(col("start_times"), 1).alias("first_data_time"),
            SF.element_at(col("start_times"), -1).alias("last_data_time"),

            col("sma_value"),
            col("sum_of_vwap"),
            col("count_of_vwap"),
            col("window_data_count"),
            
            col("real_data_count"),
            col("filled_data_count"),
        ).selectExpr(
            "CAST(symbol AS STRING) AS key",
            "to_json(struct(*)) AS value"
        ).write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "10.0.1.138:9092") \
            .option("topic", "kafka_MA_data") \
            .save()
        # df.select(            
        #     col("symbol"),
        #     lit("MA_data").alias("type"),
        #     lit(f"{window_duration}_MA_data").alias("MA_type"),
        #     col("start"),
        #     col("end"), 

        #     col("first_current_time"),
        #     SF.current_timestamp().alias("m_current_time"),
        #     col("merged_sma_value"),
        #     col("m_sum_of_vwap"),
        #     col("m_count_of_vwap"),
        #     col("m_window_data_count"),
            
        #     col("m_real_data_count"),
        #     col("m_filled_data_count"),
        # ).selectExpr(
        #     "CAST(symbol AS STRING) AS key",
        #     "to_json(struct(*)) AS value"
        # ).write \
        #     .format("kafka") \
        #     .option("kafka.bootstrap.servers", "10.0.1.138:9092") \
        #     .option("topic", "kafka_MA_data") \
        #     .save()
        
    def process_batch(df, epoch_id, ):
        try:
            df = df.selectExpr("CAST(value AS STRING) as json_data") \
                .select(SF.from_json(col("json_data"), schema).alias("data")) \
                .select("data.*")

            sma_5 = calculate_sma(df, 5)
            # merged_sma_5 = merge_overlapping_windows(sma_5)

            send_to_kafka(sma_5, 5)
            
        except Exception as e:
            print(f"Error processing batch {epoch_id}: {e}")
            
    query = kafka_df.writeStream \
        .foreachBatch(lambda df, epoch_id: process_batch(df, epoch_id)) \
        .trigger(continuous='1 second')\
        .outputMode("update") \
        .option("checkpointLocation", "/app/tmp/spark_checkpoints/spark_ma") \
        # .start()
        # .trigger(processingTime='20 seconds') \
    query.awaitTermination()

if __name__ == "__main__":
    main()
            # sma_15 = calculate_sma(df, 15, "sma_15", broadcast_15ma)
            # sma_30 = calculate_sma(df, 30, "sma_30", broadcast_30ma)

            # merged_sma_5 = merge_overlapping_windows(sma_5)

            # send_to_kafka(sma_15)
            # send_to_kafka(sma_30)

# 等待整理note ---------------------------------------------------------------------------------------------------
        # window_spec = Window.partitionBy("symbol").orderBy("window.start")
        # sma_df = sma_df.withColumn("rank", SF.row_number().over(window_spec)).orderBy("rank")
        ## 這邊的排序是全局的排序，極度耗費效能，因為要shuffle
# ---------------------------------------------------------------------------------------------------

        # sma_df = sma_df.withColumn(
        #     "sma_value",
        #     SF.when(col('count_of_vwap') != 0,
        #         SF.round(col('sum_of_vwap') / col('count_of_vwap'), 2)
        #     ).otherwise(0)
        # )

        # sma_df = sma_df.select(            
        #     col("symbol"),
        #     lit("MA_data").alias("type"),
        #     lit(f"{window_duration}_MA_data").alias("MA_type"),
        #     col(f"window.start").alias("start"),
        #     col(f"window.end").alias("end"),

        #     SF.current_timestamp().alias("current_time"),
        #     # SF.element_at(col("start_times"), 1).alias("first_data_time"),
        #     # SF.element_at(col("start_times"), -1).alias("last_data_time"),

        #     # col("sma_value"),
        #     col("sum_of_vwap"),
        #     col("count_of_vwap"),
        #     col("window_data_count"),
            
        #     col("real_data_count"),
        #     col("filled_data_count"),
        # )
# ---------------------------------------------------------------------------------------------------

    # b_5ma = 100
    # b_15ma = 200
    # b_30ma = 300

    # broadcast_5ma = spark.sparkContext.broadcast(b_5ma)
    # broadcast_15ma = spark.sparkContext.broadcast(b_15ma)
    # broadcast_30ma = spark.sparkContext.broadcast(b_30ma)

        # window_spec = Window.partitionBy("symbol").orderBy("window.start")
        # sma_df = sma_df.withColumn("rank", SF.row_number().over(window_spec)).orderBy("rank")

        # current_broadcast_value = broadcast.value
        # sma_df = sma_df.withColumn(
        #     "prev_sma", SF.lag(column_name, 2).over(window_spec)
        # )
        # result_df = result_df.withColumn(
        #     "prev_sma", SF.when(SF.col("prev_sma").isNull(), SF.lit(0))
        # )
        # sma_df = sma_df.withColumn(
        #     column_name, 
        #     SF.when(col(column_name) == 0, SF.coalesce(col("prev_sma"))) # , SF.lit(current_broadcast_value)
        #     .otherwise(col(column_name))
        # )