from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext, ProcessWindowFunction
from pyflink.common.typeinfo import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.common import Row
from datetime import datetime, timedelta
import json

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    kafka_consumer = FlinkKafkaConsumer(
        topics='kafka_per_sec_data',
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': '10.0.1.138:9092',
            'group.id': 'new_flink_group'
        }
    )

    data_stream = env.add_source(kafka_consumer)
    data_stream.print() # 這邊測試一下
    

    def parse_stock_data(value):
        data = json.loads(value)
        event_time = datetime.fromisoformat(data['current_time']).timestamp() * 1000
        return data, event_time

    watermark_strategy = WatermarkStrategy \
        .for_bounded_out_of_orderness(timedelta(seconds=30)) \
        .with_timestamp_assigner(lambda element, timestamp: element[1])

    parsed_stream = data_stream \
        .map(parse_stock_data, output_type=Types.TUPLE([Types.MAP(Types.STRING(), Types.STRING()), Types.LONG()])) \
        .assign_timestamps_and_watermarks(watermark_strategy)

    class SMAProcessWindowFunction(ProcessWindowFunction):

        def process(self, key, context, elements, out):
            sum_vwap = 0.0
            count_vwap = 0
            real_data_count = 0
            filled_data_count = 0
            window_data_count = 0

            for element in elements:
                data = element[0]
                vwap_price_per_sec = float(data.get('vwap_price_per_sec', 0.0))
                size_per_sec = int(data.get('size_per_sec', 0))
                real_or_filled = data.get('real_or_filled', '')

                if size_per_sec != 0:
                    sum_vwap += vwap_price_per_sec
                    count_vwap += 1

                if real_or_filled == 'real':
                    real_data_count += 1
                else:
                    filled_data_count += 1

                window_data_count += 1

            sma_value = sum_vwap / count_vwap if count_vwap != 0 else 0.0

            aggregated_data = {
                "symbol": key,
                "type": "MA_data",
                "MA_type": f"{window_duration}_MA_data",
                "start": datetime.fromtimestamp(context.window().start / 1000).isoformat(),
                "end": datetime.fromtimestamp(context.window().end / 1000).isoformat(),
                "current_time": datetime.utcnow().isoformat(),
                "sma_value": sma_value,
                "sum_of_vwap": sum_vwap,
                "count_of_vwap": count_vwap,
                "window_data_count": window_data_count,
                "real_data_count": real_data_count,
                "filled_data_count": filled_data_count
            }

            out.collect(json.dumps(aggregated_data))

    window_durations = [5, 10, 15]
    for window_duration in window_durations:
        keyed_stream = parsed_stream.key_by(lambda element: element[0]['symbol'])

        windowed_stream = keyed_stream.window(
            SlidingEventTimeWindows.of(
                timedelta(seconds=window_duration),
                timedelta(seconds=1)
            )
        ).process(SMAProcessWindowFunction(), output_type=Types.STRING())

        kafka_producer = FlinkKafkaProducer(
            topic='kafka_MA_data_aggregated',
            serialization_schema=SimpleStringSchema(),
            producer_config={'bootstrap.servers': '10.0.1.138:9092'}
        )

        windowed_stream.add_sink(kafka_producer)

    env.execute("Flink MA Data Stream Processing")

if __name__ == '__main__':
    main()
