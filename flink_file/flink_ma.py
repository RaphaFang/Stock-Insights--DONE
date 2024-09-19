from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.common.typeinfo import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.window import SlidingEventTimeWindows
from datetime import datetime, timedelta
import json

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers('10.0.1.138:9092') \
        .set_topics('kafka_per_sec_data') \
        .set_group_id('new_flink_group') \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    data_stream = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.for_bounded_out_of_orderness(timedelta(seconds=30)),
        source_name="Kafka Source"
    )

    data_stream.print()

    def parse_stock_data(value):
        data = json.loads(value)
        event_time = datetime.fromisoformat(data['current_time']).timestamp() * 1000
        return data, event_time

    parsed_stream = data_stream \
        .map(parse_stock_data, output_type=Types.TUPLE([Types.MAP(Types.STRING(), Types.STRING()), Types.LONG()]))

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

        kafka_sink = KafkaSink.builder() \
            .set_bootstrap_servers('10.0.1.138:9092') \
            .set_record_serializer(KafkaRecordSerializationSchema.builder()
                                   .set_topic('kafka_MA_data_aggregated')
                                   .set_value_serialization_schema(SimpleStringSchema())
                                   .build()) \
            .build()

        windowed_stream.sink_to(kafka_sink)

    env.execute("Flink MA Data Stream Processing")

if __name__ == '__main__':
    main()
