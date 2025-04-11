import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import *
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common import Duration
from pyflink.common.serialization import *
from pyflink.common.typeinfo import Types
from pyflink.common import Time
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.common.watermark_strategy import TimestampAssigner

from fusion_test.fusion_algorithms import parse_json, SignalProcessWindowFunction, SimpleProcessWindowFunction
from kafka import KafkaProducer
from pyflink.datastream.functions import MapFunction

# 注意这里直接使用pythonkafka接口写kafka
topic = "dwd_kafka_jm_drone"


class KafkaProducerFunction(MapFunction):
    def __init__(self):
        self.producer = None

    def open(self, runtime_context):
        # 在 open 方法中初始化 Kafka 生产者
        self.producer = KafkaProducer(bootstrap_servers='192.168.200.133:9092')

    def map(self, value):
        # 将每条数据发送到 Kafka
        self.producer.send(topic, value=value.encode('utf-8'))
        self.producer.flush()  # 确保消息被发送
        return value

    def close(self):
        # 在 close 方法中关闭 Kafka 生产者
        if hasattr(self, 'producer'):
            self.producer.close()


class CustomTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, element, record_timestamp):
        return int(element['timestamp'] * 1000)


env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
# #########################################################################################
# 注意：已经在flink-conf.yaml文件中添加了RocksDB状态后端为/savepoints/checkpoints/重启策略 配置
# 代码中可以不用显式配置
# #########################################################################################

jm_consumer = FlinkKafkaConsumer(
    topics='jm',
    properties={'bootstrap.servers': '192.168.200.133:9092',
                'group.id': 'dwd_jm_group',
                'auto_offset_reset': 'earliest'
                },
    deserialization_schema=SimpleStringSchema())
# checkpoints时不提交便宜量
jm_stream = env.add_source(jm_consumer)
# jm_stream.print("打印 jm_stream: ")

parsed_stream = jm_stream.map(parse_json, output_type=Types.MAP(Types.STRING(), Types.PICKLED_BYTE_ARRAY()))
# parsed_stream.print("打印 parsed_stream: ")

# 水位线的延迟最大延时时间
watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_millis(50)) \
    .with_timestamp_assigner(CustomTimestampAssigner())
timestamped_stream = parsed_stream.assign_timestamps_and_watermarks(watermark_strategy)

# 注意，这里的signal_id是drone_uuid和batch_num的拼接，表示当前时间的无人机的唯一id
keyed_stream = timestamped_stream.key_by(lambda x: x['signal_id'])

# 定义数据返回
window_time = Time.seconds(5)  # 滑动窗口长度
slide_time = Time.seconds(1)   # 滑动步长
preprocessed_stream = keyed_stream.window(SlidingEventTimeWindows.of(
    window_time,
    slide_time
)).process(
    # 传递步长参数，用于去重
    SignalProcessWindowFunction(slide=slide_time.to_milliseconds()),
    output_type=Types.MAP(Types.STRING(), Types.PICKLED_BYTE_ARRAY())
)

result_stream = preprocessed_stream.window_all(SlidingEventTimeWindows.of(
    Time.seconds(1),
    Time.seconds(1)
)).process(
    SimpleProcessWindowFunction(),
    # 输出记录同样是map结构，每条记录中包含所有字段
    output_type=Types.MAP(Types.STRING(), Types.PICKLED_BYTE_ARRAY())
)

converted_str_data = result_stream.map(lambda x: json.dumps({
    key: (
        json.loads(value.decode('utf-8')) if isinstance(value, bytearray) else value
    ) if isinstance(value, bytearray) else value
    for key, value in x.items()
}, ensure_ascii=False)) \

converted_str_data.map(KafkaProducerFunction())


env.execute("low-altitude-dwd_kafka_jm_drone")

