
from fusion_test.filter_algorithms import TrackStateEstimation
from utils import *
import json
from datetime import datetime
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import *
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common import Duration
from pyflink.common.serialization import *
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner
from kafka import KafkaProducer
from pyflink.datastream.functions import MapFunction
import requests
import sys

topic = "dws_kafka_jm_drone"
############################################################################
# 发送数据后端接口
target_url = f'http://192.168.200.138:8088/focusHz/fusion/push'
############################################################################


# 定义一个转换函数，将 MAP 数据转换为目标字典结构
class TransformResultToTrack(MapFunction):
    def map(self, value):
        # 解析 JSON 字符串为字典
        value = parse_str_dict(value)

        # 处理起飞时间： '92025ㆍ20241226174202401' 转成 2024-12-05 17:21:03
        # 获取 "20241226174202401"
        time_part = value.get('intrusion_start_time', '').split('ㆍ')[1]
        # 转换为目标时间格式
        # 截取前14位并解析
        parsed_time = datetime.strptime(time_part[:14], '%Y%m%d%H%M%S')
        takeofftime = parsed_time.strftime('%Y-%m-%d %H:%M:%S')

        # 提取 scanid 中所有的 id 值
        scanids = [item['id'] for item in value.get('scanID', []) if 'id' in item]
        # 将多个 id 用 '/' 分割
        driverid = '/'.join(scanids)

        transformed_data = {
            'fusion_id': value.get('fusion_id', ''),
            'timestamp': value.get('timestamp', 0),
            'utm_x': value.get('utm_x', 0.0),
            'utm_y': value.get('utm_y', 0.0),
            'latitude': value.get('latitude', 0.0),
            'longitude': value.get('longitude', 0.0),
            'speed': value.get('speed', 0.0),
            'height': value.get('height', 0.0),
            'heading': value.get('heading', 0.0),
            'model': value.get('model', ''),
            'drone_uuid': value.get('drone_uuid', ''),
            'status': 1,
            'batchno': value.get('batch_num', ''),
            'takeofftime': takeofftime,
            'takeofflongitude': value.get('start_longitude', 0.0),
            'takeofflatitude': value.get('start_latitude', 0.0),
            'driverlongitude': value.get('driverlongitude', 0.0),
            'driverlatitude': value.get('driverlatitude', 0.0),
            'companyid': 29570,
            'station_id': value.get('station_id', ''),
            'driverid': driverid
        }
        return transformed_data


# 自定义水位线提取器
class CustomTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, element, record_timestamp):
        # 解析 JSON 字符串为字典
        value = parse_str_dict(element)
        # print(f"提取时间戳: {value}")
        return int(value['timestamp'] * 1000)
# 注意这里直接使用pythonkafka接口写kafka


class KafkaProducerFunction(MapFunction):
    def __init__(self):
        self.producer = None

    def open(self, runtime_context):
        # 在 open 方法中初始化 Kafka 生产者
        self.producer = KafkaProducer(bootstrap_servers='192.168.200.133:9092')

    def map(self, value):
        try:
            # 将每条数据发送到 Kafka
            self.producer.send(topic, value=value.encode('utf-8'))
            self.producer.flush()  # 确保消息被发送
        except Exception as e:
            print(f"数据写kafka异常，错误信息：{e}")
            # 当调用后端接口发送异常时，打印错误信息并退出程序
            sys.exit(1)

        try:
            data = parse_str_dict(value)
            # print(f"打印 data2: {data}")

            for key, value in data.items():
                if isinstance(value, float):  # 判断值是否是浮动类型（double）
                    data[key] = round(value, 10)  # 调整为指定精度

            # 请求接口发送数据
            response = requests.post(target_url, json=data, verify=False)
            # print(response.status_code)
            # print(data)
            # print(response.text)
        except Exception as e:
            print(f"调用后端接口异常：{target_url}，错误信息：{e}")
            # 当调用后端接口发送异常时，打印错误信息并退出程序
            sys.exit(1)

        return value

    def close(self):
        # 在 close 方法中关闭 Kafka 生产者
        if hasattr(self, 'producer'):
            self.producer.close()


env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

jm_consumer = FlinkKafkaConsumer(
    topics='jm',
    properties={'bootstrap.servers': '192.168.200.133:9092',
                'group.id': 'dwd_jm_group',
                'auto_offset_reset': 'earliest'
                },
    deserialization_schema=SimpleStringSchema())

# checkpoints时不提交便宜量
# jm_consumer.set_commit_offsets_on_checkpoints(False)
jm_stream = env.add_source(jm_consumer)
# jm_stream.print("打印 jm_stream: ")·

# 水位线的延迟最大延时时间
watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_millis(50)) \
    .with_timestamp_assigner(CustomTimestampAssigner())
watermark_stream = jm_stream.assign_timestamps_and_watermarks(watermark_strategy)

transformed_stream = watermark_stream.map(TransformResultToTrack(),
                                          output_type=Types.MAP(Types.STRING(), Types.PICKLED_BYTE_ARRAY()))

keyed_stream = transformed_stream.key_by(lambda x: x['fusion_id'])
final_stream = keyed_stream.process(TrackStateEstimation())

converted_str_data = final_stream.map(lambda x: json.dumps({
    key: (
        json.loads(value.decode('utf-8')) if isinstance(value, bytearray) else value
    ) if isinstance(value, bytearray) else value
    for key, value in x.items()
}, ensure_ascii=False)) \

converted_str_data.map(KafkaProducerFunction())

env.execute("low-altitude-dws_kafka_jm_drone")
