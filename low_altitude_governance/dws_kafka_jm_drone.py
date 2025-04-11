import json
from datetime import datetime
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext, ProcessWindowFunction
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import *
from pyflink.common.watermark_strategy import WatermarkStrategy 
from pyflink.common import Duration
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.state import MapStateDescriptor
from pyflink.common.time import Time
import re
from pyflink.common.serialization import *
from pyproj import Transformer
import numpy as np
from scipy.spatial.distance import euclidean
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows
import hashlib
from itertools import groupby
from operator import itemgetter
import pickle
from pyflink.common import Row
from pyflink.datastream.checkpoint_config import CheckpointConfig
from pyflink.datastream.checkpoint_config import CheckpointStorage
from pyflink.common import Configuration
from pyflink.common import Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pykalman import KalmanFilter

class SignalData(object):
    def __init__(self, data):
        self.data = data

signal_data_type_info = Types.PICKLED_BYTE_ARRAY()

def parse_str_dict(value):
    """
    尝试将 JSON 格式的字符串转换为 Python 字典。如果转换失败，返回 None。
    参数:
        value (str): 待转换的字符串，格式："{\"utm_x\": -426232.1864696153, \"fusion_height\": 147.0}"
    返回:
        dict 或 None: 成功返回字典，失败返回 None。
    """
    if not isinstance(value, str):
        print(f"错误：输入的值不是字符串类型: {type(value)}")
        return None

    # 清理特殊字符（如非法控制字符）
    def clean_invalid_chars(input_str):
        # 替换 \r\n 为 \n，移除其他非法控制字符
        cleaned = re.sub(r"\\r\\n", "", input_str)
        cleaned = re.sub(r"\\r", "", cleaned)
        cleaned = re.sub(r"\\n", "", cleaned)
        # 移除非法控制字符（范围为0x00到0x1F，且不是合法的换行符或制表符）
        return re.sub(r"[\x00-\x1F\x7F]", "", cleaned)

    try:
        # 清理特殊字符
        cleaned_value = clean_invalid_chars(value)
        # print(f"清理后的值: {type(cleaned_value)} : {cleaned_value}")

        # 尝试将字符串解析为字典
        #json_str = json.loads(cleaned_value)
        # print(f"字符串转json字符串: {json_str}")
        parsed_dict = json.loads(cleaned_value)
        # print(f"json字符串转dict:{type(parsed_dict)} : {parsed_dict}")

        if isinstance(parsed_dict, dict):
            return parsed_dict
        else:
            print(f"错误：解析成功，但结果不是字典: {parsed_dict}")
            return None
    except json.JSONDecodeError as e:
        print(f"JSON 解析错误：{e}, 输入值: {value}")
        return None
    except Exception as e:
        print(f"未知错误：{e}, 输入值: {value}")
        return None




class TrackStateEstimation(KeyedProcessFunction):
    def open(self, runtime_context):
        # 用于存储 Kalman Filter 状态的状态变量
        # 因为 kf 对象无法直接用字符串或简单类型存储，这里使用pickle序列化
        self.kf_state = runtime_context.get_state(
            ValueStateDescriptor("kf_state", Types.PICKLED_BYTE_ARRAY()))
        
        # 存储上一次处理过的时间戳
        self.last_timestamp_state = runtime_context.get_state(
            ValueStateDescriptor("last_timestamp", Types.LONG()))

        self.utm_to_latlon_transformer = Transformer.from_crs("epsg:32651", "epsg:4326", always_xy=True)


    def process_element(self, value, ctx: KeyedProcessFunction.Context):
        current_timestamp = value['timestamp']
        last_timestamp = self.last_timestamp_state.value()

        if last_timestamp is None:
            # 第一次收到该轨迹的数据，初始化 KalmanFilter
            dt = 1.0  # 初次可用默认值
            transition_matrix = [[1, 0, dt, 0],
                                 [0, 1, 0, dt],
                                 [0, 0, 1, 0],
                                 [0, 0, 0, 1]]
            observation_matrix = [[1, 0, 0, 0],
                                  [0, 1, 0, 0]]
            initial_state_mean = [value['utm_x'], value['utm_y'], 0, 0]

            kf = KalmanFilter(
                transition_matrices=transition_matrix,
                observation_matrices=observation_matrix,
                initial_state_mean=initial_state_mean,
                observation_covariance=1e3 * np.eye(2),
                transition_covariance=1e2 * np.eye(4)
            )

            # 初次状态和协方差（可以直接使用kf初始化的值）
            state_mean = np.array(initial_state_mean)
            state_covariance = kf.transition_covariance

        else:
            # 非第一条数据，根据当前和上次时间戳计算 dt
            dt = current_timestamp - last_timestamp
            if dt <= 0:
                # 如果 dt <= 0，可能数据时间错乱，默认dt=1或跳过处理
                dt = 1.0

            kf_state_data = self.kf_state.value()
            if kf_state_data is None:
                # 理论上不会出现，因为第一次就已初始化
                # 防御性写法
                transition_matrix = [[1, 0, dt, 0],
                                     [0, 1, 0, dt],
                                     [0, 0, 1, 0],
                                     [0, 0, 0, 1]]
                observation_matrix = [[1, 0, 0, 0],
                                      [0, 1, 0, 0]]
                kf = KalmanFilter(
                    transition_matrices=transition_matrix,
                    observation_matrices=observation_matrix,
                    initial_state_mean=[value['utm_x'], value['utm_y'], 0, 0],
                    observation_covariance=1e3 * np.eye(2),
                    transition_covariance=1e2 * np.eye(4)
                )
                state_mean = np.array([value['utm_x'], value['utm_y'], 0, 0])
                state_covariance = kf.transition_covariance
            else:
                # 从状态取出上一次的KF对象和状态
                kf = kf_state_data['kf']
                state_mean = kf_state_data['state_mean']
                state_covariance = kf_state_data['state_covariance']

                # 更新 transition_matrix 中的 dt
                kf.transition_matrices = [[1, 0, dt, 0],
                                          [0, 1, 0, dt],
                                          [0, 0, 1, 0],
                                          [0, 0, 0, 1]]

                # 对当前观测点进行滤波更新
                (state_mean, state_covariance) = kf.filter_update(
                    filtered_state_mean=state_mean,
                    filtered_state_covariance=state_covariance,
                    observation=[value['utm_x'], value['utm_y']]
                )

        # 保存更新后的状态回Flink的状态中
        self.kf_state.update({
            'kf': kf,
            'state_mean': state_mean,
            'state_covariance': state_covariance
        })
        self.last_timestamp_state.update(current_timestamp)

        # 将滤波后的位置写入输出记录中
        value['fused_x'] = float(state_mean[0])
        value['fused_y'] = float(state_mean[1])

        # 使用 Transformer 将 UTM 坐标转换为经纬度
        fused_longitude,fused_latitude = self.utm_to_latlon_transformer.transform(
            state_mean[0], state_mean[1])
        value['fused_latitude'] = fused_latitude
        value['fused_longitude'] = fused_longitude

        # 输出更新后的记录
        yield value


from pyflink.datastream import WindowedStream
#from pyflink.datastream import Time
from pyflink.datastream import MapFunction
from pyflink.common import Types

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

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
# #########################################################################################
# 注意：已经在flink-conf.yaml文件中添加了RocksDB状态后端为/savepoints/checkpoints/重启策略 配置
# 代码中可以不用显式配置
# #########################################################################################



# env.enable_checkpointing(10000)  # 设置为每 10 秒钟进行一次 checkpoint
# checkpoint_path = "file:///C:/coding/data/flink/checkpoints/"
# env.get_checkpoint_config().set_checkpoint_storage(CheckpointStorage(checkpoint_path))


# 读取kafka数据
# 'enable.auto.commit': 'false'  # 禁止自动提交偏移量
# 从最新的消息开始读取 'latest'早的消息开始读取 'earliest'
# 消费者组： 测试: 'test_jm_group', 正式: dws_jm_group
jm_consumer = FlinkKafkaConsumer(
    topics='dwd_kafka_jm_drone',
    properties={'bootstrap.servers': '192.168.200.133:9092',
                'group.id': 'dws_jm_group',
                'auto_offset_reset': 'earliest',
                },
    deserialization_schema=SimpleStringSchema())
# checkpoints时不提交便宜量
# jm_consumer.set_commit_offsets_on_checkpoints(False)
jm_stream = env.add_source(jm_consumer)
# jm_stream.print("打印 jm_stream: ")

# 水位线的延迟最大延时时间
watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_millis(50)) \
    .with_timestamp_assigner(CustomTimestampAssigner()) 
watermark_stream = jm_stream.assign_timestamps_and_watermarks(watermark_strategy)
# watermark_stream.print("打印 watermark_stream: ")


# 将窗口结果映射到目标结构
# transformed_stream_name = ['fusion_id','timestamp','utm_x','utm_y','latitude','longitude','speed','height','heading','model','drone_uuid']
# transformed_stream_value = [Types.STRING(), Types.LONG(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.STRING(), Types.STRING()]
transformed_stream = watermark_stream.map(TransformResultToTrack(), output_type=Types.MAP(Types.STRING(), Types.PICKLED_BYTE_ARRAY()))
# transformed_stream.print("打印 transformed_stream: ")


# 接下来可以将 transformed_stream 传递给 TrackStateEstimation
keyed_stream = transformed_stream.key_by(lambda x: x['fusion_id'])
final_stream = keyed_stream.process(TrackStateEstimation())

# for element in final_stream.execute_and_collect(limit=10):
#     print(element)

# 反序列化，转字符串
converted_str_data = final_stream.map(lambda x: json.dumps({
    key: (
        json.loads(value.decode('utf-8')) if isinstance(value, bytearray) else value
    ) if isinstance(value, bytearray) else value
    for key, value in x.items()
}, ensure_ascii=False)) \
# .map(lambda x: x.replace('"', '\\"')) \
# .map(lambda x: "\"" + x + "\"")
# converted_str_data.print(f"打印 converted_str_data: {converted_str_data}")


# # flink开启事务写入kafka-还有问题没有解决
# # 事务id前缀set_transactional_id_prefix，自动使用Kafka的事务机制（EXACTLY_ONCE）
# # 交付保障一次性 DeliveryGuarantee.EXACTLY_ONCE： ack 一次发送、一次确认
# topic = "dws_kafka_jm_drone"
# record_serializer = KafkaRecordSerializationSchema.builder() \
#     .set_topic(topic) \
#     .set_value_serialization_schema(SimpleStringSchema()) \
#     .build()
# kafka_sink = KafkaSink.builder() \
#     .set_bootstrap_servers("192.168.200.133:9092") \
#     .set_record_serializer(record_serializer) \
#     .build()
# ## 将数据写入 Kafka
# converted_str_data.sink_to(kafka_sink)

############################################################################
# 发送数据后端接口
target_url = f'http://192.168.200.138:8088/focusHz/fusion/push'

############################################################################


# 注意这里直接使用pythonkafka接口写kafka
topic = "dws_kafka_jm_drone"
from kafka import KafkaProducer
from pyflink.datastream.functions import MapFunction
import requests
import sys
class KafkaProducerFunction(MapFunction):
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
converted_str_data.map(KafkaProducerFunction())


env.execute("low-altitude-dws_kafka_jm_drone")


