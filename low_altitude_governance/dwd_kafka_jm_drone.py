#from pyflink.common import Configuration
#config = Configuration()
#config.set_string("pipeline.jars", "file:///D:/flink-1.18.1-bin-scala_2.12/lib/flink-connector-kafka-3.1.0-1.18.jar")
#env.get_config().set_global_job_parameters(config)
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
from pyflink.common.typeinfo import Types
from pyflink.common import Row

class SignalData(object):
    def __init__(self, data):
        self.data = data

signal_data_type_info = Types.PICKLED_BYTE_ARRAY()

def preprocess_data(record, transformer):
    record['timestamp'] = record['timestamp']
    # 坐标转换
    utm_x, utm_y = transformer.transform(record['longitude'], record['latitude'])
    record['utm_x'] = utm_x
    record['utm_y'] = utm_y

    # print(f"打印 preprocess_data :  {record}")
    return record

class SignalProcessWindowFunction(ProcessWindowFunction):
    def __init__(self, slide):
        super().__init__()
        self.slide = slide  # 显式传递滑动步长
        # self.prev_record_state = None
        # self.signal_first_state = None

    def open(self, runtime_context: RuntimeContext):
        self.transformer = Transformer.from_crs("epsg:4326", "epsg:32651", always_xy=True)
        # 存储上一条记录
        prev_record_state_name = ['timestamp','utm_x','utm_y','speed','heading','driverlongitude','driverlatitude']
        prev_record_state_type = [Types.LONG(),Types.DOUBLE(),Types.DOUBLE(),Types.DOUBLE(),Types.DOUBLE(),Types.DOUBLE(),Types.DOUBLE()]
        self.prev_record_state = runtime_context.get_state(
            ValueStateDescriptor("SignalProcess_prev_record_state", Types.ROW_NAMED(prev_record_state_name,prev_record_state_type))
        )
        # 记录相同的signal_id的记录最早的 ： latitude/longitude/height/drone_uuid/lasting_time/timestamp
        signal_first_state_name = ['latitude','longitude','height','drone_uuid','lasting_time','timestamp','batch_num']
        signal_first_state_type = [Types.DOUBLE(),Types.DOUBLE(),Types.DOUBLE(),Types.STRING(),Types.INT(),Types.LONG(),Types.STRING()]
        self.signal_first_state = runtime_context.get_state(
            ValueStateDescriptor("SignalProcess_first_state", Types.ROW_NAMED(signal_first_state_name,signal_first_state_type))
        )

    # 方法描述：
    # 1.定义列表: records:记录所有数据用于计算特征/return_records:返回滑动窗口未交叉计算部分数据 /
    # 2.根据窗口状态获取窗口内为交叉计算的开始和结束时间: win_start_time, win_start_time + self.slide
    # 3.遍历数据
    #   3.1.根据保存状态计算坐标/速度/航向: utm_x, utm_y, speed, heading
    #   3.2.根据保存状态更新相同的signal_id/batch_num最早的经纬度和高度信息
    #   3.3.记录窗口返回数据: return_records
    #   3.4.根据时间戳比或者持续时间是0，更新最小记录的状态
    # 4.添加窗口内排序第一条记录的序号 :return_records
    # 5.对所有数据计算特征，结果给所有输出数据: feature
    def process(self, key, context, elements):
        signal_id = key
        records = []

        # 窗口的时间是按照左闭右开的方式计算的
        # 由于滑动窗口会有数据重复，所以要排掉滑动到下一个窗口的数据，只返回步长内的数据
        # 步长内的数据 ：win_start_time <= 事件时间 < win_start_time + slide
        return_records = []

        # 获取窗口的开始和结束时间
        win_start_time = context.window().start
        # print(f"win_start_time: {win_start_time}")
        win_end_time = context.window().end
        # print(f"win_end_time: {win_end_time}")
        # 测试数据集滑动步长
        # print(f"win_end_time: {win_end_time},win_start_time: {win_start_time},滑动步长： {self.slide} ,win_end_time - win_start_time = {win_end_time - win_start_time}")
        # win_end_time: 1733476475000,win_start_time: 1733476470000,滑动步长： 1000 ms ,win_end_time - win_start_time = 5000
        
        # 拼接当前窗口开始时间和结束时间作为当前数据批次的标识 ： 1733476475000-1733476470000
        # 拼接 win_start_time  -  win_start_time + slide
        curr_batch_time = str(win_start_time) + "-" + str(win_start_time + self.slide)
        # print(f"win_start_time: {win_start_time}")
        # print(f"slide: {self.slide}")
        # print(f"win_start_time + self.slide: {win_start_time + self.slide}")
        # print(f"curr_batch_time: {curr_batch_time}")

        # 对窗口内的记录按时间排序
        sorted_elements = sorted(elements, key=lambda x: x['timestamp'])
        
        for record in sorted_elements:
            # print(f"打印 record :  {record}")
            # 添加当前窗口返回数据的开始和结束时间： win_start_time, win_start_time + self.slide
            record['curr_batch_time'] = curr_batch_time

            # 获取上一次的prev_record信息（若有）
            prev_record = self.prev_record_state.value()
            # print(f"打印 prev_recordxxx: {prev_record}")
            # 最早一条记录
            signal_first = self.signal_first_state.value()
            # print(f"打印 signal_first00 : {signal_first}")

            record = preprocess_data(record, self.transformer)
            if prev_record is not None:
                delta_time = record['timestamp'] - prev_record['timestamp']
                delta_x = record['utm_x'] - prev_record['utm_x']
                delta_y = record['utm_y'] - prev_record['utm_y']
                if delta_time > 0:
                    calculated_speed = np.sqrt(delta_x**2 + delta_y**2) / delta_time
                    record['speed'] = calculated_speed
                    calculated_heading = np.degrees(np.arctan2(delta_y, delta_x)) % 360
                    record['heading'] = calculated_heading
                else:
                    record['speed'] = prev_record['speed']
                    record['heading'] = prev_record['heading']
            else:
                record['speed'] = record.get('speed', 0)
                record['heading'] = record.get('heading', 0)

            if signal_first is not None:
                # 更新相同的drone_uuid/batch_num最早的经纬度和高度信息
                # print(f"取 signal_first :  {signal_first}")
                record['start_latitude'] = signal_first['latitude']
                record['start_longitude'] = signal_first['longitude']
                record['start_height'] = signal_first['height']
            else:
                record['start_latitude'] = 0.0
                record['start_longitude'] = 0.0
                record['start_height'] = 0.0

            # 计算特征值需要所有的记录
            records.append(record)

            # 更新前一个记录计算位移差和速度,飞手的经纬度
            update_prev_record = Row(
                timestamp=record['timestamp'],
                utm_x=record['utm_x'],
                utm_y=record['utm_y'],
                speed=record['speed'],
                heading=record['heading'],
                driverlongitude=record['driverlongitude'],
                driverlatitude=record['driverlatitude']
                )
            self.prev_record_state.update(update_prev_record)

            # 返回滑动窗口没有被下一个窗口计算的数据，返回前1s数据
            # print(f"打印 record['timestamp']: {record['timestamp'] * 1000}")
            if record['timestamp'] * 1000 >= win_start_time and record['timestamp'] * 1000 < win_start_time + self.slide :
                # print(f"打印 record['timestamp']: {record['timestamp']}")
                return_records.append(record)
        
            # 相同的drone_uuid/batch_num 可能有很多数据，取最早的一条经纬度和高度赋值给其他记录，下游算法使用
            # 更新相同的drone_uuid/batch_num最早的记录 ： latitude/longitude/height/drone_uuid/lasting_time/timestamp
            if signal_first is not None :
                # 如果当前记录的时间戳比上一个记录小或者当前记录的持续时间是0，则把当前记录最小值更新状态
                if record['timestamp'] < signal_first['timestamp'] or record['lasting_time']  == 0:
                    # 更新数据
                    signal_first = Row(latitude=record['latitude'],longitude=record['longitude'],height=record['height'],drone_uuid=record['drone_uuid'],lasting_time=record['lasting_time'],timestamp=record['timestamp'],batch_num=record['batch_num'])
                    self.signal_first_state.update(signal_first)
                    # print(f"打印 signal_first: {signal_first}")
            else:
                signal_first_org=Row(latitude=record['latitude'],longitude=record['longitude'],height=record['height'],drone_uuid=record['drone_uuid'],lasting_time=record['lasting_time'],timestamp=record['timestamp'],batch_num=record['batch_num'])
                self.signal_first_state.update(signal_first_org)
                # print(f"打印 signal_first_org: {signal_first_org}")

        # 这里按照drone_uuid,batch_num 分组，按照timestamp 升序排序添加序号
        # 按 drone_uuid,batch_num 分组
        sorted_records = sorted(return_records, key=itemgetter('drone_uuid','batch_num', 'timestamp'))
        grouped_records = groupby(sorted_records, key=itemgetter('drone_uuid','batch_num'))
        # 给每组分配序号
        rank_result = []
        for (drone_uuid, batch_num), group in grouped_records:
            group_list = sorted(group, key=itemgetter('timestamp'))  # 按 timestamp 排序
            for idx, record in enumerate(group_list, start=1):
                record_with_rank = record.copy()  # 防止修改原列表
                record_with_rank['curr_batch_first'] = idx  # 添加序号
                # print(f"打印 排序: {record_with_rank['signal_id']},{record_with_rank['batch_num']},{record_with_rank['lasting_time']},{record_with_rank['timestamp']},{record_with_rank['curr_batch_time']},{record_with_rank['curr_batch_first']}")
                rank_result.append(record_with_rank)

        # 提取特征
        feature = self.extract_signal_features(signal_id, records)

        # 构建输出结果
        # fusion字段在此阶段还没有最终计算，只能先留空或None，等待后面merge时计算
        # 此处不对fusion_***进行赋值，仅将已有字段输出，后面SimpleProcessWindowFunction会进行融合计算并更新fusion字段
        for r in rank_result:            # 设置pre_fusion字段
            r['feature'] = feature
            # print(f"打印 结束+++++++++++++++++++++++++ 有数据  +++++++++++++++++++++++++++++++")
            yield r
        # print(f"打印 结束+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

    def extract_signal_features(self, signal_id, records):
        latitudes = [float(r['latitude']) for r in records]
        longitudes = [float(r['longitude']) for r in records]
        times = [r['timestamp'] for r in records]
        speeds = [float(r.get('speed', 0)) for r in records]
        heights = [float(r.get('height', 0)) for r in records]

        lat_range = max(latitudes) - min(latitudes) if latitudes else 0
        lon_range = max(longitudes) - min(longitudes) if longitudes else 0
        time_range = int(max(times) - min(times)) if len(times) > 1 else 0
        avg_speed = sum(speeds) / len(speeds) if speeds else 0
        avg_height = sum(heights) / len(heights) if heights else 0

        feature = {
            'signal_id': signal_id,
            'lat_range': lat_range,
            'lon_range': lon_range,
            'time_range': time_range,
            'avg_speed': avg_speed,
            'avg_height': avg_height
        }
        return feature

def custom_parse_json(input_str):
    cleaned_str = input_str.replace(r"\u318d", "")
    def convert_to_python(match):
        value = match.group(0)
        if value.startswith('null'):
            return 'None'
        elif value.startswith('true'):
            return 'True'
        elif value.startswith('false'):
            return 'False'
        return value

    python_compatible_str = re.sub(
        r'null|true|false',
        convert_to_python,
        cleaned_str
    )
    python_compatible_str = re.sub(r'(?<!\\)"', "'", python_compatible_str)
    parsed_data = eval(python_compatible_str)
    return parsed_data

#1.json解析及异常处理
#2.取'drone' 键的数据
#3.拼接signal_id=drone_uuid + batch_num
#4.提取timestamp/event_time
def parse_json(json_str):
    try:
        fixed_json_str = json_str
        orig = json.loads(fixed_json_str)
        orig = custom_parse_json(orig)
    except json.JSONDecodeError as e:
        print(f"JSON Decode Error: {e}")
        raise ValueError("JSON 数据解析失败，请检查输入格式。")

    if "drone" not in orig:
        raise KeyError("JSON 数据缺少 'drone' 键。")

    try:
        # 站点id
        station_id = 0
        station_id = orig["station_id"]
        # 将飞手数据合并到无人机数据中
        # 类型（0-遥控器1-无人机）
        for drone in orig["drone"]:
            # print(f"打印 drone数据：{drone}")
            if drone['type'] == 0:
                remote_data = drone
            elif drone['type'] == 1:
                data = drone
        data['driverlongitude'] = remote_data['longitude']
        data['driverlatitude'] = remote_data['latitude']
        data['station_id'] = station_id
        # print(f"打印 data数据：{data}")
        
        # data = list(filter(lambda x: x["type"] == 1, orig["drone"]))[0]
    except (KeyError, IndexError) as e:
        print(f"Error while filtering drone data: {e}")
        raise ValueError("无法找到满足条件的 'drone' 数据。")

    # 注意，这里的signal_id是drone_uuid和batch_num的拼接，表示当前时间的无人机的唯一id
    data['signal_id'] = data['drone_uuid'] + '_' + data['batch_num']

    match = re.search(r'\d{14}', data["batch_num"][5:])
    if not match:
        raise ValueError("Invalid batch_num format, no timestamp found")
    time_str = match.group(0)
    dt = datetime.strptime(time_str, "%Y%m%d%H%M%S")
    timestamp = dt.timestamp()

    data['timestamp'] = int(timestamp + data["lasting_time"])
    data['event_time'] = int(timestamp + data["lasting_time"])

    for key in ["frequency", "height", "speed","latitude","longitude"]:
        if key in data:
            data[key] = float(data[key])
    
    # print(f"打印 输出数据：{type(data)} ：{data}")
    return data

# 类说明 ：SimpleProcessWindowFunction
# 1.定义fusion状态
# 2.对窗口数据根据drone_uuid，batch_num分组按照timestamp升序取第一条用于数据融合 ： data_list
# 3.提取特征列表 ： signal_features
# 4.对窗口内第一条记录数据(data_list)进行融合操作，返回可以融合的设备id列表： merged_signal_map
#     4.1 根据输入数据长度生成一个N*N的对称矩阵 ： matrix
#     4.2 对矩阵中的元素做相似度计算 ： calculate_similarity
#     4.3 整合初始聚类和优化逻辑，生成最终聚类：optimize_partitioning 
#     4.4 初始化信号聚类，通过 DFS 给每个信号分配初始聚类： initialize_partitioning
#     4.5 计算当前聚类的平均相似度，衡量当前分配的质量： average_subgraph_weights
#     4.6 通过贪心算法优化初始聚类，提高聚类质量： greedy_partitioning
#     4.7 计算当前聚类的平均相似度，衡量当前分配的质量： average_subgraph_weights
#     4.8 聚类映射输出，键是聚类的索引，值是属于该聚类的信号 ID 列表： '0': [1, 2],
# 5.对输入数据提取主键： signal_id_to_records
# 6.遍历融合后的设备id列表： merged_signal_map
#     6.1 与输入数据主键关联补充完整数据信息 ： candidate_records
#     6.2 从本地状态变量中提取前一次fusion信息赋值给candidate_records的前值字段： last_fusion_state
#     6.3 对candidate_records记录中的当前值与前值比较取时间较早的fusion信息当作结果数据
#         当前值和前值：
#         fusion_id/fusion_start_time/fusion_longitude/fusion_latitude/fusion_height
#         pre_fusion_id/pre_fusion_start_time/pre_fusion_longitude/pre_fusion_latitude/pre_fusion_height
#     6.4 根据最新的经纬度和高度hash生成： fusion_id
#     6.5 更新 last_fusion_state 状态变量
#     6.6 结果数据根据drone_uuid/batch_num关联赋值给所有输入数据
class SimpleProcessWindowFunction(ProcessWindowFunction):
    def open(self, runtime_context: RuntimeContext):
        last_fusion_state_name = ['drone_uuid','batch_num','fusion_id','fusion_start_time','fusion_longitude','fusion_latitude','fusion_height']
        last_fusion_state_type = [Types.STRING(),Types.STRING(),Types.STRING(),Types.STRING(),Types.DOUBLE(),Types.DOUBLE(),Types.DOUBLE()]
        # 存储上一条记录的fusion相关信息
        self.last_fusion_state = runtime_context.get_map_state(
            MapStateDescriptor("SimpleProcess_last_fusion_state", Types.STRING(), Types.ROW_NAMED(last_fusion_state_name,last_fusion_state_type))
        )

    def process(self, context, elements):
        # print(f"type(elements): {type(elements)}")  # type(elements): <class 'list'> 字典列表
        # print(f"elements: {elements}")

        # 获取上一次保存的fusion信息
        # 遍历 valid_candidates列表drone_uuid和batch_num是否在fusion 状态中有存储
        for cr in elements: 
            # print(f"cr: {cr}")
            pre_key = cr.get('drone_uuid') + '_' + cr.get('batch_num') if cr.get('drone_uuid') and cr.get('batch_num') else None
            # pre_key = cr['drone_uuid'] + '_' + cr['batch_num']
            if self.last_fusion_state.contains(pre_key):
                fusion_info = self.last_fusion_state.get(pre_key)
                cr['pre_fusion_id'] = fusion_info['fusion_id']
                cr['pre_fusion_start_time'] = fusion_info['fusion_start_time']
                cr['pre_fusion_longitude'] = fusion_info['fusion_longitude']
                cr['pre_fusion_latitude'] = fusion_info['fusion_latitude']
                cr['pre_fusion_height'] = fusion_info['fusion_height']
            # print(f"cr: {cr}")

        # elements 是前一个窗口函数输出的记录列表，标记了当时窗口内的第一条，但是并不代表是设备的第一条
        # 对 elements 进行排序，确保按 drone_uuid 和 batch_num 排序且唯一，给后面的算法使用
        # 按 drone_uuid/batch_num/timestamp 分组排序，确保组内有序
        sorted_elements = sorted(elements, key=lambda x: (x['drone_uuid'], x['batch_num'], x['timestamp']))
        data_list = []
        # 按照(drone_uuid, batch_num) 分组
        for (drone_uuid, batch_num), group in groupby(sorted_elements, key=lambda x: (x['drone_uuid'], x['batch_num'])):
            # 取每组的第一条记录
            first_record = next(group)
            data_list.append(first_record)
        # print(f"type(data_list): {type(data_list)}")  # type(elements): <class 'list'> 字典列表
        # print(f"data_list: {data_list}")
        # data_list = list(elements)
        # data_list中包含通过SignalProcessWindowFunction传递过来的记录，
        # 这些记录已经包含pre_fusion_xxx字段和feature，但fusion字段尚未最终确定。

        # 提取各信号的特征列表
        signal_features = [r['feature'] for r in data_list if 'feature' in r]

        # 针对同一窗口内所有记录进行merge操作
        merged_signal_map = self.merge_signals(signal_features, threshold=0.8)
        # print(f"merged_signal_map: {merged_signal_map}")
        # merged_signal_map: {'0': ['F6Z9C245C003SWLG', 'F6Z9C245C003Swef']}

        # 根据merged_signal_map对聚类内的记录计算fusion_start_time、fusion坐标和fusion_id
        # 首先构建signal_id到record的映射，方便后续按cluster更新
        # data_list 是前一个窗口函数输出的记录列表，标记了当时窗口内的第一条，但是并不代表是设备的第一条
        signal_id_to_records = {}
        for r in data_list:
            sid = r['signal_id']
            if sid not in signal_id_to_records:
                signal_id_to_records[sid] = []
            signal_id_to_records[sid].append(r)
            # print(f"signal_id_to_records: {signal_id_to_records}")
            # signal_id_to_records: {'F6Z9C245C003SWLG': [{'utm_x': -422873.52832781605, 'detect_type': 1, 'fusion_start_time': None, 'drone_uuid': 'F6Z9C245C003SWLG', 'scanID': [{'id': '92025'}], 'latitude': 22.520713013248166, 'start_height': 0.0, 'type': 1, 'fusion_latitude': None, 'speed': 7.5, 'frequency': 5.7250000000000005, 'utm_y': 2518220.7499508113, 'curr_batch_time': '1733468350000-1733468360000', 'feature': {'signal_id': 'F6Z9C245C003SWLG', 'lat_range': 0.0, 'lon_range': 0.0, 'time_range': 0, 'avg_speed': 7.5, 'avg_height': 124.0}, 'fusion_height': None, 'start_latitude': 0.0, 'pre_fusion_longitude': None, 'pre_fusion_latitude': None, 'model': 'DJI Mini 4 Pro', 'pre_fusion_id': None, 'pre_fusion_height': None, 'height': 124.0, 'longitude': 114.0520450097575, 'timestamp': 1733468357, 'fusion_id': None, 'intrusion_start_time': '92025ㆍ20241206145845781', 'heading': 0, 'signal_id': 'F6Z9C245C003SWLG', 'batch_num': '92025ㆍ202412061458457811', 'start_longitude': 0.0, 'pre_fusion_start_time': None, 'fusion_longitude': None, 'lasting_time': 32, 'curr_batch_first': 1, 'event_time': 1733468357}]}


        # merged_signal_map结构: {cluster_index: [signal_id1, signal_id2, ...]}
        # 对每个cluster:
        # 1. 找出该cluster中各signal_id对应记录的最小pre_fusion_start_time
        # 2. 使用这个最小pre_fusion_start_time对应的pre_fusion_longitude等信息确定fusion坐标
        # 3. 使用这些信息生成fusion_id
        for cluster_id, signal_ids in merged_signal_map.items():
            # print(f"cluster_id: {cluster_id}, signal_ids: {signal_ids}, merged_signal_map.items(): {merged_signal_map.items()}")
            # cluster_id: 0, signal_ids: ['F6Z9C245C003SWLG'], merged_signal_map.items(): dict_items([('0', ['F6Z9C245C003SWLG'])])

            # 找出该cluster所有record的pre_fusion_start_time最小值及其对应信息
            candidate_records = []
            for sid in signal_ids:
                if sid in signal_id_to_records:
                    candidate_records.extend(signal_id_to_records[sid])
 
            # 筛选有有效pre_fusion_start_time的记录
            valid_candidates = [cr for cr in candidate_records if cr.get('pre_fusion_start_time') is not None]
            if not valid_candidates:
                # 若没有任何pre_fusion_start_time可用，则跳过或默认赋值
                # 这里给默认值，valid_start_time是字符串类型需要给最大的字符串，其他是浮点数
                valid_start_time = 'ZZZZZㆍZZZZZZZZZZZZZZZZZ'    # intrusion_start_time：'92025ㆍ20241206163120171' 
                valid_longitude = 0.0
                valid_latitude = 0.0
                valid_height = 0.0
            else:
                # 找最小的pre_fusion_start_time
                valid_candidates.sort(key=lambda x: x['pre_fusion_start_time'])#+所有被融合目标的开始时间
                # 列表排序取第一个字典数据
                valid_record = valid_candidates[0]
                valid_start_time = valid_record['pre_fusion_start_time'] #使用最早时间对应的经纬度和高度
                valid_longitude = valid_record['pre_fusion_longitude']
                valid_latitude = valid_record['pre_fusion_latitude']
                valid_height = valid_record['pre_fusion_height']

            # print(f"candidate_records: { candidate_records}")
            # print(f"len(candidate_records) > 0: { len(candidate_records) > 0}") # len(candidate_records) > 0: True
            # print(f"if not candidate_records: { not candidate_records}")   # if not candidate_records: False
            if len(candidate_records) > 0 :
                # 找最小的pre_fusion_start_time
                # print(f"len(candidate_records) > 0: { len(candidate_records) > 0}")
                candidate_records.sort(key=lambda x: x['intrusion_start_time'])#+所有被融合目标的开始时间
                curr_record = candidate_records[0]
                curr_start_time = curr_record['intrusion_start_time'] #使用最早时间对应的经纬度和高度
                curr_longitude = curr_record['start_longitude']
                curr_latitude = curr_record['start_latitude']
                curr_height = curr_record['start_height']

            if curr_start_time < valid_start_time:
                fusion_start_time = curr_start_time
                fusion_longitude = curr_longitude
                fusion_latitude = curr_latitude
                fusion_height = curr_height
            else:
                fusion_start_time = valid_start_time
                fusion_longitude = valid_longitude
                fusion_latitude = valid_latitude
                fusion_height = valid_height

            # 根据fusion_start_time、fusion_longitude、fusion_latitude、fusion_height生成fusion_id
            fusion_id_source = f"{fusion_start_time}_{fusion_longitude}_{fusion_latitude}_{fusion_height}"
            fusion_id = hashlib.md5(fusion_id_source.encode('utf-8')).hexdigest()

            updated_records = []
            # 更新cluster内所有记录的fusion字段
            for sid in signal_ids:
                if sid in signal_id_to_records:
                    for rec in signal_id_to_records[sid]:
                        rec['fusion_id'] = fusion_id
                        rec['fusion_start_time'] = fusion_start_time
                        rec['fusion_longitude'] = fusion_longitude
                        rec['fusion_latitude'] = fusion_latitude
                        rec['fusion_height'] = fusion_height
                        updated_records.append(rec)

                        # 更新状态
                        map_key = rec['drone_uuid'] + '_' + rec['batch_num']
                        map_vlu = Row(drone_uuid=rec['drone_uuid'],batch_num=rec['batch_num'],fusion_id=rec['fusion_id'],
                                      fusion_start_time=rec['fusion_start_time'],fusion_longitude=rec['fusion_longitude'],
                                      fusion_latitude=rec['fusion_latitude'],fusion_height=rec['fusion_height'])
                        # print(f"打印 map_vlu : {map_vlu}")
                        self.last_fusion_state.put(map_key, map_vlu)

            # 按 drone_uuid 和 batch_num 关联将updated_records的数据全部更新到elements列表中
            updated_records_dict = {
                (rec['drone_uuid'], rec['batch_num']): rec for rec in updated_records
            }
            for element in elements:
                key = (element['drone_uuid'], element['batch_num'])
                if key in updated_records_dict:
                    updated_record = updated_records_dict[key]
                    element['fusion_id'] = updated_record['fusion_id']
                    element['fusion_start_time'] = updated_record['fusion_start_time']
                    element['fusion_longitude'] = updated_record['fusion_longitude']
                    element['fusion_latitude'] = updated_record['fusion_latitude']
                    element['fusion_height'] = updated_record['fusion_height']

        # 将更新后的记录全部yield出去
        for r in elements:
            yield r

    def merge_signals(self, signal_features, threshold=0.2):
        import numpy as np
        N = len(signal_features)
        if N == 0:
            return {}

        matrix = np.zeros((N, N))
        for i in range(N):
            matrix[i][i] = 1.0

        # 计算相似度矩阵
        for i in range(N):
            for j in range(i + 1, N):
                sim = self.calculate_similarity(signal_features[i], signal_features[j])
                matrix[i][j] = sim
                matrix[j][i] = sim

        def dfs(node, visited, clusters, cluster_id, threshold):
            visited[node] = True
            clusters[node] = cluster_id
            for neighbor in range(N):
                if not visited[neighbor] and matrix[node][neighbor] > threshold:
                    dfs(neighbor, visited, clusters, cluster_id, threshold)

        def initialize_partitioning(threshold):
            visited = [False] * N
            clusters = [-1] * N
            cluster_id = 0
            for node in range(N):
                if not visited[node]:
                    dfs(node, visited, clusters, cluster_id, threshold)
                    cluster_id += 1
            return clusters

        def calculate_avg_weight(cluster_data):
            total_weight = 0
            edge_count = 0
            for i in range(len(cluster_data)):
                for j in range(i + 1, len(cluster_data)):
                    node_i, node_j = cluster_data[i], cluster_data[j]
                    total_weight += matrix[node_i][node_j]
                    edge_count += 1
            return total_weight / edge_count if edge_count > 0 else 0

        def average_subgraph_weights(clusters):
            cluster_avg_weights = []
            unique_clusters = set(clusters)
            for cluster_id in unique_clusters:
                cluster_nodes = [idx for idx, cid in enumerate(clusters) if cid == cluster_id]
                avg_weight = calculate_avg_weight(cluster_nodes)
                cluster_avg_weights.append(avg_weight)
            return np.mean(cluster_avg_weights) if cluster_avg_weights else 0

        def greedy_partitioning(clusters, threshold, max_iter=100):
            for _ in range(max_iter):
                improved = False
                for i in range(N):
                    current_cluster = clusters[i]
                    best_cluster = current_cluster
                    best_avg_weight = average_subgraph_weights(clusters)

                    for cluster_id in set(clusters):
                        if cluster_id != current_cluster:
                            members_in_cluster = [idx for idx, c in enumerate(clusters) if c == cluster_id]
                            similarities = [matrix[i][j] for j in members_in_cluster]
                            if any(sim > threshold for sim in similarities):
                                new_clusters = clusters.copy()
                                new_clusters[i] = cluster_id
                                new_avg_weight = average_subgraph_weights(new_clusters)

                                if new_avg_weight > best_avg_weight:
                                    best_avg_weight = new_avg_weight
                                    best_cluster = cluster_id
                                    improved = True

                    clusters[i] = best_cluster
                if not improved:
                    break
            return clusters

        def optimize_partitioning(threshold):
            clusters = initialize_partitioning(threshold)
            best_partition = clusters.copy()
            best_avg_weight = average_subgraph_weights(clusters)

            curr_clusters = clusters.copy()
            max_iter = 100
            for iteration in range(max_iter):
                curr_clusters = greedy_partitioning(curr_clusters, threshold)
                avg_weight = average_subgraph_weights(curr_clusters)

                if avg_weight > best_avg_weight:
                    best_avg_weight = avg_weight
                    best_partition = curr_clusters.copy()
                else:
                    break
            return best_partition

        best_partition = optimize_partitioning(threshold)

        merged_signal_map = {}
        clusters = {}
        for idx, cluster_id in enumerate(best_partition):
            if cluster_id not in clusters:
                clusters[cluster_id] = []
            clusters[cluster_id].append(signal_features[idx]['signal_id'])

        for idx, (cluster_id, signal_ids) in enumerate(clusters.items()):
            merged_signal_map[str(idx)] = signal_ids
        return merged_signal_map

    def calculate_similarity(self, feature1, feature2):
        vector1 = np.array([
            feature1['lat_range'],
            feature1['lon_range'],
            feature1['time_range'],
            feature1['avg_speed'],
            feature1['avg_height']
        ])
        vector2 = np.array([
            feature2['lat_range'],
            feature2['lon_range'],
            feature2['time_range'],
            feature2['avg_speed'],
            feature2['avg_height']
        ])
        distance = euclidean(vector1, vector2)
        similarity = 1 / (1 + distance)
        return similarity


from pyflink.common import Time
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.common.watermark_strategy import TimestampAssigner

class CustomTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, element, record_timestamp):
        return int(element['timestamp'] * 1000)

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
# #########################################################################################
# 注意：已经在flink-conf.yaml文件中添加了RocksDB状态后端为/savepoints/checkpoints/重启策略 配置
# 代码中可以不用显式配置
# #########################################################################################

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.checkpoint_config import CheckpointConfig
from pyflink.datastream.checkpoint_config import CheckpointStorage
from pyflink.common import Configuration

# env.enable_checkpointing(10000)  # 设置为每 10 秒钟进行一次 checkpoint
# checkpoint_path = "file:///C:/coding/data/flink/checkpoints/"
# env.get_checkpoint_config().set_checkpoint_storage(CheckpointStorage(checkpoint_path))


# 读取kafka数据
# 'enable.auto.commit': 'false'  # 禁止自动提交偏移量
# 从最新的消息开始读取 'latest'早的消息开始读取 'earliest'
# 消费者组： 测试: 'test_jm_group', 正式： dwd_jm_group
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
    SignalProcessWindowFunction(slide = slide_time.to_milliseconds()),
    output_type=Types.MAP(Types.STRING(), Types.PICKLED_BYTE_ARRAY())
)
# preprocessed_stream.print("打印 preprocessed_stream: ")

result_stream = preprocessed_stream.window_all(SlidingEventTimeWindows.of(
    Time.seconds(1),
    Time.seconds(1)
)).process(
    SimpleProcessWindowFunction(),
    # 输出记录同样是map结构，每条记录中包含所有字段
    output_type=Types.MAP(Types.STRING(), Types.PICKLED_BYTE_ARRAY())
)
# result_stream.print("打印 result_stream: ")


# # 打印10条数据
# import pickle
# for element in result_stream.execute_and_collect(limit=100):
#     # 解码每个字段
#     decoded_element = {key: pickle.loads(value) if isinstance(value, bytearray) else value
#                        for key, value in element.items()}
#     print(decoded_element)


converted_str_data = result_stream.map(lambda x: json.dumps({
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
# topic = "dwd_kafka_jm_drone"
# record_serializer = KafkaRecordSerializationSchema.builder() \
#     .set_topic(topic) \
#     .set_value_serialization_schema(SimpleStringSchema()) \
#     .build()
# kafka_sink = KafkaSink.builder() \
#     .set_bootstrap_servers("192.168.200.133:9092") \
#     .set_record_serializer(record_serializer) \
#     .build()
# ## 将数据写入 Kafka
# # converted_str_data.sink_to(kafka_sink)



# 注意这里直接使用pythonkafka接口写kafka
topic = "dwd_kafka_jm_drone"
from kafka import KafkaProducer # type: ignore
from pyflink.datastream.functions import MapFunction
class KafkaProducerFunction(MapFunction):
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
converted_str_data.map(KafkaProducerFunction())




env.execute("low-altitude-dwd_kafka_jm_drone")


