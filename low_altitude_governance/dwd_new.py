import json
import re
from datetime import datetime
import numpy as np
from scipy.spatial.distance import euclidean
from pyproj import Transformer
import hashlib
from itertools import groupby
from operator import itemgetter
import pandas as pd


# loads后不需要该函数
def custom_parse_json(input_str):
    cleaned_str = input_str.replace(r"\u318d", "")  # 替换unicode字符u318d 即‘ㆍ’

    def convert_to_python(match):
        value = match.group(0)  # 获取整个匹配的字符串
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
    python_compatible_str = re.sub(r'(?<!\\)"', "'", python_compatible_str)  # 匹配的是前面没有反斜杠转义的双引号
    parsed_data = eval(python_compatible_str)  # 将字符串转化为Py对象
    return parsed_data


def parse_dict_record(drone_data):
    drone_data['longitude'] = drone_data['takeoffLongitude']
    drone_data['latitude'] = drone_data['takeoffLatitude']
    drone_data['signal_id'] = drone_data['drone_uuid'] + '_' + drone_data['batch_num']

    match = re.search(r'\d{14}', drone_data["batch_num"][12:])  # 从 batch_num 字符串的第 13 个字符开始查找 14 位数字（精确到秒的时间）
    if not match:
        raise ValueError("Invalid batch_num format, no timestamp found")
    time_str = match.group(0)
    dt = datetime.strptime(time_str, "%Y%m%d%H%M%S")
    timestamp = dt.timestamp()

    drone_data['timestamp'] = int(timestamp + drone_data["lasting_time"])
    drone_data['event_time'] = int(timestamp + drone_data["lasting_time"])

    for key in ["frequency", "height", "speed", "latitude", "longitude"]:
        if key in drone_data:
            drone_data[key] = float(drone_data[key])

    return drone_data


def preprocess_data(record, transformer):  # 坐标转换
    record['timestamp'] = record['timestamp']
    utm_x, utm_y = transformer.transform(record['longitude'], record['latitude'])
    record['utm_x'] = utm_x
    record['utm_y'] = utm_y
    return record


def extract_signal_features(signal_id, records):
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
        'signal_id':signal_id,
        'lat_range':lat_range,
        'lon_range':lon_range,
        'time_range':time_range,
        'avg_speed':avg_speed,
        'avg_height':avg_height
    }
    return feature


# 对输入的 DataFrame 数据按 signal_id 进行分组处理，计算每个分组内数据的速度、航向，记录起始经纬度和高度信息，提取特征并将特征添加到每个记录中
def process_signal_data(df, transformer):
    # 按signal_id分组处理
    grouped = df.groupby('signal_id')

    results = []

    for signal_id, group in grouped:
        # 按时间排序
        sorted_group = group.sort_values('timestamp')

        # 计算速度和航向
        prev_record = None
        signal_first = None
        records = []

        for _, record in sorted_group.iterrows():
            record_dict = record.to_dict()
            record_dict = preprocess_data(record_dict, transformer)

            if prev_record is not None:
                delta_time = record_dict['timestamp'] - prev_record['timestamp']
                delta_x = record_dict['utm_x'] - prev_record['utm_x']
                delta_y = record_dict['utm_y'] - prev_record['utm_y']
                if delta_time > 0:
                    calculated_speed = np.sqrt(delta_x ** 2 + delta_y ** 2) / delta_time
                    record_dict['speed'] = calculated_speed
                    calculated_heading = np.degrees(np.arctan2(delta_y, delta_x)) % 360
                    record_dict['heading'] = calculated_heading
                else:
                    record_dict['speed'] = prev_record.get('speed', 0)
                    record_dict['heading'] = prev_record.get('heading', 0)
            else:
                record_dict['speed'] = record_dict.get('speed', 0)
                record_dict['heading'] = record_dict.get('heading', 0)

            if signal_first is not None:
                record_dict['start_latitude'] = signal_first['latitude']
                record_dict['start_longitude'] = signal_first['longitude']
                record_dict['start_height'] = signal_first['height']
            else:
                record_dict['start_latitude'] = 0.0
                record_dict['start_longitude'] = 0.0
                record_dict['start_height'] = 0.0

            # 更新signal_first
            if signal_first is None or record_dict['timestamp'] < signal_first['timestamp'] or record_dict[
                'lasting_time'] == 0:
                signal_first = {
                    'latitude':record_dict['latitude'],
                    'longitude':record_dict['longitude'],
                    'height':record_dict['height'],
                    'drone_uuid':record_dict['drone_uuid'],
                    'lasting_time':record_dict['lasting_time'],
                    'timestamp':record_dict['timestamp'],
                    'batch_num':record_dict['batch_num']
                }

            records.append(record_dict)
            prev_record = record_dict

        # 提取特征
        feature = extract_signal_features(signal_id, records)

        # 添加特征到所有记录
        for record in records:
            record['feature'] = feature
            results.append(record)

    return pd.DataFrame(results)


# 通过计算欧几里得距离对两个特征的相似度进行评分
def calculate_similarity(feature1, feature2):
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
    distance = euclidean(vector1, vector2)  # 欧几里得距离
    similarity = 1 / (1 + distance)  # 相似度得分
    return similarity


# 返回一个表示聚类结果的字典 merged_signal_map，其中键为聚类编号，值为属于该聚类的信号 ID 列表
def merge_signals(signal_features, threshold=0.2):
    N = len(signal_features)
    if N == 0:
        return {}

    matrix = np.zeros((N, N))
    for i in range(N):
        matrix[i][i] = 1.0

    # 计算相似度矩阵
    for i in range(N):
        for j in range(i + 1, N):
            sim = calculate_similarity(signal_features[i], signal_features[j])
            matrix[i][j] = sim
            matrix[j][i] = sim

    def dfs(node, visited, clusters, cluster_id, threshold):
        visited[node] = True
        clusters[node] = cluster_id
        for neighbor in range(N):
            if not visited[neighbor] and matrix[node][neighbor] > threshold:
                dfs(neighbor, visited, clusters, cluster_id, threshold)

    # 初始聚类，根据相似度将某个节点和与它的相似度大于threshold的节点聚类
    def initialize_partitioning(threshold):
        visited = [False] * N
        clusters = [-1] * N
        cluster_id = 0
        for node in range(N):
            if not visited[node]:
                dfs(node, visited, clusters, cluster_id, threshold)
                cluster_id += 1
        return clusters

    # 计算聚类平均权重：各节点对的相似度累加值除以节点对数
    def calculate_avg_weight(cluster_data):
        total_weight = 0
        edge_count = 0
        for i in range(len(cluster_data)):
            for j in range(i + 1, len(cluster_data)):
                node_i, node_j = cluster_data[i], cluster_data[j]
                total_weight += matrix[node_i][node_j]
                edge_count += 1
        return total_weight / edge_count if edge_count > 0 else 0

    # 先计算每个聚类的平均权重，存储在cluster_avg_weights中，再计算cluster_avg_weights的均值
    def average_subgraph_weights(clusters):
        cluster_avg_weights = []
        unique_clusters = set(clusters)
        for cluster_id in unique_clusters:
            cluster_nodes = [idx for idx, cid in enumerate(clusters) if cid == cluster_id]
            avg_weight = calculate_avg_weight(cluster_nodes)
            cluster_avg_weights.append(avg_weight)
        return np.mean(cluster_avg_weights) if cluster_avg_weights else 0

    # 对于每个节点，尝试将其移动到其他聚类中，计算移动后的所有聚类的平均权重，如果平均权重有所提高，则更新节点的聚类编号
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

    # 首先进行初始聚类，然后在每次迭代中调用 greedy_partitioning 函数对聚类结果进行优化，并更新最优权重和聚类结果
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


# 按时间窗口分组处理数据
# 在每个窗口内，将相似信号聚类
# 为每个聚类生成统一的融合ID和参数
# 更新所有相关记录的融合信息
# 维护融合状态以便后续处理
def apply_fusion(df):
    # 模拟Flink的窗口处理 - 这里简化处理，按时间窗口分组
    df['time_window'] = (df['timestamp'] // 1) * 1  # 1秒窗口

    results = []
    last_fusion_state = {}

    for _, window_group in df.groupby('time_window'):
        # 每个窗口内按(drone_uuid, batch_num)的组合分组，取第一条记录
        first_records = window_group.sort_values(['drone_uuid', 'batch_num', 'timestamp']).groupby(
            ['drone_uuid', 'batch_num']).first().reset_index()

        # 提取特征
        signal_features = [r['feature'] for _, r in first_records.iterrows() if 'feature' in r]

        # 合并信号
        merged_signal_map = merge_signals(signal_features, threshold=0.8)

        # 构建signal_id到record的映射
        signal_id_to_records = {}
        for _, r in first_records.iterrows():
            sid = r['signal_id']
            if sid not in signal_id_to_records:
                signal_id_to_records[sid] = []
            signal_id_to_records[sid].append(r.to_dict())

        # 处理每个cluster
        for cluster_id, signal_ids in merged_signal_map.items():
            candidate_records = []
            for sid in signal_ids:
                if sid in signal_id_to_records:
                    candidate_records.extend(signal_id_to_records[sid])

            # 筛选有有效pre_fusion_start_time的记录
            # valid_candidates = [cr for cr in candidate_records if cr.get('pre_fusion_start_time') is not None]
            #
            # if not valid_candidates:
            #     valid_start_time = 'ZZZZZㆍZZZZZZZZZZZZZZZZZ'
            #     valid_longitude = 0.0
            #     valid_latitude = 0.0
            #     valid_height = 0.0
            # else:
            #     valid_candidates.sort(key=lambda x:x['pre_fusion_start_time'])
            #     valid_record = valid_candidates[0]
            #     valid_start_time = valid_record['pre_fusion_start_time']
            #     valid_longitude = valid_record['pre_fusion_longitude']
            #     valid_latitude = valid_record['pre_fusion_latitude']
            #     valid_height = valid_record['pre_fusion_height']
            #
            # if len(candidate_records) > 0:
            #     candidate_records.sort(key=lambda x:x['intrusion_start_time'])
            #     curr_record = candidate_records[0]
            #     curr_start_time = curr_record['intrusion_start_time']
            #     curr_longitude = curr_record['start_longitude']
            #     curr_latitude = curr_record['start_latitude']
            #     curr_height = curr_record['start_height']
            # else:
            #     curr_start_time = valid_start_time
            #     curr_longitude = valid_longitude
            #     curr_latitude = valid_latitude
            #     curr_height = valid_height
            #
            # if curr_start_time < valid_start_time:
            #     fusion_start_time = curr_start_time
            #     fusion_longitude = curr_longitude
            #     fusion_latitude = curr_latitude
            #     fusion_height = curr_height
            # else:
            #     fusion_start_time = valid_start_time
            #     fusion_longitude = valid_longitude
            #     fusion_latitude = valid_latitude
            #     fusion_height = valid_height
            fusion_start_time = datetime.now()
            fusion_longitude = valid_longitude
            fusion_latitude = valid_latitude
            fusion_height = valid_height

            # 生成fusion_id
            fusion_id_source = f"{fusion_start_time}_{fusion_longitude}_{fusion_latitude}_{fusion_height}"
            fusion_id = hashlib.md5(fusion_id_source.encode('utf-8')).hexdigest()

            # 更新cluster内所有记录的fusion字段
            for sid in signal_ids:
                if sid in signal_id_to_records:
                    for rec in signal_id_to_records[sid]:
                        rec['fusion_id'] = fusion_id
                        rec['fusion_start_time'] = fusion_start_time
                        rec['fusion_longitude'] = fusion_longitude
                        rec['fusion_latitude'] = fusion_latitude
                        rec['fusion_height'] = fusion_height

                        # 更新状态
                        map_key = rec['drone_uuid'] + '_' + rec['batch_num']
                        last_fusion_state[map_key] = {
                            'drone_uuid':rec['drone_uuid'],
                            'batch_num':rec['batch_num'],
                            'fusion_id':fusion_id,
                            'fusion_start_time':fusion_start_time,
                            'fusion_longitude':fusion_longitude,
                            'fusion_latitude':fusion_latitude,
                            'fusion_height':fusion_height
                        }

        # 将更新后的记录添加到结果中
        for _, r in window_group.iterrows():
            key = (r['drone_uuid'], r['batch_num'])
            if key in [(rec['drone_uuid'], rec['batch_num']) for rec in candidate_records]:
                updated_record = next(rec for rec in candidate_records if (rec['drone_uuid'], rec['batch_num']) == key)
                r['fusion_id'] = updated_record['fusion_id']
                r['fusion_start_time'] = updated_record['fusion_start_time']
                r['fusion_longitude'] = updated_record['fusion_longitude']
                r['fusion_latitude'] = updated_record['fusion_latitude']
                r['fusion_height'] = updated_record['fusion_height']

            results.append(r.to_dict())

    return pd.DataFrame(results)


def main(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    for i in range(len(data)):
        height = data[i].pop('flyHeight')
        drone_uuid = data[i].pop('droneSeriaID')
        lasting_time = data[i].pop('lastsec')
        batch_num = data[i].pop('batchNo')

        data[i]['height'] = height
        data[i]['drone_uuid'] = drone_uuid
        data[i]['lasting_time'] = lasting_time
        data[i]['batch_num'] = batch_num
        data[i] = parse_dict_record(data[i])

    # 转换为DataFrame
    df = pd.DataFrame(data)

    # 2. 初始化坐标转换器
    transformer = Transformer.from_crs("epsg:4326", "epsg:32651", always_xy=True)

    # 3. 处理信号数据
    processed_df = process_signal_data(df, transformer)
    # print(processed_df.head())

    # 4. 应用融合逻辑
    final_df = apply_fusion(processed_df)

    # 5. 保存结果
    final_df.to_json(output_file, orient='records', lines=True, force_ascii=False)
    print(f"处理完成，结果已保存到 {output_file}")


if __name__ == "__main__":
    input_data = "解码表.json"
    output_data = "fusion_output.json"
    main(input_data, output_data)
