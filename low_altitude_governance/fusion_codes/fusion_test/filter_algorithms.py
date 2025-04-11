
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import ValueStateDescriptor
from pyproj import Transformer
import numpy as np
from pykalman import KalmanFilter


class TrackStateEstimation(KeyedProcessFunction):
    def __init__(self):
        self.utm_to_latlon_transformer = None
        self.last_timestamp_state = None
        self.kf_state = None

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
        fused_longitude, fused_latitude = self.utm_to_latlon_transformer.transform(
            state_mean[0], state_mean[1])
        value['fused_latitude'] = fused_latitude
        value['fused_longitude'] = fused_longitude

        # 输出更新后的记录
        yield value