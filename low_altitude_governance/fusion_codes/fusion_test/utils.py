from datetime import datetime
import json
import re


def preprocess_data(record, transformer):
    record['timestamp'] = record['timestamp']
    # 坐标转换
    utm_x, utm_y = transformer.transform(record['longitude'], record['latitude'])
    record['utm_x'] = utm_x
    record['utm_y'] = utm_y

    # print(f"打印 preprocess_data :  {record}")
    return record


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


# 1.json解析及异常处理
# 2.取'drone' 键的数据
# 3.拼接signal_id=drone_uuid + batch_num
# 4.提取timestamp/event_time
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

    for key in ["frequency", "height", "speed", "latitude", "longitude"]:
        if key in data:
            data[key] = float(data[key])

    # print(f"打印 输出数据：{type(data)} ：{data}")
    return data


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
