'''
实现一个脚本，根据文件 data/media_source_empty_users_uniq.csv中的用户 id和设备 id 到数据库表t_af_user_info去找用户的apps_flyer_id,
然后根据apps_flyer_id到appsflyer raw data 文件查找归因信息
使用 csv库读取文件内容
'''

import mysql.connector
from typing import List, Tuple, Set
import csv
import argparse
import re
import json
import logging

def setup_logging(log_file=None, debug=False):
    """设置日志配置"""
    # 创建logger
    logger = logging.getLogger('query-campaign')
    logger.setLevel(logging.DEBUG if debug else logging.INFO)

    # 设置日志格式
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # 创建控制台处理器
    if debug or not log_file:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG if debug else logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    # 如果指定了日志文件，添加文件处理器
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG if debug else logging.INFO)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

def connect_to_database_xiaoshuo() -> mysql.connector.connection.MySQLConnection:
    """连接到MySQL数据库"""
    return mysql.connector.connect(
        host="coredb-new-cluster.cluster-ro-cxs17h1ropjt.us-east-1.rds.amazonaws.com",  # 替换为实际的数据库主机
        user="xiaoshuo",  # 替换为实际的用户名
        password="D3v@HY2o17",  # 替换为实际的密码
        database="xiaoshuo"  # 替换为实际的数据库名
    )

def connect_to_database_promotional() -> mysql.connector.connection.MySQLConnection:
    """连接到MySQL数据库"""
    return mysql.connector.connect(
        host="coredb-new-cluster.cluster-ro-cxs17h1ropjt.us-east-1.rds.amazonaws.com",  # 替换为实际的数据库主机
        user="k8spromotional",  # 替换为实际的用户名
        password="CNuugf9s0a209SFVHqp",  # 替换为实际的密码
        database="promotional"  # 替换为实际的数据库名
    )

def query_appsflyer_ids_from_login_id(cursor, login_id: str) -> Set[str]:
    """
    查询设备对应的用户appsflyer_id
    返回: appsflyer_id列表
    """
    query = """
    SELECT apps_flyer_id
    FROM t_af_user_info
    WHERE qid = %s AND product_id = 1
    ORDER BY created_at ASC
    """
    cursor.execute(query, (login_id,))

    result = cursor.fetchall()
    if result:
        return set([row[0] for row in result])
    else:
        return set()

def query_appsflyer_ids_from_device_ids(cursor, device_ids: Set[str]) -> Set[str]:
    """
    查询设备对应的用户appsflyer_id
    返回: (appsflyer_id, device_id, qid) 元组，如果没找到匹配的记录则返回None
    """
    placeholders = ','.join(['%s'] * len(device_ids))
    query = f"""
    SELECT apps_flyer_id
    FROM t_af_user_info
    WHERE device_id IN ({placeholders}) AND product_id = 1
    ORDER BY created_at ASC
    """
    cursor.execute(query, tuple(device_ids))
    result = cursor.fetchall()
    if result:
        return set([row[0] for row in result])
    else:
        return set()

def process_appsflyer_raw_data(appsflyer_raw_data_files: List[str]):
    data = {}
    """处理appsflyer raw data文件并输出结果"""
    for appsflyer_raw_data_file in appsflyer_raw_data_files:
        with open(appsflyer_raw_data_file, 'r', encoding='utf-8') as f_in:
            reader = csv.DictReader(f_in, delimiter=',')
            for row in reader:
                appsflyer_id = row['AppsFlyer ID']
                media_source = row['Media Source']
                campaign = row['Campaign']
                data[appsflyer_id] = (media_source, campaign)

    return data

def is_android_id(android_id: str) -> bool:
    """判断android_id是否是android id: 即符合[0-9a-f]{16}"""
    return len(android_id.strip()) == 16 and re.match(r'^[0-9a-f]{16}$', android_id.strip())

def is_login_id(login_id: str) -> bool:
    """判断login_id是否是login id: 即符合[0-9]{10}"""
    return len(login_id.strip()) == 10 and re.match(r'^[0-9]{10}$', login_id.strip())

def query_campaign_in_database(cursor, apps_flyer_ids: Set[str], logger: logging.Logger) -> List[dict]:
    """查询apps_flyer_ids对应的campaign"""
    placeholders = ','.join(['%s'] * len(apps_flyer_ids))
    query = f"""
    SELECT info
    FROM af_attribution_info
    WHERE apps_flyer_id IN ({placeholders}) AND info != '' AND (event_name = 'install' OR event_name = 're-attribution')
    """
    cursor.execute(query, tuple(apps_flyer_ids))
    rows = cursor.fetchall()
    result = []
    for row in rows:
        try:
            raw_data = json.loads(row[0])
            result.append(raw_data)
        except json.JSONDecodeError:
            logger.info(f'Not found json data, apps_flyer_id: {apps_flyer_ids}, info: {row[0]}')
    return result

def process_devices(input_file: str, output_file: str, logger: logging.Logger):
    """处理设备文件并输出结果"""
    # 连接数据库
    conn_xs = connect_to_database_xiaoshuo()
    conn_promotional = connect_to_database_promotional()
    cursor_xs = conn_xs.cursor()
    cursor_promotional = conn_promotional.cursor()

    # 读取输入CSV文件
    with open(input_file, 'r', encoding='utf-8') as f_in, open(output_file, 'w', encoding='utf-8') as f_out:
        reader = csv.DictReader(f_in, delimiter='\t')
        
        found = set()
        # 遍历每一行
        for row in reader:
            sensor_id = row['id']
            second_id = row['second_id']
            first_id = row['first_id']
            device_id = row['$device_id']
                
            if sensor_id in found:
                continue

            login_id_matched = False

            apps_flyer_ids_with_login_id = set()
            apps_flyer_ids_with_device_ids = set()
            campaign_result = list()
            if is_login_id(second_id):
                login_id = second_id
                apps_flyer_ids_with_login_id = query_appsflyer_ids_from_login_id(cursor_xs, login_id)
                if len(apps_flyer_ids_with_login_id) > 0:
                    campaign_result = query_campaign_in_database(cursor_promotional, apps_flyer_ids_with_login_id, logger)
                    login_id_matched = True

            # 神策$device_id 和 $first_id 可能不相同，iOS 设备的$device_id是设备首次的匿名 ID
            # 所以需要同时使用$device_id 和 $first_id来查询appsflyer_id
            if len(campaign_result) == 0:
                logger.warning(f'Not found campaign data for login id: {login_id}, apps_flyer_ids: {apps_flyer_ids_with_login_id}, now try to query with device id.')
                
                device_ids = set()
                if not is_login_id(first_id):
                    device_ids.add(first_id)
                
                if len(device_id) > 0 and device_id not in device_ids:
                    device_ids.add(device_id)

                if len(device_ids) > 0:
                    apps_flyer_ids_with_device_ids = query_appsflyer_ids_from_device_ids(cursor_xs, device_ids)
                    apps_flyer_ids = apps_flyer_ids_with_device_ids - apps_flyer_ids_with_login_id
                    if len(apps_flyer_ids) > 0:
                        campaign_result = query_campaign_in_database(cursor_promotional, apps_flyer_ids, logger)

            if len(apps_flyer_ids_with_device_ids) == 0 and len(apps_flyer_ids_with_login_id) == 0:
                logger.warning(f'Not found appsflyer_id for this user: {row}')
                continue

            if len(campaign_result) == 0:
                all_apps_flyer_ids = apps_flyer_ids_with_device_ids | apps_flyer_ids_with_login_id
                logger.warning(f'Not found campaign data for this user, apps_flyer_ids: {all_apps_flyer_ids}, row: {row}')
                continue

            first_campaign_data = min(campaign_result, key=lambda x: x['install_time_selected_timezone'])
            log_data = {
                'sensor_data': row,
                'campaign_data': first_campaign_data,
                'is_login_id_matched': login_id_matched,
            }
            f_out.write(json.dumps(log_data) + '\n')
            found.add(sensor_id)

    # 关闭数据库连接
    cursor_xs.close()
    cursor_promotional.close()
    conn_xs.close()
    conn_promotional.close()

if __name__ == "__main__":
    # 通过argparse读取输入文件和输出文件
    parser = argparse.ArgumentParser()
    parser.add_argument('--sensor_data_file', type=str, required=True)
    parser.add_argument('--output_file', type=str, required=True)
    parser.add_argument('--log_file', type=str, required=True)
    parser.add_argument('--debug', type=bool, required=False, default=False)
    args = parser.parse_args()

    logger = setup_logging(args.log_file, args.debug)
    process_devices(args.sensor_data_file, args.output_file, logger)