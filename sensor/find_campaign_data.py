'''
实现一个脚本，根据文件 data/media_source_empty_users_uniq.csv中的用户 id和设备 id 到数据库表t_af_user_info去找用户的apps_flyer_id,
然后根据apps_flyer_id到appsflyer raw data 文件查找归因信息
使用 csv库读取文件内容
'''

import mysql.connector
from typing import List, Tuple
import csv
import argparse
import re
import json

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

def query_appsflyer_id_from_login_id(cursor, login_id: str = None) -> Tuple[str, str, str]:
    """
    查询设备对应的用户appsflyer_id
    返回: (appsflyer_id, device_id, qid) 元组，如果没找到匹配的记录则返回None
    """
    query = """
    SELECT apps_flyer_id, device_id, qid
    FROM t_af_user_info
    WHERE qid = %s AND created_at < '2025-03-16 00:00:00'
    ORDER BY created_at DESC
    LIMIT 1
    """
    cursor.execute(query, (login_id,))

    result = cursor.fetchone()
    if result:
        return (result[0], result[1], result[2])
    else:
        return None

def query_appsflyer_id_from_device_id(cursor, device_id: str = None) -> Tuple[str, str, str]:
    """
    查询设备对应的用户appsflyer_id
    返回: (appsflyer_id, device_id, qid) 元组，如果没找到匹配的记录则返回None
    """
    query = """
    SELECT apps_flyer_id, device_id, qid
    FROM t_af_user_info
    WHERE device_id = %s AND created_at < '2025-03-16 00:00:00'
    ORDER BY created_at DESC
    LIMIT 1
    """
    cursor.execute(query, (device_id,))
    result = cursor.fetchone()
    if result:
        return (result[0], result[1], result[2])
    else:
        return None

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

def query_campaign_in_database(cursor, apps_flyer_id: str) -> Tuple[str, str]:
    """查询apps_flyer_id对应的campaign"""
    query = """
    SELECT *
    FROM af_attribution_info
    WHERE apps_flyer_id = %s AND created_at < '2025-03-16 00:00:00' AND info != ''
    ORDER BY created_at DESC
    LIMIT 1
    """
    cursor.execute(query, (apps_flyer_id,))
    result = cursor.fetchone()
    return result if result else None

def process_devices(input_file: str):
    """处理设备文件并输出结果"""
    # 连接数据库
    conn_xs = connect_to_database_xiaoshuo()
    conn_promotional = connect_to_database_promotional()
    cursor_xs = conn_xs.cursor()
    cursor_promotional = conn_promotional.cursor()

    # 读取输入CSV文件
    with open(input_file, 'r', encoding='utf-8') as f_in:
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

            apps_flyer_id = None
            campaign_result = None
            if is_login_id(second_id):
                login_id = second_id
                result = query_appsflyer_id_from_login_id(cursor_xs, login_id=login_id)
                if result:
                    apps_flyer_id, database_device_id, qid = result
                    campaign_result = query_campaign_in_database(cursor_promotional, apps_flyer_id)
                    login_id_matched = True

            if not campaign_result:
                device_id = first_id if is_android_id(first_id) else device_id
                result = query_appsflyer_id_from_device_id(cursor_xs, device_id=device_id)
                if result:
                    apps_flyer_id, database_device_id, qid = result
                    campaign_result = query_campaign_in_database(cursor_promotional, apps_flyer_id)

            if campaign_result:
                info = campaign_result[4]
                created_at = campaign_result[6]
                updated_at = campaign_result[7]
                try:
                    raw_data = json.loads(info)
                    if len(raw_data) > 0:
                        json_data = {
                            'sensor_id': sensor_id,
                            'first_id': first_id,
                            'second_id': second_id,
                            'qid': qid,
                            'device_id': database_device_id,
                            'apps_flyer_id': apps_flyer_id,
                            'media_source': raw_data.get('media_source', ''),
                            'campaign': raw_data.get('campaign', ''),
                            'compaign_raw_data': info,
                            'created_at': created_at.strftime('%Y-%m-%d %H:%M:%S'),
                            'updated_at': updated_at.strftime('%Y-%m-%d %H:%M:%S'),
                            'login_id_matched': login_id_matched
                        }
                        found.add(sensor_id)
                        print(json.dumps(json_data))
                except json.JSONDecodeError:
                    print('Not found json data, apps_flyer_id: ', apps_flyer_id, 'info: ', info)
            else:
                    print('Not found campaign data, apps_flyer_id: ', apps_flyer_id, row)
            
            if not apps_flyer_id:
                print('Not found appsflyer_id for this user: device_id', device_id, 'qid', qid, row)
    
    # 关闭数据库连接
    cursor_xs.close()
    cursor_promotional.close()
    conn_xs.close()
    conn_promotional.close()

if __name__ == "__main__":
    # 通过argparse读取输入文件和输出文件
    parser = argparse.ArgumentParser()
    parser.add_argument('--sensor_data_file', type=str, required=True)
    args = parser.parse_args()
    process_devices(args.sensor_data_file)

