import mysql.connector
from typing import List, Tuple
import csv

def connect_to_database() -> mysql.connector.connection.MySQLConnection:
    """连接到MySQL数据库"""
    return mysql.connector.connect(
        host="coredb-new-cluster.cluster-ro-cxs17h1ropjt.us-east-1.rds.amazonaws.com",  # 替换为实际的数据库主机
        user="xiaoshuo",  # 替换为实际的用户名
        password="D3v@HY2o17",  # 替换为实际的密码
        database="xiaoshuo"  # 替换为实际的数据库名
    )

def query_device(cursor, android_id: str = None, apps_flyer_id: str = None, advertising_id: str = None) -> Tuple[str, int]:
    """
    查询设备对应的用户ID
    返回: (device_id, user_id) 元组，如果没找到匹配的记录则返回None
    """
    if android_id:
        # 使用Android ID查询
        query = """
        SELECT apps_flyer_id, qid, 0
        FROM t_device 
        WHERE device_id = %s
        LIMIT 1
        """
        cursor.execute(query, (android_id,))
        result = cursor.fetchone()
        if result:
            return (result[0], result[1], result[2])
        else:
            print ('Not found: device_id', android_id, 'advertising_id', advertising_id)

    if apps_flyer_id:
        # 使用apps_flyer_id查询
        query = """
        SELECT apps_flyer_id, qid, 0
        FROM t_device 
        WHERE apps_flyer_id = %s
        LIMIT 1
        """
        cursor.execute(query, (apps_flyer_id,))
        result = cursor.fetchone()
        if result:
            return (result[0], result[1], result[2])
        else:
            print('Not found: apps_flyer_id', apps_flyer_id, 'advertising_id', advertising_id)
    
    return None

def process_devices(input_file: str, output_file: str):
    """处理设备文件并输出结果"""
    # 连接数据库
    conn = connect_to_database()
    cursor = conn.cursor()

    # 打开输出文件
    with open(output_file, 'w', newline='', encoding='utf-8') as f_out:
        writer = csv.writer(f_out, delimiter='\t')
        writer.writerow(['device_id', 'user_id', 'media_source', 'campaign'])  # 写入标题行

        # 读取输入CSV文件
        with open(input_file, 'r', encoding='utf-8') as f_in:
            reader = csv.reader(f_in, delimiter='\t')
            
            # 遍历每一行
            for row in reader:
                apps_flyer_id, advertising_id, android_id, media_source, campaign = row
                android_id = android_id.strip().split('@')[0]
                # 查询设备
                result = query_device(
                    cursor,
                    android_id=android_id,
                    apps_flyer_id=apps_flyer_id,
                    advertising_id=advertising_id
                )

                # 如果找到匹配的记录，写入输出文件
                if result:
                    afid, qid, adid = result
                    print(qid, apps_flyer_id, afid, advertising_id, adid, media_source, campaign)
                    writer.writerow([qid, apps_flyer_id, afid, advertising_id, adid, media_source, campaign])

    # 关闭数据库连接
    cursor.close()
    conn.close()

if __name__ == "__main__":
    input_file = "android_id_empty.csv"  # 替换为实际的输入文件路径
    output_file = "device_user_mapping.csv"  # 替换为实际的输出文件路径
    process_devices(input_file, output_file)

