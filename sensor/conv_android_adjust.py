import csv
import json
import re
import time
from datetime import datetime
import argparse
from collections import defaultdict

def is_valid_android_id(android_id):
    """检查是否是有效的 Android ID 格式 (15-16位的十六进制字符)"""
    if not android_id:
        return False
    pattern = r'^[0-9a-f]{15,16}$'
    return bool(re.match(pattern, android_id.lower()))

def is_valid_gaid(gaid):
    """检查是否是有效的 Google Advertising ID 格式 (UUID 格式)"""
    if not gaid:
        return False
    pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    return bool(re.match(pattern, gaid.lower()))

def convert_to_unix_timestamp(time_str):
    """将时间字符串转换为 Unix 时间戳"""
    try:
        dt = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S.%f')
        return int(dt.timestamp())
    except (ValueError, TypeError):
        return None

def extract_campaign_info(passback_content):
    """从 passback_content 中提取 campaign, adgroup 和 creative 信息"""
    try:
        if not passback_content:
            return "", "", ""
        
        data = json.loads(passback_content)
        campaign = data.get('campaign', '')
        adgroup = ''
        creative = ''
        
        return campaign, adgroup, creative
    except (json.JSONDecodeError, TypeError):
        return "", "", ""

def convert_csv(input_file, android_output_file, gaid_output_file):
    """将输入 CSV 转换为两个指定格式的输出 CSV"""
    # 用于存储每个 ID 的最早记录
    android_records = defaultdict(lambda: {"timestamp": float('inf'), "data": None})
    gaid_records = defaultdict(lambda: {"timestamp": float('inf'), "data": None})
    
    with open(input_file, 'r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile, delimiter='\t')
        
        for row in reader:
            first_id = row.get('first_id', '')
            time_str = row.get('time', '')
            gaid = row.get('googleadid', '')
            timestamp = convert_to_unix_timestamp(time_str)
            if timestamp is None:
                timestamp = 1577808000 # 2020-01-01 00:00:00
                
            passback_content = row.get('passback_content', '{}')
            
            # 提取 campaign 信息
            campaign, adgroup, creative = extract_campaign_info(passback_content)
            
            print(f"first_id: {first_id}, ", is_valid_android_id(first_id))
            # 处理 android_id
            if is_valid_android_id(first_id):
                android_id = first_id
                # 只保留时间戳最早的记录
                if timestamp < android_records[android_id]["timestamp"]:
                    android_records[android_id] = {
                        "timestamp": timestamp,
                        "data": [android_id, timestamp, "ImportedDevices", campaign, adgroup, creative]
                    }
            
            # 处理 gaid
            try:
                data = json.loads(passback_content)
                potential_gaid = data.get('advertising_id', '')
                if is_valid_gaid(potential_gaid):
                    gaid = potential_gaid
                
                print(f"gaid: {gaid}, ", is_valid_gaid(gaid))
                if is_valid_gaid(gaid):
                    # 只保留时间戳最早的记录
                    if timestamp < gaid_records[gaid]["timestamp"]:
                        gaid_records[gaid] = {
                            "timestamp": timestamp,
                            "data": [gaid, timestamp, "ImportedDevices", campaign, adgroup, creative]
                        }
            except (json.JSONDecodeError, TypeError):
                pass
    
    # 写入 android_id 文件
    with open(android_output_file, 'w', encoding='utf-8', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['device_id', 'unix_timestamp', 'network', 'campaign', 'adgroup', 'creative'])
        
        for record in android_records.values():
            if record["data"]:
                writer.writerow(record["data"])
    
    # 写入 gaid 文件
    with open(gaid_output_file, 'w', encoding='utf-8', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['device_id', 'unix_timestamp', 'network', 'campaign', 'adgroup', 'creative'])
        
        for record in gaid_records.values():
            if record["data"]:
                writer.writerow(record["data"])

def main():
    parser = argparse.ArgumentParser(description='转换 CSV 文件格式')
    parser.add_argument('-i', '--input', required=True, help='输入 CSV 文件路径')
    parser.add_argument('-a', '--android-output', required=True, help='Android ID 输出 CSV 文件路径')
    parser.add_argument('-g', '--gaid-output', required=True, help='GAID 输出 CSV 文件路径')
    
    args = parser.parse_args()
    
    convert_csv(args.input, args.android_output, args.gaid_output)
    print(f"转换完成，Android ID 输出文件: {args.android_output}")
    print(f"转换完成，GAID 输出文件: {args.gaid_output}")
    
if __name__ == "__main__":
    main()