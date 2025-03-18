import csv
import json
import re
import time
from datetime import datetime
import argparse
from collections import defaultdict

def is_valid_device_id(data):
    """
    Check if a string matches IDFA/IDFV format
    IDFA/IDFV format: 8-4-4-4-12 hexadecimal digits, total 36 chars including hyphens
    Example: 1234ABCD-12AB-34CD-56EF-1234567ABCDE
    """
    if not data or len(data) != 36:
        return False
        
    parts = data.lower().split('-')
    if len(parts) != 5:
        return False
        
    expected_lengths = [8, 4, 4, 4, 12]
    for part, length in zip(parts, expected_lengths):
        if len(part) != length or not all(c in '0123456789abcdef' for c in part):
            return False
            
    return True

def convert_to_unix_timestamp(time_str):
    """将时间字符串转换为 Unix 时间戳"""
    try:
        dt = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S.%f')
        return int(dt.timestamp())
    except (ValueError, TypeError):
        return None
    
def fix_json_content(content):
    """
    尝试修复被截断或格式错误的JSON字符串
    """
    if not content:
        return '{}'
    
    # 如果已经是有效的JSON，直接返回
    try:
        json.loads(content)
        return content
    except json.JSONDecodeError:
        pass
    
    # 处理常见的截断情况
    try:
        # 确保以 { 开头
        if not content.strip().startswith('{'):
            content = '{' + content
        
        # 确保以 } 结尾
        if not content.strip().endswith('}'):
            content = content + '}'
        
        # 尝试解析，如果成功则返回
        json.loads(content)
        return content
    except json.JSONDecodeError:
        pass
    
    # 更复杂的修复尝试
    try:
        # 移除最后一个可能不完整的键值对
        if content.rstrip().endswith(','):
            content = content.rstrip().rstrip(',') + '}'
        
        # 如果最后一个字段被截断，尝试找到最后一个完整的键值对
        last_comma = content.rfind(',')
        if last_comma > 0:
            content = content[:last_comma] + '}'
            
            # 尝试解析
            json.loads(content)
            return content
    except json.JSONDecodeError:
        pass
    
    # 如果所有尝试都失败，提取所有可能的键值对
    result = {}
    try:
        # 移除首尾的大括号
        content = content.strip()
        if content.startswith('{'): 
            content = content[1:]
        if content.endswith('}'): 
            content = content[:-1]
        
        # 按逗号分割
        pairs = content.split(',')
        for pair in pairs:
            if ':' in pair:
                key, value = pair.split(':', 1)
                key = key.strip().strip('"\'')
                value = value.strip()
                
                # 尝试解析值
                try:
                    if value.startswith('"') and value.endswith('"'):
                        value = value[1:-1]
                    elif value.lower() == 'true':
                        value = True
                    elif value.lower() == 'false':
                        value = False
                    elif value.isdigit():
                        value = int(value)
                    elif value.replace('.', '', 1).isdigit():
                        value = float(value)
                except:
                    pass
                
                result[key] = value
    except Exception:
        pass
    
    # 返回修复后的JSON字符串
    return json.dumps(result)

def extract_conversion_data(content):
    """从 passback_content 中提取 campaign, adgroup 和 creative 信息"""
    try:
        if not content:
            return "", ""
        
        data = json.loads(content)
        if data.get('af_status', '') == 'Organic':
            return "", ""
        
        campaign = data.get('campaign', '')
        idfa = data.get('idfa', '')
        return campaign,idfa
    except (json.JSONDecodeError, TypeError):
        print(f"无法解析JSON: {content}, 错误: {e}")
        return "",""
    
def convert_to_adjust(input_file, idfa_output_file, idfv_output_file):
    """将输入 CSV 转换为两个指定格式的输出 CSV"""
    # 用于存储每个 ID 的最早记录
    idfa_records = dict()
    idfv_records = dict()
    
    with open(input_file, 'r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile, delimiter='\t')
        for row in reader:
            first_id = row.get('first_id', '')
            content = row.get('passback_content', '')
            time_str = row.get('time', '')
            idfa = row.get('idfa', '')
            
            # 使用修复函数处理JSON内容
            fixed_content = fix_json_content(content)
            campaign,conv_idfa = extract_conversion_data(fixed_content)

            timestamp = convert_to_unix_timestamp(time_str)
            if timestamp is None:
                timestamp = 1577808000 # 2020-01-01 00:00:00
            
            idfv = first_id if first_id != idfa and first_id != conv_idfa else ''

            if is_valid_device_id(idfa) and idfa_records.get(idfa) is None:
                idfa_records[idfa] = [idfa, timestamp, 'imported devices', campaign, '', '']
                
            if is_valid_device_id(idfv) and idfv_records.get(idfv) is None:
                idfv_records[idfv] = [idfv, timestamp, 'imported devices', campaign, '', '']
                
        with open(idfa_output_file, 'w', encoding='utf-8', newline='') as outfile:
            writer = csv.writer(outfile, lineterminator='\n')
            writer.writerow(['device_id', 'unix_timestamp', 'network', 'campaign', 'adgroup', 'creative'])
            for record in idfa_records.values():
                writer.writerow(record)
            
        with open(idfv_output_file, 'w', encoding='utf-8', newline='') as outfile:
            writer = csv.writer(outfile, lineterminator='\n')
            writer.writerow(['device_id', 'unix_timestamp', 'network', 'campaign', 'adgroup', 'creative'])
            for record in idfv_records.values():
                writer.writerow(record)
    
def main():
    parser = argparse.ArgumentParser(description='转换 CSV 文件格式')
    parser.add_argument('-i', '--input', required=True, help='输入 CSV 文件路径')
    parser.add_argument('-a', '--idfa-output', required=True, help='IDFA 输出 CSV 文件路径')
    parser.add_argument('-v', '--idfv-output', required=True, help='IDFV 输出 CSV 文件路径')
    
    args = parser.parse_args()
    convert_to_adjust(args.input, args.idfa_output, args.idfv_output)
    print(f"转换完成，IDFA 输出文件: {args.idfa_output}")
    print(f"转换完成，IDFV 输出文件: {args.idfv_output}")

if __name__ == '__main__':
    main()
