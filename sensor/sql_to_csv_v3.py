import requests
import csv
import os
import time
import argparse
import logging
from http.client import HTTPConnection
from datetime import datetime, timedelta
from dotenv import load_dotenv  # 添加 dotenv 库导入

def setup_http_debugging():
    """设置HTTP调试日志"""
    # 启用HTTP连接的调试日志
    HTTPConnection.debuglevel = 1

    # 配置logging模块
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True

def query_sql_by_date_range(api_key, project, sql_template, start_date, end_date, base_url):
    """
    使用日期范围查询SQL并返回结果
    """
    # 替换SQL模板中的日期参数
    sql = sql_template.replace("{start_date}", start_date).replace("{end_date}", end_date)

    # 打印执行的SQL语句
    print(f"执行SQL: {sql}")

    # 准备请求头和请求体
    headers = {
        "api-key": api_key,
        "sensorsdata-project": project,
        "Content-Type": "application/json"
    }

    payload = {
        # "sql": sql,
        "sql": "select time from events limit 100",
        "limit": "10"
    }

    # 发送请求
    url = f"{base_url}/model/sql/query"
    response = requests.post(url, headers=headers, json=payload)

    # 检查响应状态
    if response.status_code != 200:
        raise Exception(f"API请求失败，状态码: {response.status_code}, 响应: {response.text}")

    # 解析响应
    result = response.json()
    return result

def save_to_csv(data, output_file, append=False):
    """
    将数据保存到CSV文件
    """
    mode = 'a' if append else 'w'
    write_header = not (append and os.path.exists(output_file) and os.path.getsize(output_file) > 0)

    with open(output_file, mode, newline='', encoding='utf-8') as f:
        writer = csv.writer(f)

        # 写入表头
        if write_header and 'columns' in data:
            writer.writerow(data['columns'])

        # 写入数据
        if 'data' in data:
            # 检查数据格式，如果是列表的列表，直接写入
            if isinstance(data['data'][0], list):
                writer.writerows(data['data'])
            else:
                # 如果是单行数据，转换为列表的列表
                writer.writerow(data['data'])

def main():
    parser = argparse.ArgumentParser(description='执行SQL查询并将结果保存为CSV')
    parser.add_argument('--api-key', help='API密钥 (可选，默认从.env文件读取)')
    parser.add_argument('--project', required=True, help='项目名称')
    parser.add_argument('--sql-file', required=True, help='包含SQL模板的文件路径')
    parser.add_argument('--output', required=True, help='输出CSV文件路径')
    parser.add_argument('--start-date', required=True, help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True, help='结束日期 (YYYY-MM-DD)')
    parser.add_argument('--interval-days', type=int, default=1, help='每次查询的天数间隔')
    parser.add_argument('--base-url', default='http://bi.stary.ltd/api/v3/analytics/v1', help='API基础URL')
    parser.add_argument('--env-file', default='.env', help='.env文件路径')
    parser.add_argument('--debug', action='store_true', help='启用HTTP调试模式')

    args = parser.parse_args()

    # 如果启用了调试模式，设置HTTP调试
    if args.debug:
        setup_http_debugging()

    # 从.env文件加载环境变量
    load_dotenv(args.env_file)

    # 获取项目对应的API key
    project_name = args.project
    api_key_env_var = f"API_KEY_{project_name}"
    api_key = args.api_key or os.getenv(api_key_env_var)

    # 如果没有找到特定项目的API key，尝试使用通用的API_KEY
    if not api_key:
        api_key = os.getenv('API_KEY')

    if not api_key:
        raise ValueError(f"API密钥未提供，请通过--api-key参数或在.env文件中设置{api_key_env_var}或API_KEY")

    # 读取SQL模板
    with open(args.sql_file, 'r', encoding='utf-8') as f:
        sql_template = f.read()

    # 解析日期
    start = datetime.strptime(args.start_date, '%Y-%m-%d')
    end = datetime.strptime(args.end_date, '%Y-%m-%d')

    # 初始化是否为第一次写入
    first_write = True

    # 按日期范围分批查询
    current_start = start
    while current_start <= end:
        # 计算当前批次的结束日期
        current_end = current_start + timedelta(days=args.interval_days - 1)
        if current_end > end:
            current_end = end

        # 格式化日期为字符串
        start_str = current_start.strftime('%Y-%m-%d')
        end_str = current_end.strftime('%Y-%m-%d')

        #print(f"查询日期范围: {start_str} 至 {end_str}")

        try:
            # 执行查询
            result = query_sql_by_date_range(
                api_key,  # 使用从环境变量获取的api_key
                args.project,
                sql_template,
                start_str,
                end_str,
                args.base_url
            )

            # 保存结果到CSV
            save_to_csv(result, args.output, not first_write)
            first_write = False

            print(f"成功保存 {len(result.get('data', []))} 条记录")

        except Exception as e:
            print(f"处理日期范围 {start_str} 至 {end_str} 时出错: {e}")

        # 移动到下一个日期范围
        current_start = current_end + timedelta(days=1)

        # 添加延迟以避免API限制
        time.sleep(1)

if __name__ == "__main__":
    main()
