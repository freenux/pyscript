import requests
import csv
import os
import time
import argparse
import logging
from http.client import HTTPConnection
from datetime import datetime, timedelta
from dotenv import load_dotenv  # 添加 dotenv 库导入

def setup_logging(log_file=None, debug=False):
    """设置日志配置"""
    # 创建logger
    logger = logging.getLogger('sql_to_csv')
    logger.setLevel(logging.DEBUG if debug else logging.INFO)

    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG if debug else logging.INFO)

    # 设置日志格式
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)

    # 添加控制台处理器到logger
    logger.addHandler(console_handler)

    # 如果指定了日志文件，添加文件处理器
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG if debug else logging.INFO)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

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

def query_sql_by_date_range(api_key, project, sql_template, start_date, end_date, base_url, logger):
    """
    使用日期范围查询SQL并返回结果
    """
    # 替换SQL模板中的日期参数
    sql = sql_template.replace("{start_date}", start_date).replace("{end_date}", end_date)

    # 记录执行的SQL语句
    logger.info(f"执行SQL: {sql}")

    # 准备请求头和请求体
    headers = {
        "sensorsdata-token": api_key,
    }

    payload = {
        "q": sql,
        "format": "csv"
    }

    # 发送请求
    url = f"{base_url}/sql/query?project={project}"
    logger.debug(f"请求URL: {url}")
    response = requests.post(url, headers=headers, data=payload)
    # 检查响应状态
    if response.status_code != 200:
        logger.error(f"API请求失败，状态码: {response.status_code}, 响应: {response.text}")
        raise Exception(f"API请求失败，状态码: {response.status_code}, 响应: {response.text}")

    # 直接返回CSV格式的响应内容
    return response.text

def save_to_csv(data, output_file, append=False, logger=None):
    """
    将CSV格式的数据保存到文件
    """
    # 检查文件是否存在且不为空
    file_exists = os.path.exists(output_file) and os.path.getsize(output_file) > 0
    
    # 确定写入模式
    mode = 'a' if append and file_exists else 'w'
    
    lines_count = 0
    with open(output_file, mode, encoding='utf-8') as f:
        # 如果是追加模式且文件已存在且不为空，则不写入CSV头部
        if append and file_exists:
            # 跳过CSV的第一行（标题行）
            lines = data.strip().split('\n')
            if len(lines) > 1:
                f.write('\n'.join(lines[1:]) + '\n')
                lines_count = len(lines) - 1
                if logger:
                    logger.debug(f"追加写入 {lines_count} 行数据到 {output_file}")
        else:
            # 直接写入完整的CSV数据
            f.write(data)
            if logger:
                lines_count = data.count('\n')
                logger.debug(f"写入 {lines_count} 行数据到 {output_file}")
    return lines_count


def main():
    parser = argparse.ArgumentParser(description='执行SQL查询并将结果保存为CSV')
    parser.add_argument('--api-key', help='API密钥 (可选，默认从.env文件读取)')
    parser.add_argument('--project', required=True, help='项目名称')
    parser.add_argument('--sql-file', required=True, help='包含SQL模板的文件路径')
    parser.add_argument('--output', required=True, help='输出CSV文件路径')
    parser.add_argument('--start-date', required=True, help='开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True, help='结束日期 (YYYY-MM-DD)')
    parser.add_argument('--interval-days', type=int, default=1, help='每次查询的天数间隔')
    parser.add_argument('--base-url', default='http://bi.stary.ltd/api', help='API基础URL')
    parser.add_argument('--env-file', default='.env', help='.env文件路径')
    parser.add_argument('--debug', action='store_true', help='启用调试模式')
    parser.add_argument('--log-file', help='日志文件路径')

    args = parser.parse_args()

    # 设置日志
    logger = setup_logging(args.log_file, args.debug)

    # 如果启用了调试模式，设置HTTP调试
    if args.debug:
        setup_http_debugging()
        logger.info("已启用HTTP调试模式")

    # 从.env文件加载环境变量
    load_dotenv(args.env_file)
    logger.info(f"已从 {args.env_file} 加载环境变量")

    # 获取项目对应的API key
    project_name = args.project
    api_key_env_var = f"API_KEY_{project_name}"
    api_key = args.api_key or os.getenv(api_key_env_var)

    # 如果没有找到特定项目的API key，尝试使用通用的API_KEY
    if not api_key:
        api_key = os.getenv('API_KEY')
        if api_key:
            logger.info("使用通用API_KEY")

    if not api_key:
        error_msg = f"API密钥未提供，请通过--api-key参数或在.env文件中设置{api_key_env_var}或API_KEY"
        logger.error(error_msg)
        raise ValueError(error_msg)
    else:
        logger.info(f"已获取API密钥")

    # 读取SQL模板
    with open(args.sql_file, 'r', encoding='utf-8') as f:
        sql_template = f.read()
    logger.info(f"已从 {args.sql_file} 读取SQL模板")

    # 解析日期
    start = datetime.strptime(args.start_date, '%Y-%m-%d')
    end = datetime.strptime(args.end_date, '%Y-%m-%d')
    logger.info(f"查询日期范围: {args.start_date} 至 {args.end_date}")

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

        logger.info(f"查询日期范围: {start_str} 至 {end_str}")

        try:
            # 执行查询
            result = query_sql_by_date_range(
                api_key,  # 使用从环境变量获取的api_key
                args.project,
                sql_template,
                start_str,
                end_str,
                args.base_url,
                logger
            )

            # 保存结果到CSV
            lines_count = save_to_csv(result, args.output, not first_write, logger)
            first_write = False
            logger.info(f"成功保存 {lines_count} 条记录")

        except Exception as e:
            logger.error(f"处理日期范围 {start_str} 至 {end_str} 时出错: {e}", exc_info=True)

        # 移动到下一个日期范围
        current_start = current_end + timedelta(days=1)

        # 添加延迟以避免API限制
        time.sleep(1)

    logger.info(f"任务完成，数据已保存到 {args.output}")

if __name__ == "__main__":
    main()
