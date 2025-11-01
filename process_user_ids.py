import csv
import re
import json
import time
import os
import logging
from dotenv import load_dotenv
import mysql.connector
from typing import Dict, List
from mysql.connector import Error
from datetime import timezone
# 加载.env文件
load_dotenv()

# 配置日志
def setup_logging(log_file=None, debug=False):
    """设置日志配置"""
    # 创建logger
    logger = logging.getLogger('query-campaign')
    logger.setLevel(logging.DEBUG if debug else logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
    # 创建控制台处理器
    if not log_file or debug:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG if debug else logging.INFO)

        # 设置日志格式
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


def connect_to_database(logger: logging.Logger):
    """连接到MySQL数据库"""
    try:
        return mysql.connector.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            user=os.getenv('DB_USER', 'root'),
            password=os.getenv('DB_PASSWORD', ''),
            database=os.getenv('DB_NAME', ''),
            charset='utf8mb4'
        )
    except Error as e:
        logger.error(f"Error connecting to MySQL: {e}")
        raise

def is_login_id(login_id: str) -> bool:
    """判断login_id是否是login id: 即符合[0-9]{10}"""
    if not login_id:
        return False
    return len(login_id.strip()) == 10 and re.match(r'^[0-9]{10}$', login_id.strip())

def get_preferred_user_id(row: Dict[str, str]) -> str:
    """根据优先级获取用户ID"""
    # 优先使用second_id
    if row['second_id'] and is_login_id(row['second_id']):
        return row['second_id']
    
    # 其次使用distinct_id
    if row['distinct_id'] and is_login_id(row['distinct_id']):
        return row['distinct_id']
    
    # 最后使用first_id
    if row['first_id'] and is_login_id(row['first_id']):
        return row['first_id']
    
    return None

def execute_batch(cursor, batch: List[tuple], logger: logging.Logger, sql_file: str, max_retries: int = 3, debug: bool = False) -> bool:
    """执行一批SQL语句，带重试机制"""
    for retry in range(max_retries):
        try:
            # 开始事务
            if debug:
                logger.info("Starting transaction")
            else:
                cursor.execute("START TRANSACTION")
            
            # 准备批量插入语句
            values_list = []
            for user_id, last_login in batch:
                properties = {
                    "latestActiveTime": str(last_login)
                }
                values_list.append(f"({user_id}, '{json.dumps(properties)}')")
            
            values_str = ",\n".join(values_list)
            sql = f"""INSERT INTO t_user_hub (qid, properties) 
VALUES {values_str}
ON DUPLICATE KEY UPDATE 
    properties = JSON_MERGE_PATCH(properties, VALUES(properties))"""
            
            # 输出SQL到文件
            with open(sql_file, 'a') as f:
                f.write("START TRANSACTION;\n")
                f.write(sql + ";\n")
                f.write("COMMIT;\n")
                f.write("DO SLEEP(0.1);\n\n")
            
            # 执行SQL
            logger.info(f"Executing SQL")
            logger.info("Committing transaction")
            
            if not debug:
                cursor.execute(sql)
                cursor.execute("COMMIT")
            
            # 等待100ms
            time.sleep(0.1)
            return True
            
        except Error as e:
            logger.error(f"Error executing batch (attempt {retry + 1}/{max_retries}): {e}")
            # 回滚事务
            cursor.execute("ROLLBACK")
            if retry < max_retries - 1:
                logger.info(f"Retrying in 1 second...")
                time.sleep(1)  # 等待1秒后重试
            else:
                return False

def process_user_data(input_file: str, batch_size: int = 100, logger: logging.Logger = None, sql_file: str = None):
    """处理用户数据并直接执行SQL语句"""
    # 设置日志
    logger.info("Starting user data processing")
    
    user_data: Dict[str, int] = {}  # {user_id: last_login_time}
    
    # 读取输入CSV文件
    logger.info(f"Reading input file: {input_file}")
    with open(input_file, 'r', encoding='utf-8') as f_in:
        reader = csv.DictReader(f_in, delimiter='\t')
        
        for row in reader:
            # 获取优先的用户ID
            preferred_id = get_preferred_user_id(row)
            if not preferred_id:
                continue
            
            # 解析登录时间
            try:
                login_time = int(row['last_login_time'])
            except ValueError:
                logger.warning(f"Invalid date format in row: {row}")
                continue
            
            # 更新用户数据
            if preferred_id not in user_data or login_time > user_data[preferred_id]:
                user_data[preferred_id] = login_time
    
    logger.info(f"Total unique users found: {len(user_data)}")
    
    # 连接数据库
    try:
        logger.info("Connecting to database...")
        conn = connect_to_database(logger)
        cursor = conn.cursor()
        
        # 分批处理数据
        sorted_data = sorted(user_data.items())
        total_batches = (len(sorted_data) + batch_size - 1) // batch_size
        
        for i in range(0, len(sorted_data), batch_size):
            batch = sorted_data[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} records)")
            
            if not execute_batch(cursor, batch, logger, sql_file):
                logger.error(f"Failed to process batch {batch_num}, skipping...")
                continue
            
            logger.info(f"Successfully processed batch {batch_num}")
        
    except Error as e:
        logger.error(f"Database error: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Process user IDs and execute SQL statements')
    parser.add_argument('--input', required=True, help='Input CSV file path')
    parser.add_argument('--batch-size', type=int, default=100, help='Number of records to process in each batch')
    parser.add_argument('--log-file', help='Path to log file')
    parser.add_argument('--sql-file', required=True, help='Path to SQL output file')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    args = parser.parse_args()

    logger = setup_logging(args.log_file, args.debug)
    process_user_data(args.input, args.batch_size, logger, args.sql_file) 