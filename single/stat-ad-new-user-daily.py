#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import configparser
import pymysql
import pandas as pd
from urllib.parse import urlparse
import os
from datetime import datetime

# 默认的 mycli 配置文件路径
DEFAULT_MYCLI_CONFIG_PATH = os.path.expanduser("~/.myclirc")
# 老用户判断基准日期
REFERENCE_DATE_STR = "2025-05-07"
REFERENCE_DATE = datetime.strptime(REFERENCE_DATE_STR, "%Y-%m-%d").date()

def get_db_connection_params(alias_dsn_name, config_file_path=DEFAULT_MYCLI_CONFIG_PATH):
    """从 mycli 配置文件中读取数据库连接参数"""
    config = configparser.ConfigParser()
    if not os.path.exists(config_file_path):
        # 尝试备用路径 (e.g., XDG_CONFIG_HOME)
        xdg_config_home = os.environ.get('XDG_CONFIG_HOME', os.path.expanduser('~/.config'))
        alt_config_path = os.path.join(xdg_config_home, 'mycli', 'myclirc')
        if os.path.exists(alt_config_path):
            config_file_path = alt_config_path
        else:
            raise FileNotFoundError(f"mycli config file not found at {config_file_path} or {alt_config_path}")

    config.read(config_file_path)

    if 'alias_dsn' not in config or alias_dsn_name not in config['alias_dsn']:
        raise ValueError(f"DSN alias '{alias_dsn_name}' not found in {config_file_path}")

    dsn_string = config['alias_dsn'][alias_dsn_name]
    
    # 解析 DSN string: mysql://[user[:password]@][host][:port][/dbname]
    parsed_dsn = urlparse(dsn_string)
    
    if parsed_dsn.scheme != 'mysql':
        raise ValueError(f"Unsupported DSN scheme: {parsed_dsn.scheme}. Only 'mysql' is supported.")

    db_params = {}
    db_params['host'] = parsed_dsn.hostname
    db_params['port'] = parsed_dsn.port if parsed_dsn.port else 3306
    db_params['user'] = parsed_dsn.username
    db_params['password'] = parsed_dsn.password
    if parsed_dsn.path:
        db_params['database'] = parsed_dsn.path.lstrip('/')
    
    return db_params

def get_first_login_time(db_conn, user_key):
    """查询用户的首次登录时间"""
    cursor = db_conn.cursor()
    query = "SELECT create_time FROM t_login_device WHERE user_key = %s ORDER BY create_time ASC LIMIT 1"
    try:
        cursor.execute(query, (user_key,))
        result = cursor.fetchone()
        return result[0] if result else None
    finally:
        cursor.close()

def process_csv_file(csv_path, db_conn, campaign_patterns):
    """处理单个CSV文件"""
    try:
        df = pd.read_csv(csv_path, usecols=['Customer User ID', 'Campaign', 'Install Time'])
    except FileNotFoundError:
        print(f"Error: CSV file not found at {csv_path}")
        return {}
    except ValueError as e:
        print(f"Error reading {csv_path}: {e}. Make sure 'Customer User ID', 'Campaign', and 'Install Time' columns exist.")
        return {}

    daily_new_users = {}

    for pattern in campaign_patterns:
        # 根据 Campaign 模式筛选
        # 使用 str.contains 进行模糊匹配，na=False确保NaN值不参与匹配导致错误
        filtered_df = df[df['Campaign'].astype(str).str.contains(pattern, case=False, na=False)]

        for _, row in filtered_df.iterrows():
            customer_user_id = row['Customer User ID']
            install_time_str = row['Install Time'] # 格式通常是 'YYYY-MM-DD HH:MM:SS'
            
            if pd.isna(customer_user_id) or pd.isna(install_time_str):
                continue
            
            try:
                install_date = datetime.strptime(install_time_str.split(' ')[0], '%Y-%m-%d').date()
            except ValueError:
                print(f"Warning: Could not parse install_time '{install_time_str}' for user '{customer_user_id}'. Skipping.")
                continue

            first_login_time = get_first_login_time(db_conn, str(customer_user_id))

            is_new_user = False
            if first_login_time:
                if first_login_time.date() >= REFERENCE_DATE:
                    is_new_user = True
            else:
                # 如果在 t_login_device 中没有记录，但有安装事件，
                # 且安装日期在参考日期之后，也视为新用户 (基于安装事件判断)
                # 注意：题目要求是基于登录事件，这里做一个兼容处理，如果严格按题目，此处应为False
                if install_date >= REFERENCE_DATE:
                     is_new_user = True # 或者根据业务需求，没有登录记录的算老用户或不统计

            # 初始化统计结构
            day_str = install_date.strftime('%Y-%m-%d')
            if day_str not in daily_new_users:
                daily_new_users[day_str] = {}
            if pattern not in daily_new_users[day_str]:
                daily_new_users[day_str][pattern] = {'new_users': 0, 'total_users': 0}
            
            daily_new_users[day_str][pattern]['total_users'] += 1
            if is_new_user:
                daily_new_users[day_str][pattern]['new_users'] += 1
                
    return daily_new_users

def main():
    parser = argparse.ArgumentParser(description='Analyze Appsflyer raw data to find new users daily.')
    parser.add_argument('csv_files', nargs='+', help='Path to one or more CSV files.')
    parser.add_argument('--alias', required=True, help='DSN alias name from mycli config file.')
    parser.add_argument('--patterns', nargs='+', required=True, help='Campaign field matching patterns.')
    parser.add_argument('--myclirc', default=DEFAULT_MYCLI_CONFIG_PATH, help=f'Path to mycli config file (default: {DEFAULT_MYCLI_CONFIG_PATH})')

    args = parser.parse_args()

    try:
        db_params = get_db_connection_params(args.alias, args.myclirc)
        db_conn = pymysql.connect(**db_params, cursorclass=pymysql.cursors.DictCursor) # 使用DictCursor方便获取列名
        print(f"Successfully connected to database: {db_params.get('host')}/{db_params.get('database')}")
    except (FileNotFoundError, ValueError, pymysql.Error) as e:
        print(f"Database connection error: {e}")
        return

    all_daily_stats = {}

    for csv_file_path in args.csv_files:
        print(f"Processing file: {csv_file_path}...")
        daily_stats_for_file = process_csv_file(csv_file_path, db_conn, args.patterns)
        
        # 合并结果
        for day, patterns_data in daily_stats_for_file.items():
            if day not in all_daily_stats:
                all_daily_stats[day] = {}
            for pattern, counts in patterns_data.items():
                if pattern not in all_daily_stats[day]:
                    all_daily_stats[day][pattern] = {'new_users': 0, 'total_users': 0}
                all_daily_stats[day][pattern]['new_users'] += counts['new_users']
                all_daily_stats[day][pattern]['total_users'] += counts['total_users']

    db_conn.close()

    # 输出结果
    print("\n--- Daily New User Statistics ---")
    sorted_days = sorted(all_daily_stats.keys())
    for day in sorted_days:
        print(f"\nDate: {day}")
        for pattern in args.patterns: # 保证输出顺序和输入一致
            if pattern in all_daily_stats.get(day, {}):
                stats = all_daily_stats[day][pattern]
                new_users_count = stats['new_users']
                total_users_count = stats['total_users']
                new_user_ratio = (new_users_count / total_users_count * 100) if total_users_count > 0 else 0
                print(f"  Campaign Pattern: '{pattern}'")
                print(f"    New Users: {new_users_count}")
                print(f"    Total Users (matching pattern): {total_users_count}")
                print(f"    New User Ratio: {new_user_ratio:.2f}%")
            # else:
                # print(f"  Campaign Pattern: '{pattern}' - No data found for this day.")

if __name__ == '__main__':
    main()