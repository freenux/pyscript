#!/usr/bin/env python3
"""
Query user login device creation time from database.
Reads user_id from stdin and queries t_login_device table for create_time.
"""
import argparse
import logging
import sys
from typing import Optional
import pymysql
from pymysql.cursors import DictCursor
import datetime

def setup_logging(level: str = "INFO") -> None:
    """Configure logging with appropriate format and level."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('query_user_key.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )

def parse_mycli_config(mycli_config: str, dsn_name: str) -> dict:
    """Parse mycli config file and extract database connection parameters."""
    import os
    from urllib.parse import urlparse
    
    config_path = os.path.expanduser(mycli_config)
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"mycli config file not found at {config_path}")
    
    dsn_line = None
    with open(config_path, 'r') as f:
        for line in f:
            if line.strip().startswith(f"{dsn_name} = "):
                dsn_line = line.strip()[len(f"{dsn_name} = "):]
                break
    
    if not dsn_line:
        raise ValueError(f"DSN {dsn_name} not found in mycli config")
    
    # Parse DSN format: mysql://[user[:password]@][host][:port][/dbname]
    if not dsn_line.startswith('mysql://'):
        raise ValueError(f"Invalid DSN format: {dsn_line}")
    
    parsed = urlparse(dsn_line)
    db_config = {
        'host': parsed.hostname or 'localhost',
        'user': parsed.username,
        'password': parsed.password or '',
        'port': parsed.port or 3306,
        'database': parsed.path.lstrip('/') if parsed.path else '',
        'charset': 'utf8mb4',
        'cursorclass': pymysql.cursors.DictCursor
    }
    
    return db_config

def setup_database_connection(db_config: dict) -> pymysql.Connection:
    """Create and return a MySQL database connection using config dict."""
    connection = pymysql.connect(**db_config)
    logging.info(f"Connected to database: {db_config['database']}")
    return connection

def query_user_creation_time(conn: pymysql.Connection, user_id: str) -> Optional[dict]:
    """Query the creation time for a given user_id from t_login_device table."""
    query = """
    SELECT user_key, create_time
    FROM t_login_device 
    WHERE user_key = %s
    ORDER BY create_time ASC
    LIMIT 1
    """
    
    logger = logging.getLogger(__name__)
    logger.debug(f"Executing SQL: {query.strip()}" % (f"'{user_id}'",)) 

    with conn.cursor() as cursor:
        cursor.execute(query, (user_id,))
        result = cursor.fetchone()
        logger.debug(f"Query result: {result}")
        return result

def query_user_daily_revenue(conn: pymysql.Connection, user_key: str, create_time) -> dict:
    """Query the revenue for a given user_key on their registration day."""
    # Convert UTC create_time to UTC+8 to get the correct date
    utc8_create_time = create_time + datetime.timedelta(hours=8)
    create_date = utc8_create_time.date()
    
    # Build start and end time for the day in UTC+8
    utc8_start = datetime.datetime.combine(create_date, datetime.time.min)  # 00:00:00
    utc8_end = datetime.datetime.combine(create_date, datetime.time.max)    # 23:59:59.999999
    
    # Convert back to UTC for database query
    utc_start = utc8_start - datetime.timedelta(hours=8)
    utc_end = utc8_end - datetime.timedelta(hours=8)
    
    query = """
    SELECT 
        COUNT(*) as payment_count,
        COALESCE(SUM(payment), 0) as total_revenue
    FROM jingyu_prepaid 
    WHERE user_key = %s 
    AND pc_finish_time >= %s 
    AND pc_finish_time <= %s
    """
    
    logger = logging.getLogger(__name__)
    logger.debug(f"Executing SQL: {query.strip()}" % (f"'{user_key}'", f"'{utc_start}'", f"'{utc_end}'"))
    logger.debug(f"UTC+8 date range: {create_date} ({utc8_start} - {utc8_end})")
    
    with conn.cursor() as cursor:
        cursor.execute(query, (user_key, utc_start, utc_end))
        result = cursor.fetchone()
        logger.debug(f"Revenue query result: {result}")
        return result or {'payment_count': 0, 'total_revenue': 0}

def read_user_ids_from_stdin() -> list:
    """Read user IDs from stdin, one per line."""
    user_ids = []
    if sys.stdin.isatty():
        # Interactive mode - prompt user
        print("Enter user IDs (one per line, press Ctrl+D when done):")
        try:
            while True:
                user_id = input().strip()
                if user_id:
                    user_ids.append(user_id)
        except EOFError:
            pass
    else:
        # Piped input - read from stdin line by line
        for line in sys.stdin:
            user_id = line.strip()
            if user_id:
                user_ids.append(user_id)
    
    if not user_ids:
        raise ValueError("No user IDs provided")
    
    return user_ids

def parse_arguments() -> argparse.Namespace:
    """Parse and return command line arguments."""
    parser = argparse.ArgumentParser(
        description="Query user login device creation time from database"
    )
    parser.add_argument(
        "--mycli-config", 
        default="~/.myclirc", 
        help="mycli config file path (default: ~/.myclirc)"
    )
    parser.add_argument(
        "--dsn", 
        required=True, 
        help="DSN alias name in mycli config"
    )
    parser.add_argument(
        "--log-level", 
        default="INFO", 
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Set logging level (default: INFO)"
    )
    parser.add_argument(
        "--json", 
        action="store_true",
        help="Output result in JSON format"
    )
    return parser.parse_args()

def main():
    """Main function - entry point of the script."""
    args = parse_arguments()
    setup_logging(args.log_level)
    
    logger = logging.getLogger(__name__)
    logger.info("Query user creation time script started")
    
    # Read user IDs from stdin
    user_ids = read_user_ids_from_stdin()
    logger.info(f"Processing {len(user_ids)} user IDs")
    
    # Parse database configuration from mycli config
    db_config = parse_mycli_config(args.mycli_config, args.dsn)
    
    # Database operations
    conn = setup_database_connection(db_config)
    
    results = []
    not_found_count = 0
    
    ad_date = datetime.datetime.fromtimestamp(1746633600) # Thu May  8 00:00:00 CST 2025

    # Query each user ID
    for i, user_id in enumerate(user_ids, 1):
        logger.debug(f"Processing user {i}/{len(user_ids)}: {user_id}")
        result = query_user_creation_time(conn, user_id)
        
        if result and result['create_time'] >= ad_date:
            # Query daily revenue for this user
            revenue_result = query_user_daily_revenue(conn, result['user_key'], result['create_time'])
            
            # Combine results
            combined_result = {
                'user_key': result['user_key'],
                'create_time': result['create_time'],
                'daily_payment_count': revenue_result['payment_count'],
                'daily_total_revenue': revenue_result['total_revenue']
            }
            results.append(combined_result)
            
            if not args.json:
                print(f"--- User {i}: {user_id} ---")
                print(f"User Key: {combined_result['user_key']}")
                print(f"Creation Time: {combined_result['create_time']}")
                print(f"Daily Payment Count: {combined_result['daily_payment_count']}")
                print(f"Daily Total Revenue: {combined_result['daily_total_revenue']}")
                print()
        else:
            not_found_count += 1
            logger.warning(f"No records found for user_id: {user_id}")
            if not args.json:
                print(f"--- User {i}: {user_id} ---")
                print(f"No login device records found for user_id: {user_id}")
                print()
    
    # Output JSON format if requested
    if args.json:
        import json
        json_results = []
        for result in results:
            json_result = {}
            for key, value in result.items():
                if hasattr(value, 'isoformat'):
                    json_result[key] = value.isoformat()
                else:
                    json_result[key] = value
            json_results.append(json_result)
        
        output = {
            "total_queried": len(user_ids),
            "found": len(results),
            "not_found": not_found_count,
            "results": json_results
        }
        print(json.dumps(output, ensure_ascii=False))
    else:
        # Summary
        print("=" * 50)
        print(f"Summary: {len(results)} found, {not_found_count} not found out of {len(user_ids)} total")
    
    conn.close()
    logger.info(f"Query completed successfully. Found: {len(results)}, Not found: {not_found_count}")
    
    # Exit with error code if some users not found
    if not_found_count > 0:
        sys.exit(1)

if __name__ == "__main__":
    main()
