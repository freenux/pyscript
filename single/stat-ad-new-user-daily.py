#!/usr/bin/env python3
import argparse
import csv
import re
import pymysql
import configparser
from datetime import datetime, timedelta
import os
from collections import defaultdict
from urllib.parse import urlparse
import logging

def setup_logging(level=logging.INFO):
    """Set up logging configuration."""
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger('stat-ad-new-user')

def parse_mysql_dsn(dsn):
    """Parse MySQL DSN into connection parameters."""
    parsed = urlparse(dsn)
    
    user = parsed.username or ''
    password = parsed.password or ''
    host = parsed.hostname or 'localhost'
    port = parsed.port or 3306
    db = parsed.path.lstrip('/') if parsed.path else ''
    
    return {
        'host': host,
        'user': user,
        'password': password,
        'port': port,
        'database': db
    }

def load_mysql_config(config_path, alias_name, logger):
    """Load MySQL configuration from mycli config file."""
    logger.info(f"Loading MySQL config from {config_path} with alias {alias_name}")
    config = configparser.ConfigParser()
    config.read(os.path.expanduser(config_path))
    
    if 'alias_dsn' not in config or alias_name not in config['alias_dsn']:
        logger.error(f"Alias DSN '{alias_name}' not found in mycli config")
        raise ValueError(f"Alias DSN '{alias_name}' not found in mycli config")
    
    dsn = config['alias_dsn'][alias_name]
    logger.debug(f"Found DSN: {dsn.replace(':'+''.join(dsn.split(':')[1:2]), ':***')}")
    return parse_mysql_dsn(dsn)

def process_csv_files(csv_files, mysql_config, campaign_patterns, logger):
    """Process AppsFlyer CSV files and check user status in MySQL."""
    logger.info(f"Processing {len(csv_files)} CSV files")
    logger.info(f"Using campaign patterns: {campaign_patterns}")
    
    # Connect to MySQL
    logger.info(f"Connecting to MySQL at {mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}")
    connection = pymysql.connect(**mysql_config)
    cursor = connection.cursor()
    
    # Dictionary to store results by date and pattern
    results = defaultdict(lambda: defaultdict(lambda: {'new': 0, 'old': 0}))
    
    # Set to track processed user_ids to avoid duplicates
    processed_user_ids = set()
    total_rows = 0
    matching_rows = 0
    
    try:
        for csv_file in csv_files:
            logger.info(f"Processing file: {csv_file}")
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f, delimiter=',')
                file_rows = 0
                file_matching_rows = 0
                
                for row in reader:
                    total_rows += 1
                    file_rows += 1
                    
                    # Skip if no Customer User ID
                    user_id = row.get('Customer User ID')
                    if not user_id:
                        continue

                    if '@' in user_id:  # Get user_key from cuid
                        user_id = user_id.split('@')[0]
                    
                    media_source = row.get('Media Source')
                    if media_source.lower() != 'facebook_ads':
                        continue

                    # Check if campaign matches any pattern
                    campaign = row.get('Campaign', '')
                    matching_patterns = []
                    for pattern in campaign_patterns:
                        if re.search(pattern, campaign, re.IGNORECASE):
                            logger.debug(f"Campaign {campaign} matches pattern {pattern}, user_id: {user_id}")
                            matching_patterns.append(pattern)
                    
                    if not matching_patterns:
                        continue  # Skip if campaign doesn't match any pattern
                    
                    matching_rows += 1
                    file_matching_rows += 1
                    
                    # Skip if already processed this user_id
                    if user_id in processed_user_ids:
                        logger.debug(f"Skipping duplicate user ID: {user_id}")
                        continue
                    processed_user_ids.add(user_id)
                    
                    # Get install date from the row
                    install_date_str = row.get('Install Time', '')
                    if not install_date_str:
                        logger.debug(f"Skipping row with no install time, user ID: {user_id}")
                        continue
                    
                    try:
                        install_date = datetime.strptime(install_date_str, '%Y-%m-%d %H:%M:%S')
                        install_date_key = install_date.strftime('%Y-%m-%d')
                    except ValueError:
                        logger.warning(f"Invalid install date format: {install_date_str}")
                        continue
                    
                    # Query MySQL for first login time
                    query = "SELECT create_time FROM t_login_device WHERE user_key = %s ORDER BY create_time LIMIT 1"
                    cursor.execute(query, (user_id,))
                    result = cursor.fetchone()
                    
                    # Determine if new or old user
                    cutoff_date = datetime(2025, 5, 7)
                    
                    if result and result[0]:
                        first_login = result[0]
                        is_new_user = first_login >= cutoff_date
                        logger.debug(f"User {user_id} first login: {first_login}, is_new_user: {is_new_user}")
                    else:
                        # If no login record found, consider as new user
                        is_new_user = True
                        logger.debug(f"No login record found for user {user_id}, considering as new user")
                    
                    # Update results for each matching pattern
                    for pattern in matching_patterns:
                        if is_new_user:
                            results[install_date_key][pattern]['new'] += 1
                        else:
                            results[install_date_key][pattern]['old'] += 1
                
                logger.info(f"Processed {file_rows} rows in {csv_file}, matched {file_matching_rows} rows")
            
        logger.info(f"Total rows processed: {total_rows}, matched rows: {matching_rows}")
        logger.info(f"Unique users processed: {len(processed_user_ids)}")
    
    except Exception as e:
        logger.error(f"Error processing CSV files: {e}")
        raise
    finally:
        cursor.close()
        connection.close()
        logger.info("Database connection closed")
    
    return results

def print_results(results, logger):
    """Print results in a formatted table."""
    logger.info("Generating results table")
    
    print("\nDaily New User Statistics:")
    print("-" * 90)
    print(f"{'Date':<12} {'Pattern':<40} {'New Users':<10} {'Old Users':<10} {'Total':<10} {'New %':<10}")
    print("-" * 90)
    
    # Sort by date
    for date in sorted(results.keys()):
        date_data = results[date]
        for pattern in sorted(date_data.keys()):
            stats = date_data[pattern]
            new_users = stats['new']
            old_users = stats['old']
            total = new_users + old_users
            new_percentage = (new_users / total * 100) if total > 0 else 0
            
            print(f"{date:<12} {pattern[:38]:<40} {new_users:<10} {old_users:<10} {total:<10} {new_percentage:.2f}%")
            logger.debug(f"Date: {date}, Pattern: {pattern}, New: {new_users}, Old: {old_users}, Total: {total}, New%: {new_percentage:.2f}%")
    
    print("-" * 90)

def main():
    parser = argparse.ArgumentParser(description='Analyze AppsFlyer install data for new user statistics.')
    parser.add_argument('--csv', nargs='+', required=True, help='CSV file paths from AppsFlyer')
    parser.add_argument('--config', default='~/.myclirc', help='Path to mycli config file')
    parser.add_argument('--alias', required=True, help='Alias DSN name in mycli config')
    parser.add_argument('--patterns', nargs='+', required=True, help='Campaign patterns to match')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    logger = setup_logging(log_level)
    
    logger.info("Starting AppsFlyer new user statistics analysis")
    logger.debug(f"Command line arguments: {args}")
    
    try:
        # Load MySQL config
        mysql_config = load_mysql_config(args.config, args.alias, logger)
        
        # Process CSV files
        results = process_csv_files(args.csv, mysql_config, args.patterns, logger)
        
        # Print results
        print_results(results, logger)
        
        logger.info("Analysis completed successfully")
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
