import pandas as pd
import pymysql
from datetime import datetime
import os
import configparser
import logging
import re
import argparse
import sys

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 1. Read the AppsFlyer install data
def read_appsflyer_data(file_path):
    df = pd.read_csv(file_path)
    return df

# 2. Extract unique Customer User IDs
def extract_customer_ids(df):
    # Filter out empty values and format IDs (remove '@' suffix if present)
    customer_ids = df['Customer User ID'].dropna()
    customer_ids = customer_ids.apply(lambda x: x.split('@')[0] if '@' in str(x) else x)
    return customer_ids.unique().tolist()

# Parse DSN string to get database configuration
def parse_dsn(dsn_string):
    # Common DSN format: mysql://user:password@host:port/database
    match = re.match(r'mysql://([^:]+)(?::([^@]*))?@([^:/]+)(?::(\d+))?/(.+)', dsn_string)
    
    if match:
        user, password, host, port, database = match.groups()
        return {
            'host': host,
            'user': user,
            'password': password or '',
            'database': database,
            'port': int(port) if port else 3306
        }
    else:
        raise ValueError(f"Could not parse DSN string: {dsn_string}")

# Read database configuration from mycli config using configparser
def get_db_config(dsn_name='alisn_dsn'):
    # Find the mycli configuration file
    config_path = os.path.expanduser('~/.myclirc')
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"mycli config file not found at: {config_path}")
    
    # Read the config file with configparser
    config = configparser.ConfigParser()
    config.read(config_path)
    
    # Check if the aliases section exists
    if 'alias_dsn' not in config:
        raise ValueError("No 'alias_dsn' section found in mycli config")
    
    # Get the DSN string from the aliases section
    if dsn_name not in config['alias_dsn']:
        raise ValueError(f"DSN '{dsn_name}' not found in mycli config")
    
    dsn_string = config['alias_dsn'][dsn_name]
    
    # Parse the DSN string
    return parse_dsn(dsn_string)

# 3. Query MySQL for first login times
def get_first_login_times(customer_ids, db_config):
    # Handle empty list case
    if not customer_ids:
        logger.warning("No customer IDs to query")
        return {}
    
    try:
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()
        
        # Split into smaller batches to avoid query size limits
        batch_size = 500
        results = {}
        
        for i in range(0, len(customer_ids), batch_size):
            batch = customer_ids[i:i+batch_size]
            placeholders = ', '.join(['%s'] * len(batch))
            
            query = f"""
            SELECT user_key, create_time
            FROM t_login_device
            WHERE user_key IN ({placeholders})
            """
            
            cursor.execute(query, batch)
            batch_results = cursor.fetchall()
            
            # Add results to the dictionary
            for user_key, create_time in batch_results:
                results[user_key] = create_time
                
            logger.info(f"Processed batch {i//batch_size + 1}, found {len(batch_results)} matches")
        
        cursor.close()
        conn.close()
        
        return results
        
    except Exception as e:
        logger.error(f"Database error: {e}")
        raise

# 4. Classify users and generate statistics
def classify_users(df, login_times):
    # Convert Install Time to datetime
    df['Install Time'] = pd.to_datetime(df['Install Time'])
    
    # Add classification column
    df['User Type'] = 'Unknown'
    cutoff_date = datetime.strptime('2025-05-07', '%Y-%m-%d')
    
    # Classify each user
    classified_count = 0
    for index, row in df.iterrows():
        customer_id = row['Customer User ID']
        if pd.notna(customer_id):
            # Remove '@' suffix if present
            clean_id = customer_id.split('@')[0] if '@' in str(customer_id) else customer_id
            
            if clean_id in login_times:
                first_login = login_times[clean_id]
                df.at[index, 'User Type'] = 'Returning' if first_login < cutoff_date else 'New'
                classified_count += 1
    
    logger.info(f"Classified {classified_count} users out of {len(df)} records")
    
    # Group by date and calculate statistics
    df['Date'] = df['Install Time'].dt.date
    
    daily_stats = df.groupby('Date').apply(lambda x: {
        'total_installs': len(x),
        'new_users': sum(x['User Type'] == 'New'),
        'returning_users': sum(x['User Type'] == 'Returning'),
        'unknown_users': sum(x['User Type'] == 'Unknown'),
        'new_user_ratio': sum(x['User Type'] == 'New') / len(x) if len(x) > 0 else 0
    }).reset_index()
    
    return daily_stats

# Parse command line arguments
def parse_arguments():
    parser = argparse.ArgumentParser(description='Analyze AppsFlyer install data and determine new vs returning users.')
    parser.add_argument('-f', '--file', required=True, help='Path to the AppsFlyer raw data CSV file')
    parser.add_argument('-d', '--dsn', default='alisn_dsn', help='DSN name in mycli config (default: alisn_dsn)')
    parser.add_argument('-o', '--output', default='appsflyer_user_analysis.csv', help='Output CSV file path')
    parser.add_argument('-c', '--cutoff', default='2025-05-07', help='Cutoff date for new/returning users (YYYY-MM-DD)')
    
    return parser.parse_args()

# Main function
def main():
    try:
        # Parse command line arguments
        args = parse_arguments()
        
        # Validate file path
        if not os.path.exists(args.file):
            logger.error(f"File not found: {args.file}")
            sys.exit(1)
        
        # Get database configuration from mycli
        logger.info(f"Reading database configuration from mycli using DSN: {args.dsn}")
        db_config = get_db_config(args.dsn)
        logger.info(f"Using database: {db_config['database']} on host: {db_config['host']}")
        
        # Process data
        logger.info(f"Reading AppsFlyer data from {args.file}")
        df = read_appsflyer_data(args.file)
        logger.info(f"Read {len(df)} records from AppsFlyer data")
        
        customer_ids = extract_customer_ids(df)
        logger.info(f"Extracted {len(customer_ids)} unique customer IDs")
        
        logger.info("Querying database for first login times")
        login_times = get_first_login_times(customer_ids, db_config)
        logger.info(f"Found {len(login_times)} matching users in database")
        
        logger.info("Classifying users and generating statistics")
        daily_stats = classify_users(df, login_times)
        
        # Output results
        print("\nDaily New User Statistics:")
        for _, row in daily_stats.iterrows():
            date = row['Date']
            stats = row[0]  # The dictionary is stored in column 0
            print(f"Date: {date}")
            print(f"  Total Installs: {stats['total_installs']}")
            print(f"  New Users: {stats['new_users']} ({stats['new_user_ratio']*100:.2f}%)")
            print(f"  Returning Users: {stats['returning_users']}")
            print(f"  Unknown Users: {stats['unknown_users']}")
            print("---")
        
        # Save results to CSV
        result_df = pd.DataFrame({
            'Date': daily_stats['Date'],
            'Total Installs': daily_stats[0].apply(lambda x: x['total_installs']),
            'New Users': daily_stats[0].apply(lambda x: x['new_users']),
            'Returning Users': daily_stats[0].apply(lambda x: x['returning_users']),
            'Unknown Users': daily_stats[0].apply(lambda x: x['unknown_users']),
            'New User Ratio': daily_stats[0].apply(lambda x: x['new_user_ratio'])
        })
        
        result_df.to_csv(args.output, index=False)
        logger.info(f"Results saved to {args.output}")
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    main()