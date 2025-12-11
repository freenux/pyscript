#!/usr/bin/env python3
import argparse
import logging
import os
import re
import sys
from urllib.parse import urlparse
from decimal import Decimal

import pymysql
import pymysql.cursors
import geoip2.database
import geoip2.errors

def setup_logging(debug=False):
    log_level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger()

def parse_mycli_config(mycli_config, dsn_name):
    config_path = mycli_config
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

def load_sku_data(filepath):
    """Load SKU data from file and create dictionaries.
    
    File format:
    {sku}-{country-code}\t{"amount":150000000,"country":"JP","currency":"JPY"}
    """
    import json
    
    # Create dictionaries
    sku_country_amount = {}  # d1: {sku: {country: local_amount}}
    sku_currency_amount = {}  # d2: {sku: {currency_code: local_amount}}
    sku_number_amount = {}  # d3: {sku: {amount_number: local_amount}}
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                # Skip empty lines or comments
                if not line.strip() or line.strip().startswith('#'):
                    continue
                
                # Parse line: "{sku}-{country-code}\t{"amount":150000000,"country":"JP","currency":"JPY"}"
                try:
                    key_part, json_part = line.strip().split('\t', 1)
                    sku, country_code = key_part.strip().split('-', 1)
                    
                    # Parse JSON data
                    data = json.loads(json_part)
                    
                    amount = data.get('amount')
                    country = data.get('country')
                    currency = data.get('currency')
                    
                    if not (sku and country and currency and amount is not None):
                        logging.warning(f"Incomplete data in line: {line}")
                        continue
                    
                    # Format the local amount as "CURRENCYamount"
                    # Convert numeric amount to string with proper formatting
                    if not isinstance(amount, (int, float)):
                        raise ValueError(f"Invalid amount type: {type(amount)}")
                    
                    amount = float(amount / 1000000)
                    formatted_amount = f"{amount:.2f}"
                    
                    local_amount = {
                        'currency': currency,
                        'amount': formatted_amount,
                        'local_amount': f"{currency}{formatted_amount}",
                    }
                    
                    # Build d1: sku-country-local amount
                    if sku not in sku_country_amount:
                        sku_country_amount[sku] = {}
                    sku_country_amount[sku][country] = local_amount
                    
                    # Build d2: sku-currency code-local amount
                    if sku not in sku_currency_amount:
                        sku_currency_amount[sku] = {}
                    sku_currency_amount[sku][currency] = local_amount
                    
                    # Build d3: sku-local amount number-local amount
                    if sku not in sku_number_amount:
                        sku_number_amount[sku] = {}
                    sku_number_amount[sku][formatted_amount] = local_amount
                    
                except (ValueError, json.JSONDecodeError) as e:
                    logging.error(f"Error parsing line '{line}': {e}")
                    continue
                    
    except Exception as e:
        logging.error(f"Error loading SKU data from {filepath}: {e}")
        raise
    
    logging.info(f"Loaded SKU data: {len(sku_country_amount)} SKUs, covering {sum(len(v) for v in sku_country_amount.values())} country entries")
    
    return sku_country_amount, sku_currency_amount, sku_number_amount

def get_country_from_ip(ip, geo_reader):
    """Get country code from IP address using MaxMind GeoLite2 database."""
    try:
        response = geo_reader.city(ip)
        country_code = response.country.iso_code
        if country_code:
            return country_code
    except geoip2.errors.AddressNotFoundError:
        logging.warning(f"IP {ip} not found in GeoLite2 database")
    
    return 'US'  # Default if IP lookup fails

def get_currency_code_from_symbol(symbol, country):
    """Convert currency symbol to currency code based on symbol and country."""
    # Basic mapping of currency symbols to codes
    symbol_map = {
        '$': {'default': 'USD', 'CN': 'HKD'},  # $ is USD, but in China it could be HKD
        '¥': {'default': 'JPY', 'CN': 'CNY'},  # ¥ is JPY, but in China it's CNY
        '€': {'default': 'EUR'},
        '£': {'default': 'GBP'},
        'Rp': {'default': 'IDR'},
        '₩': {'default': 'KRW'},
        '₺': {'default': 'TRY'},
        '₱': {'default': 'PHP'},
        'S/': {'default': 'PEN'},
        'RM': {'default': 'MYR'},
        '₹': {'default': 'INR'},
    }
    
    if symbol in symbol_map:
        return symbol_map[symbol].get(country, symbol_map[symbol]['default'])
    return None

def fix_local_amount(order, d1, d2, d3, logger, geo_reader, debug=False):
    """Fix local_amount field according to the given rules."""
    order_id = order['id']
    sku = order['product_id']
    local_amount = order['local_amount']
    ip = order['ip']
    country = get_country_from_ip(ip, geo_reader)
    fixed_amount = None
    fix_reason = None

    logger.info(f"Processing order {order_id} {sku} {local_amount} {country}")
    
    # Skip if SKU is not in our reference data
    if sku not in d1 or sku not in d2 or sku not in d3:
        logger.warning(f"Order {order_id} {sku} not found in reference data")
        return None, None
    
    # Case 1: Parse currency code from local_amount
    currency_code_match = re.match(r'^([A-Z]{3})(\d+(\.\d+)?)$', local_amount)
    if currency_code_match:
        currency_code = currency_code_match.group(1)
        amount_value = currency_code_match.group(2)
        if currency_code in d2[sku]:
            sku_amount = d2[sku][currency_code]
            decimal_sku_amount = Decimal(sku_amount['amount'])
            decimal_amount_value = Decimal(amount_value)
            if abs(decimal_sku_amount - decimal_amount_value) / decimal_amount_value > Decimal(0.2):
                logger.warning(f"Order {order_id}: SKU {sku} has currency code {currency_code} but amount {amount_value} does not match {sku_amount['amount']}")
                fixed_amount = sku_amount['local_amount']
                fix_reason = f"Corrected currency code format from {local_amount} to {fixed_amount}"
                return fixed_amount, fix_reason
            else:
                logger.info(f"Order {order_id}: SKU {sku} has currency code {currency_code} but amount {amount_value} matches {sku_amount['amount']}")
                return None, None
    
    # Case 2: local_amount contains currency symbol
    symbol_match = re.match(r'^([^0-9.]+)(\d+(\.\d+)?)$', local_amount)
    if symbol_match:
        symbol = symbol_match.group(1)
        amount_value = symbol_match.group(2)
        currency_code = get_currency_code_from_symbol(symbol, country)
        if currency_code and currency_code in d2[sku]:
            sku_amount = d2[sku][currency_code]
            if sku_amount['amount'] != amount_value:
                logger.warning(f"Order {order_id}: SKU {sku} has currency code {currency_code} but amount {amount_value} does not match {sku_amount['amount']}")
            fixed_amount = sku_amount['local_amount']
            fix_reason = f"Replaced currency symbol {symbol} with code {currency_code}"
            return fixed_amount, fix_reason
    
    # Case 3: local_amount is 0
    if local_amount == '0' or local_amount == '0.0' or local_amount == '0.00':
        if country in d1[sku]:
            fixed_amount = d1[sku][country]['local_amount']
            fix_reason = f"Replaced zero amount with country-based amount from IP"
            return fixed_amount, fix_reason
        
    # Case 4: local_amount is just a number
    number_match = re.match(r'^(\d+(\.\d+)?)$', local_amount)
    if number_match:
        amount_number = number_match.group(1)
        # Check if the number exists in d3
        if amount_number in d3[sku]:
            sku_amount = d3[sku][amount_number]
            fixed_amount = sku_amount['local_amount']
            fix_reason = f"Added currency code to numeric amount {amount_number}"
            return fixed_amount, fix_reason
        # If not in d3, look up in d1 by IP
        elif country in d1[sku]:
            fixed_amount = d1[sku][country]['local_amount']
            fix_reason = f"Used country {country} from IP to determine local amount"
            return fixed_amount, fix_reason

    # Case 5: Unhandled case
    logger.error(f"Order {order_id}: Unable to fix local_amount '{local_amount}' for SKU {sku}")
    return None, "Unhandled case, manual analysis required"

def update_prepaid_order(conn, cursor, order_id, local_amount, fix_reason, logger, debug=False):
    """Update the prepaid order with the fixed local_amount."""
    update_query = "UPDATE jingyu_prepaid SET local_amount = %s WHERE id = %s AND local_amount = %s"
    if not debug:
        cursor.execute(update_query, (local_amount, order_id, local_amount))
        conn.commit()
    logger.info(f"SQL: {update_query} with params {(local_amount, order_id, local_amount)}")
    logger.info(f"Updated order {order_id}: {local_amount} -> {local_amount} ({fix_reason})")

def update_order(conn, cursor, user_id, order_id, local_amount, fix_reason, logger, debug):
    """Update the order with the fixed local_amount."""
    update_query = f"UPDATE t_order_{user_id % 100:02d} SET price_local = %s WHERE prepaid_id = %s AND price_local = %s"
    if not debug:
        cursor.execute(update_query, (local_amount, order_id, local_amount))
        conn.commit()
    logger.info(f"SQL: {update_query} with params {(local_amount, order_id, local_amount)}")
    logger.info(f"Updated order {order_id}: {local_amount} -> {local_amount} ({fix_reason})")

def process_orders(conn, start_time, end_time, d1, d2, d3, logger, geo_reader, debug=False):
    """Process orders within the specified time range."""
    cursor = conn.cursor()
    
    try:
        query = """
        SELECT id, qid, pc_finish_time, ip, local_amount, product_id, pay_type, pay_way
        FROM jingyu_prepaid
        WHERE pc_finish_time BETWEEN %s AND %s AND pay_type IN (22,23)
        """
        cursor.execute(query, (start_time, end_time))
        orders = cursor.fetchall()
        
        logger.info(f"Found {len(orders)} orders to process")
        
        updates = 0
        for order in orders:
            fixed_amount, fix_reason = fix_local_amount(order, d1, d2, d3, logger, geo_reader, debug)
            
            if fixed_amount and fixed_amount != order['local_amount']:
                #update_prepaid_order(conn, cursor, order['id'], fixed_amount, fix_reason, logger, debug)
                update_order(conn, cursor, order['qid'], order['id'], fixed_amount, fix_reason, logger, debug)
                updates += 1
        
        logger.info(f"Completed processing. Updated {updates} orders.")
        
    except Exception as e:
        logger.error(f"Error processing orders: {e}")
        conn.rollback()
    finally:
        cursor.close()

def main():
    parser = argparse.ArgumentParser(description="Fix local_amount field in orders table")
    parser.add_argument('--mycli-config', required=True, help="mycli config file")
    parser.add_argument('--dsn', required=True, help="DSN name in mycli config")
    parser.add_argument('--start-time', required=True, help="Start time in YYYY-MM-DD HH:MM:SS format")
    parser.add_argument('--end-time', required=True, help="End time in YYYY-MM-DD HH:MM:SS format")
    parser.add_argument('--sku-file', required=True, help="Path to SKU data file")
    parser.add_argument('--geoip-db', default='GeoLite2-City.mmdb', help="Path to GeoLite2-City.mmdb database")
    parser.add_argument('--debug', action='store_true', help="Debug mode - don't modify database, just print SQL")
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging(args.debug)
    
    try:
        # Parse database configuration from mycli config
        db_config = parse_mycli_config(args.mycli_config, args.dsn)
        
        # Load SKU data
        d1, d2, d3 = load_sku_data(args.sku_file)  # Replace with actual file loading logic
        
        geo_reader = geoip2.database.Reader(args.geoip_db)

        # Connect to database
        conn = pymysql.connect(**db_config)
        
        try:
            # Process orders
            process_orders(conn, args.start_time, args.end_time, d1, d2, d3, logger, geo_reader, args.debug)
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"Error: {e}")
        geo_reader.close()
        sys.exit(1)

if __name__ == "__main__":
    main()
