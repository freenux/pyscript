import csv
import re
from collections import defaultdict
from decimal import Decimal
import json
import geoip2.database
import argparse

currency_symbol_2c_map = {
    'Rp': 'IDR',
    'RM': 'MYR',
    # 'kr': 'SEK',
    'S/': 'PEN',
}

currency_symbol_1c_map = {
    '£': 'GBP',
    '€': 'EUR',
    '¥': 'CNY',
    '₩': 'KRW',
    '$': 'USD',
    '₺': 'TRY', 
    '₱': 'PHP',
}

def down_update_sql(user_id, order_id, product_id, new_local_amount):
    print(f"UPDATE jingyu_prepaid SET local_amount = '{new_local_amount}' WHERE id = {order_id} and product_id = '{product_id}';")
    table_name = f"t_order_{user_id[-2:]}"
    print(f"UPDATE {table_name} SET price_local = '{new_local_amount}' WHERE prepaid_id = {order_id} and iap_product_id = '{product_id}';")

def parse_amount(amount_str):
    """解析金额字符串，返回货币符号和金额"""
    match = re.match(r'([A-Z]{3})\s*([\d.]+)', amount_str)
    if match:
        return match.group(1), Decimal(match.group(2))
    
    c2 = amount_str[:2]
    c1 = amount_str[:1]

    if c2 in currency_symbol_2c_map:
        currency = currency_symbol_2c_map[c2]
        return currency, Decimal(amount_str[2:])
    if c1 in currency_symbol_1c_map:
        currency = currency_symbol_1c_map[c1]
        return currency, Decimal(amount_str[1:])
    return None, None

def main():
        # 解析命令行参数
    parser = argparse.ArgumentParser(description='调整本地金额')
    parser.add_argument('--internal', required=True, help='内部订单文件路径')
    parser.add_argument('--external', required=True, help='外部苹果结算单文件路径')
    args = parser.parse_args()

    # 文件路径
    internal_file = args.internal
    external_file = args.external
    
    # 创建字典结构
    sku_price_dict = defaultdict(lambda: {})
    sku_country_local_amount_dict = defaultdict(lambda: {})

    geo_reader = geoip2.database.Reader("/Users/sandyanz/dev/data/GeoLite2-City.mmdb")
    
    # 读取外部订单表并创建字典
    with open(external_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='\t')
        # 获取列名
        headers = reader.fieldnames
        
        # 处理每一行数据
        for row in reader:
            if row['Sale or Return'] == 'R':
                continue
                
            sku = row['SKU'].strip()
            price = Decimal(row['Customer Price'].strip())
            currency = row['Customer Currency'].strip()
            country = row['Country of Sale'].strip()
            
            if not sku or not price or not currency:
                print("跳过空值行")
                continue
                
            if currency not in sku_price_dict[sku]:
                sku_price_dict[sku][currency] = defaultdict(int)
            sku_price_dict[sku][currency][price] += 1

            local_amount = f'{currency}{price}'
            if country not in sku_country_local_amount_dict[sku]:
                sku_country_local_amount_dict[sku][country] = defaultdict(int)
            sku_country_local_amount_dict[sku][country][local_amount] += 1
    
    # 读取内部订单表
    with open(internal_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='\t')
        
        for row in reader:
            order_id = row['id']
            user_id = row['qid']
            product_id = row['product_id']
            local_amount = row['local_amount']
            ip = row['ip']
            try:
                country = geo_reader.city(ip).country.iso_code
            except Exception as e:
                print(f"IP解析失败: {ip}")
                country = 'US'
            
            if product_id in sku_price_dict:
                external_currency_prices = sku_price_dict[product_id]

                currency, amount = parse_amount(local_amount)
                # 1. 货币符号为错误
                if currency is None:
                    has_adjust = False
                    try:
                        dec = Decimal(local_amount)
                        for ext_currency, prices in external_currency_prices.items():
                            if dec in prices:
                                new_local_amount = f"{ext_currency}{dec}"
                                print(f"无货币符号-更新金额: {row['id']}, 商品ID: {row['product_id']}, 旧金额: {local_amount}, 新金额: {new_local_amount}")
                                down_update_sql(user_id, order_id, product_id, new_local_amount)
                                has_adjust = True
                                break
                    except Exception as e:
                        print(f">>>>>货币符号为空, 金额错误，不修正 - 订单ID: {row['id']}, 商品ID: {row['product_id']}, 国家: {country} 金额: {local_amount} 外部金额: {external_currency_prices}")
                    
                    # 走这里，说明没有货币符号只有金额, 根据IP修正货币符号
                    if not has_adjust:
                        if country in sku_country_local_amount_dict[product_id]:
                            local_amounts = sku_country_local_amount_dict[product_id][country]
                            max_count_local_amount = max(local_amounts.items(), key=lambda x: x[1])[0]
                            new_local_amount = max_count_local_amount
                            print(f"没有货币符号-根据IP修正-更新金额: {row['id']}, 商品ID: {row['product_id']}, 旧金额: {local_amount}, 新金额: {new_local_amount}")
                            down_update_sql(user_id, order_id, product_id, new_local_amount)
                        else:
                            print(f"没有货币符号-国家码不存在不修正-订单ID: {row['id']}, 商品ID: {row['product_id']}, 国家: {country} 金额: {local_amount}")
                    continue
                
                if currency in sku_price_dict[product_id]:
                    prices = sku_price_dict[product_id][currency]
                    # 2. 货币符号正确，金额错误，用苹果结算单金额修正
                    if amount not in prices:
                        new_price = max(prices.items(), key=lambda x: x[1])[0]
                        new_local_amount = f"{currency}{new_price}"   
                        print(f"金额不匹配，更新金额 - 订单ID: {row['id']}, 商品ID: {row['product_id']}, 旧金额: {local_amount}, 新金额: {new_local_amount}")
                        down_update_sql(user_id, order_id, product_id, new_local_amount)
                    else:
                        # 3. 货币符号正确，金额正确，不修正
                        new_local_amount = f'{currency}{amount}'
                        if new_local_amount != local_amount:
                            print(f"货币符号修正，更新金额 - 订单ID: {row['id']}, 商品ID: {row['product_id']}, 旧金额: {local_amount}, 新金额: {new_local_amount}")
                            down_update_sql(user_id, order_id, product_id, new_local_amount)

                    # 货币符号不匹配
                    print(f"货币符号不匹配 - 订单ID: {row['id']}, 商品ID: {row['product_id']}, 内部金额: {local_amount}, {currency} {amount} 外部价格: {sku_price_dict[product_id]}")
            else:
                print(f"商品不存在：{row}")

if __name__ == "__main__":
    main()
