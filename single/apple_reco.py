import csv
import re
from decimal import Decimal

def load_exchange_rate():
    exchange_rate_map = {}
    pattern = r'\(([A-Z]{3})\)'  # 只匹配括号内的3个大写字母

    with open("./data/apple/apple_financial_report.csv", 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file, delimiter=',')
        for row in reader:
            currency_raw = row['Country or Region (Currency)']
            exchange_rate = row['Exchange Rate']
            match = re.search(pattern, currency_raw)
            if match:
                currency = match.group(1)
                exchange_rate_map[currency] = Decimal(exchange_rate)
            else:
                print("currency not match: ", currency_raw)

    return exchange_rate_map


def load_transactions(exchange_rate_map):
    data_files = [
        "./data/apple/apple_settlement_202501.csv",
        "./data/apple/apple_settlement_202502.csv",
    ]

    total_amount = 0
    total_count = 0
    refund_amount = 0
    refund_count = 0
    for file in data_files:
        with open(file, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file, delimiter='\t')
            for row in reader:
                if not row['Transaction Date'].startswith('01/'):
                    continue    

                # print(row)
                currency = row['Customer Currency']
                price = Decimal(row['Customer Price'])
                quantity = int(row['Quantity'])

                total_price = price * quantity * exchange_rate_map[currency]
                sale_or_return = row['Sale or Return']
                if sale_or_return == 'S':
                    total_amount += total_price
                    total_count += quantity
                else:
                    refund_amount += total_price
                    refund_count += quantity

    print(f"total_amount: {total_amount}, refund_amount: {refund_amount}, total_count: {total_count}, refund_count: {refund_count}")


def main():
    exchange_rate_map = load_exchange_rate()
    load_transactions(exchange_rate_map)

if __name__ == "__main__":
    main()

