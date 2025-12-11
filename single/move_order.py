import csv

def down_update_sql(user_id, order_id, local_amount):
    table_name = f"t_order_{user_id[-2:]}"
    print(f"UPDATE {table_name} SET price_local = {local_amount} WHERE prepaid_id = {order_id};")

def main():
    order_user_map = {}
    with open("Feb_order.csv", "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            user_id = row["qid"]
            order_id = row["id"]
            order_user_map[order_id] = user_id

    with open("update_local_amount.sql", "r", encoding="utf-8") as f:
        for line in f:
            fields = line.split()
            local_amount = fields[5]
            order_id = fields[9]
            if order_id in order_user_map:
                user_id = order_user_map[order_id]
                down_update_sql(user_id, order_id, local_amount)
            

if __name__ == "__main__":
    main()