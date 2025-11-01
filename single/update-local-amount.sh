#!/bin/bash

cat logs/sql-result.txt | grep -v local_amount | while read id qid local_amount
do
    # 获取qid的后2位
    table_name="t_order_${qid: -2}"
    echo "UPDATE ${table_name} SET price_local = ${local_amount} WHERE prepaid_id = ${id}"
done