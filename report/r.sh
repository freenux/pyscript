#!/bin/bash

for ((i = 0; i < 100; i++))
do
    n=`printf "%02d" $i `
    table_name="jingyu_user_pay_${n}"
    conn main "select * from ${table_name} WHERE novel_id = 6132655 and chapter_id = 22243511" >> ${table_name}.csv
done