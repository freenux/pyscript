#!/bin/bash

python3 fix-order-local-amount.py \
--mycli-config=/home/developer/.myclirc \
--dsn=main \
--start-time="2023-04-01 00:00:00" \
--end-time="2023-04-01 23:59:59" \
--sku-file=./data/dreame-sku-price.txt \
--geoip-db=/home/q/system/payment-center/storage/data/GeoLite2-City.mmdb \
--debug

# Process each day in April 2025
for day in {02..30}; do
  echo "Processing April $day, 2025..."
  
  python3 fix-order-local-amount.py \
  --mycli-config=/home/developer/.myclirc \
  --dsn=wmain \
  --start-time="2025-04-$day 00:00:00" \
  --end-time="2025-04-$day 23:59:59" \
  --sku-file=./data/dreame-sku-price.txt \
  --geoip-db=/home/q/system/payment-center/storage/data/GeoLite2-City.mmdb
  
  echo "Completed April $day, 2025"
  echo "------------------------"
done
