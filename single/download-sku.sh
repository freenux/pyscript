#!/bin/bash

DOWNLOAD_FILEPATH=./data/dreame-sku-price.txt

redis-cli \
-h payment-1.1jegko.ng.0001.use1.cache.amazonaws.com \
-p 16380 \
hgetall iaps_cache_iap_product_local_price_v3_google_country_1 \
| sed 'N;s/\n/\t/g' \
> $DOWNLOAD_FILEPATH

redis-cli \
-h payment-1.1jegko.ng.0001.use1.cache.amazonaws.com \
-p 16380 \
hgetall iaps_cache_iap_product_local_price_v3_google_currency_1 \
| sed 'N;s/\n/\t/g' \
>> $DOWNLOAD_FILEPATH

redis-cli \
-h payment-1.1jegko.ng.0001.use1.cache.amazonaws.com \
-p 16380 \
hgetall iaps_cache_iap_product_local_price_v3_apple_country_1 \
| sed 'N;s/\n/\t/g' \
>> $DOWNLOAD_FILEPATH

redis-cli \
-h payment-1.1jegko.ng.0001.use1.cache.amazonaws.com \
-p 16380 \
hgetall iaps_cache_iap_product_local_price_v3_apple_currency_1 \
| sed 'N;s/\n/\t/g' \
>> $DOWNLOAD_FILEPATH


