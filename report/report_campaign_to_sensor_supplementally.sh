#! /bin/bash

if [ -z "$1" ]; then
    DATE=$(date +%Y-%m-%d -d "2 day ago")
else
    DATE=$1
fi

SENSOR_DATA_FILE=./data/no_campaign_users.$DATE.csv
CAMPAIGN_DATA_FILE=./data/campaign_data.$DATE.jsonl

# 1. 导出没有归因数据的神策用户
echo "Start to export no campaign users from sensor for $DATE"
python sql_to_csv.py \
    --project d_project \
    --sql-file query_no_campaign_users.sql \
    --out $SENSOR_DATA_FILE \
    --start-date $DATE \
    --end-date $DATE \
    --log-file ./logs/sql_to_csv.$DATE.log
if [ $? -ne 0 ]; then
    echo "Export no campaign users from sensor failed"
    exit 1
else
    echo "Export no campaign users from sensor done"
fi

# 2. 查询归因数据
echo "Start to query campaign data for $DATE"
if [ -f $SENSOR_DATA_FILE ]; then
    python query-campaign.py \
        --sensor_data_file $SENSOR_DATA_FILE \
        --output_file $CAMPAIGN_DATA_FILE \
        --log_file ./logs/query-campaign.$DATE.log
    if [ $? -ne 0 ]; then
        echo "Query campaign data failed"
        exit 1
    else
        echo "Query campaign data done"
    fi
fi

# 3. 归因数据上报到神策
echo "Start to report campaign data to sensor for $DATE"
if [ -f $CAMPAIGN_DATA_FILE ]; then
    python report-campaign.py \
        --project_name d_project \
        --campaign_data_file $CAMPAIGN_DATA_FILE
    if [ $? -ne 0 ]; then
        echo "Report campaign data to sensor failed"
        exit 1
    else
        echo "Report campaign data to sensor done"
    fi
fi

