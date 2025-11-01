#!/bin/bash

# Set up logging
LOG_DIR="logs"
LOG_FILE="$LOG_DIR/update-$(date +\%Y\%m\%d).log"

# Create log directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Function to log messages
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Function to handle errors
handle_error() {
    log_message "ERROR: $1"
    exit 1
}

# Check if required arguments are provided
if [ $# -lt 2 ]; then
    FROM_HOUR=$(TZ=Asia/Shanghai date -d "2 hour ago" +"%Y-%m-%d %H:00:00")
    TO_HOUR=$(TZ=Asia/Shanghai date -d "1 hour ago" +"%Y-%m-%d %H:00:00")
else
    FROM_HOUR="$1"
    TO_HOUR="$2"
fi

# Validate time format
if ! date -d "$FROM_HOUR" > /dev/null 2>&1 || ! date -d "$TO_HOUR" > /dev/null 2>&1; then
    handle_error "Invalid time format. Please use format: 'YYYY-MM-DD HH:MM:SS'"
fi

FROM_HOUR_STR=$(echo $FROM_HOUR | sed 's/[ -:]//g')
TO_HOUR_STR=$(echo $TO_HOUR | sed 's/[ -:]//g')

log_message "Starting data processing for time range: $FROM_HOUR to $TO_HOUR"

sensor_data_file="./data/last_login_users-${FROM_HOUR_STR}-${TO_HOUR_STR}.csv"

# Pull data from the specified time range
log_message "Pulling data from sensor"
python dump-sensor-from-sql.py \
--project d_project \
--sql-file ./config/query_start_user.sql \
--out $sensor_data_file \
--start-time "$FROM_HOUR" \
--end-time "$TO_HOUR" \
--log-file ./${LOG_DIR}/sql_to_csv.log

if [ $? -ne 0 ]; then
    handle_error "Failed to pull data from sensor"
fi

sql_file="./data/update-user-hub-${FROM_HOUR_STR}-${TO_HOUR_STR}.sql"

# Process the data
log_message "Processing data..."
python process_user_ids.py \
--input $sensor_data_file \
--batch-size 100 \
--sql-file "${sql_file}" \
--log-file ./${LOG_DIR}/update-user-hub.log

if [ $? -ne 0 ]; then
    handle_error "Failed to process data"
fi

log_message "Data processing completed successfully"

exit 0

