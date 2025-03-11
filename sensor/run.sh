#!/bin/sh

nohup python sql_to_csv.py --sql-file query_af_passback_ios.sql --start-date 2020-10-01 --end-date 2025-02-28 --output af_passback_ios.csv --debug --project MB_project --log-file innovel-ios.log &
nohup python sql_to_csv.py --sql-file query_af_passback_android.sql --start-date 2020-05-01 --end-date 2025-02-28 --output af_passback_android.csv --debug --project  D_In_Project --log-file innovel-android.log &