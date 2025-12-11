import boto3
import argparse
import datetime
import sys
import os


def download_raw_data_from_s3(bucket_name, s3_key, local_path):
    s3 = boto3.Session(profile_name='s3').client('s3')
    s3_path = f's3://{bucket_name}/{s3_key}'
    print(f'Downloading raw data file from {s3_path} to {local_path}')
    s3.download_file(bucket_name, s3_key, local_path)
    return local_path


def parse_date_range(date_str):
    """Parse date string, support both single date and date range format"""
    if '-' in date_str:
        # Date range format: YYYYMMDD-YYYYMMDD
        start_date_str, end_date_str = date_str.split('-')
        start_date = datetime.datetime.strptime(start_date_str, '%Y%m%d')
        end_date = datetime.datetime.strptime(end_date_str, '%Y%m%d')

        # Generate all dates between start and end (inclusive)
        dates = []
        current_date = start_date
        while current_date <= end_date:
            dates.append(current_date.strftime('%Y%m%d'))
            current_date += datetime.timedelta(days=1)
        return dates
    else:
        # Single date format: YYYYMMDD
        return [date_str]


def get_appsflyer_app_id(app_name: str, platform = None) -> list[str]|None:
    appsflyer_app_config = {
        "dreame": {
            "android": "com.dreame.reader",
            "ios": "id1421091911",
        },
        "innovel": {
            "android": "com.dreame.reader.indonesia",
            "ios": "id1529082813",
        },
        "starynovel": {
            "android": "com.dreame.unlimited.gp",
            "ios": "id1562061999",
        },
        "short": {
            "android": "com.stary.dreamshort",
            "ios": "id6462846884",
        },
        "wehear": {
            "android": "com.joyreading.wehear",
            "ios": "id1560926379",
        },
    }

    app_name = app_name.lower()
    if app_name not in appsflyer_app_config:
        return None

    if platform is None:
        return list(appsflyer_app_config[app_name].values())
    else:
        platform = platform.lower()
        if platform not in appsflyer_app_config[app_name]:
            return None

        return [appsflyer_app_config[app_name][platform]]


def main():
    parser = argparse.ArgumentParser(description='Download Appsflyer raw data files from s3')
    parser.add_argument('-a', '--app', type=str, default='dreame', help='App name')
    parser.add_argument('-p', '--platform', type=str, help='Platform (android, ios)')
    parser.add_argument('-d', '--date', type=str, help='Date in format YYYYMMDD or date range YYYYMMDD-YYYYMMDD')
    parser.add_argument('-t', '--target_local_path', type=str, default='.', help='Target local path to save the raw data file')
    parser.add_argument('-c', '--conversion_type', type=str, default='install', help='Conversion type (install, reinstall)')
    args = parser.parse_args()

    if args.date is None:
        date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
        dates = [date]
        print(f'No date provided, using yesterday date: {date}')
    else:
        try:
            dates = parse_date_range(args.date)
            print(f'Processing dates: {dates[0]} to {dates[-1]} ({len(dates)} days)')
        except ValueError as e:
            print(f'Invalid date format: {args.date}. Use YYYYMMDD or YYYYMMDD-YYYYMMDD', file=sys.stderr)
            return

    if args.conversion_type not in ('install', 'reinstall'):
            print(f'Invalid convertion type: {args.conversion_type}')
            return

    app_ids = get_appsflyer_app_id(args.app, args.platform)
    if app_ids is None:
        print(f'Invalid app name: {args.app} or platform: {args.platform}', file=sys.stderr)
        return

    total_files = len(app_ids) * len(dates)
    current_file = 0

    for date in dates:
        for app_id in app_ids:
            current_file += 1
            conversion_type = args.conversion_type
            s3_key = f'afRawData/{date}/{conversion_type}/{app_id}.txt'

            local_path = f'./data/appsflyer/{args.app}/{app_id}_{conversion_type}_{date}.txt'
            # is dirname of local_path not exists, create it
            if not os.path.exists(os.path.dirname(local_path)):
                os.makedirs(os.path.dirname(local_path))

            print(f'[{current_file}/{total_files}] {s3_key} -> {local_path}')
            try:
                download_raw_data_from_s3('stary-emr', s3_key, local_path)
            except Exception as e:
                print(f'Error downloading {s3_key}: {e}', file=sys.stderr)


if __name__ == "__main__":
    main()

