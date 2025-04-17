import sensorsanalytics
import json
import re
import argparse

def is_login_id(login_id: str) -> bool:
    """判断login_id是否是login id: 即符合[0-9]{10}"""
    return len(login_id.strip()) == 10 and re.match(r'^[0-9]{10}$', login_id.strip())

def report_campaign_data_to_sensor(sa, file: str):
    with open(file, 'r', encoding='utf-8') as f:
        for line in f:
            data = json.loads(line)
            distinct_id = data['sensor_data']['second_id']
            campaign_data = data['campaign_data']
            if len(distinct_id) == 0:
                distinct_id = data['sensor_data']['first_id']
                if len(distinct_id) == 0:
                    continue

            profile_data = {
                "IP":          campaign_data.get('ip', ''),
                "mediasource": campaign_data.get('media_source', ''),
                "Ad":          campaign_data.get('af_ad', ''),
                "Adset":       campaign_data.get('af_adset', ''),
                "Partner":     campaign_data.get('af_prt', ''),
                "Campaign":    campaign_data.get('campaign', ''),
                "CampaignId":  campaign_data.get('af_c_id', ''),
                "AdsetId":     campaign_data.get('af_adset_id', ''),
                "adgroup":     campaign_data.get('adgroup', ''),
            }
            if len(campaign_data.get('advertising_id', '')) > 0:
                profile_data['googleAdId'] = campaign_data['advertising_id']

            sa.profile_set_once(distinct_id, profile_data, is_login_id=is_login_id(distinct_id))
            print('report distinct_id:', distinct_id)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--campaign_data_file', type=str, required=True)
    parser.add_argument('--project_name', type=str, required=True)
    args = parser.parse_args()

    consumer = sensorsanalytics.BatchConsumer(
        'http://bi.dreame.com:8106/sa',
        max_size=100,
        # write_data=True,
        request_timeout=60
    )
    
    sa = sensorsanalytics.SensorsAnalytics(consumer, project_name=args.project_name)
    report_campaign_data_to_sensor(sa, args.campaign_data_file)
    sa.close()

if __name__ == '__main__':
    main()
