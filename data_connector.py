import os
import json
import time
import pytz
import requests
import pandas as pd
import datetime as dt
from datetime import timedelta

from dotenv import load_dotenv
from google.cloud import bigquery

class Connector:

    def load_data(client, dataset_name, table_name, data):
        dataset = client.dataset(dataset_name)
        table_id = dataset.table(table_name)

        job_config = bigquery.LoadJobConfig()
        load_job = client.load_table_from_dataframe(data, table_id, job_config = job_config)
        return load_job.result()


    def delete_data(client, dataset_name, table_name):
        """Delete yesterday data from temp table"""

        delete_statement = (
        "TRUNCATE TABLE " + dataset_name + "." + table_name + " "
        )
        query_job = client.query(delete_statement)
        query_job.result()
        return True
        
    def delete_when_match(client, dataset_name_ori, table_name_ori,dataset_name_temp,table_name_temp,condition):
        """Delete existing data from original when match"""

        delete_statement = (
        "MERGE "+ dataset_name_ori + "." + table_name_ori + " ori "
        "USING "+ dataset_name_temp + "." + table_name_temp + " temp "
        + condition +
        "WHEN MATCHED THEN DELETE"
        )
        query_job = client.query(delete_statement)
        query_job.result()
        return True

    def get_Zocialeye_token():
        generate_token_url = "https://apix.zocialeye.com/generate-token"
        authen_info = '''{
        "username": "Ydm_admin",
        "password": "ydm2019"
        }'''
        headers = {
        'Content-Type': 'application/json'
        }
        authen = requests.post(generate_token_url, headers=headers, data=authen_info)
        token = authen.json()['token']
        return token
    
    def get_Zocialeye_campaign_list(token):
        headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + token
        }

        campaign_response = requests.get('https://apix.zocialeye.com/api/v1/campaigns/list', headers=headers)
        return campaign_response.json()
    
    def get_Zocialeye_message(campaign_list,token,start,end,days):
        
        date_end = dt.datetime.now()
        date_start =  date_end - dt.timedelta(days=days)
        unix_date_start = int(date_start.timestamp())
        unix_date_end = int(date_end.timestamp())
        
        campaign_message_list = []
        for i in range(start,end):   
            for each_channel in ['facebook','x','instagram','tiktok','forum','news','blog','youtube']:
                stop = 0
                start_form_index = 0
                while stop == 0:
                    values = {
                        "date_start": unix_date_start,
                        "date_end": unix_date_end,
                        "total": 50,
                        "from": start_form_index,
                        "filter": {
                        "channel": each_channel
                        }
                    }
                    
                    headers = {
                    'Content-Type': 'application/json',
                    'Authorization': 'Bearer ' + token
                    }
                    
                    id = campaign_list[i]['campaign_id']
                    campaign_message_response = requests.post(f'https://apix.zocialeye.com/api/v1/campaigns/{id}/messages', data=json.dumps(values), headers=headers)
                    campaign_message = campaign_message_response.json()
                    
                    for each_campagin_message in campaign_message:
                        each_campagin_message['campaign_name'] = campaign_list[i]['campaign_name']
                        each_campagin_message['campaign_id'] = campaign_list[i]['campaign_id']
                    campaign_message_list += campaign_message

                    start_form_index += 50

                    if len(campaign_message) < 50:
                        stop = 1

                time.sleep(2)

            time.sleep(4)
            campaign_data = pd.json_normalize(campaign_message_list)
            campaign_data = campaign_data.rename(columns=lambda x: x.replace('.', '_'))
        
        return campaign_data
    
    def get_Zocialeye_wordcloud(token,day):
        current_date = dt.datetime.now()
        date_start =  current_date - timedelta(days=day)
        date_end = date_start + timedelta(days=1)
        unix_date_start = int(date_start.timestamp())
        unix_date_end = int(date_end.timestamp())
        
        result_list = []
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + token
        }

        while date_end < current_date:
            print(date_start, date_end)
            values = {
                "date_start": unix_date_start,
                "date_end": unix_date_end
            }

            response = requests.post('https://apix.zocialeye.com/api/v1/campaigns/101915/wordcloud', data=json.dumps(values), headers=headers)
            data = response.json()
            for item in data:
                item['date'] = date_start.strftime("%Y-%m-%d")
            result_list += data

            date_start += timedelta(days=1)
            date_end += timedelta(days=1)
            
        wordcloud_df = pd.DataFrame(result_list)
        return wordcloud_df


    def send_line_noti(msg):
        url = 'https://notify-api.line.me/api/notify'
        token = os.environ['LINETOKEN']
        headers = {'content-type':'application/x-www-form-urlencoded','Authorization':'Bearer '+token}
        r = requests.post(url, headers=headers, data = {'message':msg})
        return json.dumps({'success': msg}), 200
    
    def send_gg_chat_noti(msg):
        webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAlqqeJy0/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=RI1VkC7IEXTxasururCjWNgjfj5XFAQNs8dfruUumNU"
        message = {
            "cards_v2" : [{ 
                "card": {
                    "sections": [{
                        "widgets": [
                            {
                                "textParagraph": {
                                    "text": msg \
                                    + dt.datetime.now(pytz.timezone('Asia/Bangkok')).strftime('%Y/%m/%d %H:%M:%S') \
                                    + " 🎉"
                                }
                            }
                        ]
                    }]
                }
            }]
        }
        response = requests.post(webhook_url, headers={"Content-Type": "application/json"}, data=json.dumps(message))
        return json.dumps({'success': msg}), 200
    
    def send_gg_chat_noti_with_divider(msg1,msg2,msg3,msg4,msg5):
        webhook_url = "https://chat.googleapis.com/v1/spaces/AAAAlqqeJy0/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=RI1VkC7IEXTxasururCjWNgjfj5XFAQNs8dfruUumNU"
        message = {
            "cards_v2" : [{ 
                "card": {
                    "sections": [{
                        "header": "<b>Media Most Recent Data</b>",
                        "widgets": [
                            {
                                "textParagraph": {
                                    "text": msg1
                                }
                            },
                            {
                            "divider": {}
                            },
                            {
                                "textParagraph": {
                                    "text": msg2
                                }
                            },
                            {
                            "divider": {}
                            },
                            {
                                "textParagraph": {
                                    "text": msg3
                                }
                            },
                            {
                            "divider": {}
                            },
                            {
                                "textParagraph": {
                                    "text": msg4
                                }
                            },
                            {
                            "divider": {}
                            },
                            {
                                "textParagraph": {
                                    "text": msg5
                                }
                            }
                        ]
                    }]
                }
            }]
        }
        response = requests.post(webhook_url, headers={"Content-Type": "application/json"}, data=json.dumps(message))
        return json.dumps({'success': "success checking data"}), 200
    

    # Function to calculate the midpoint
    def get_midpoint(account_label):
        account_label_ranges = {
            "pico": (0, 1000),
            "nano": (1001, 10000),
            "micro": (10001, 50000),
            "mid-tier": (50001, 500000),
            "macro": (500001, 1000000),
            "mega": (1000001, 5000000),
            "elite": (5000001, float('inf'))  # 'inf' for no upper limit
        }
        min_range, max_range = account_label_ranges[account_label]
        if max_range == float('inf'):  # Handle 'elite' with no upper bound
            return min_range  # Return min_range as a fallback for elite
        return (min_range + max_range) // 2  # Calculate the midpoint
    
    def get_account_label(follower_count):
        account_label_ranges = {
            "pico": (0, 1000),
            "nano": (1001, 10000),
            "micro": (10001, 50000),
            "mid-tier": (50001, 500000),
            "macro": (500001, 1000000),
            "mega": (1000001, 5000000),
            "elite": (5000001, float('inf'))  # 'inf' for no upper limit
        }
        for label, (min_range, max_range) in account_label_ranges.items():
            if max_range == float('inf'):  # Handle 'elite' with no upper bound
                if follower_count >= min_range:
                    return label
            elif min_range <= follower_count <= max_range:
                return label
        return ''
    
    def update_category(row):
        category_set = set(row['category'])  # Convert category list to a set for fast lookup
        for logo in row['logo_detections']:
            if logo not in category_set:  # Add logo if not already in category
                category_set.add(logo)
        return list(category_set)