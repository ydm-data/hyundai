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

    def send_line_noti(msg):
        url = 'https://notify-api.line.me/api/notify'
        token = os.environ['LINETOKEN']
        headers = {'content-type':'application/x-www-form-urlencoded','Authorization':'Bearer '+token}
        r = requests.post(url, headers=headers, data = {'message':msg})
        return json.dumps({'success': msg}), 200
    
    def send_gg_chat_noti(msg):
        chat_key = os.environ['GG_chat_key']
        webhook_url = f"https://chat.googleapis.com/v1/spaces/AAAAlqqeJy0/messages?key={chat_key}"
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
        chat_key = os.environ['GG_chat_key']
        webhook_url = f"https://chat.googleapis.com/v1/spaces/AAAAlqqeJy0/messages?key={chat_key}"
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