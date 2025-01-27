import os
import json
import pytz
import requests
import pandas as pd
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build

class h_function:
    def get_service():
        service_account_info = json.loads(os.environ.get("hmth-bigquery"))
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info, scopes=['https://www.googleapis.com/auth/spreadsheets.readonly'])
        return build('sheets', 'v4', credentials=credentials)
    
    def get_account(service,sheet_range,gs_key,target_column,channel):
        sheet = service.spreadsheets()
        ggsheet = sheet.values().get(spreadsheetId=gs_key,range=sheet_range).execute().get('values',[])
        adid = pd.DataFrame(ggsheet[1:], columns=ggsheet[0])
        ad_list = adid[adid['Channel'] == channel][target_column].to_list()
        return ad_list
    
    def get_account_batch_run(service,sheet_range,gs_key,round,target_column):
        sheet = service.spreadsheets()
        ggsheet = sheet.values().get(spreadsheetId=gs_key,range=sheet_range).execute().get('values',[])
        adid = pd.DataFrame(ggsheet[1:], columns=ggsheet[0])
        ad_list = adid[adid['round'] == round][target_column].to_list()
        return ad_list
    
    def send_line_noti(msg):
        url = 'https://notify-api.line.me/api/notify'
        token = os.environ['LINETOKEN']
        headers = {'content-type':'application/x-www-form-urlencoded','Authorization':'Bearer '+token}
        r = requests.post(url, headers=headers, data = {'message':msg})
        return json.dumps({'success': msg}), 200
    
    def convert_timestamp(value):
        if pd.isna(value):
            return None
        if isinstance(value, pd.Timestamp):
            return value.strftime('%Y-%m-%d %H:%M:%S')
        return value
    
    def send_gg_chat_noti(msg):
        chat_key = os.environ['GG_CHAT_KEY']
        chat_token = os.environ['GG_CHAT_TOKEN']
        webhook_url = f"https://chat.googleapis.com/v1/spaces/AAAAlqqeJy0/messages?key={chat_key}&token={chat_token}"
        message = {
            "cards_v2" : [{ 
                "card": {
                    "sections": [{
                        "widgets": [
                            {
                                "textParagraph": {
                                    "text": msg \
                                    + datetime.now(pytz.timezone('Asia/Bangkok')).strftime('%Y/%m/%d %H:%M:%S') \
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
        chat_key = os.environ['GG_CHAT_KEY']
        chat_token = os.environ['GG_CHAT_TOKEN']
        webhook_url = f"https://chat.googleapis.com/v1/spaces/AAAAlqqeJy0/messages?key={chat_key}&token={chat_token}"
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