import os
import json
import requests
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build

class h_function:
    def get_service():
    
        SERVICE_ACCOUNT_PRIVATE_KEY = os.environ['SERVICE_ACCOUNT_PRIVATE_KEY']
        SERVICE_ACCOUNT_EMAIL = os.environ['SERVICE_ACCOUNT_EMAIL']
        PRIVATE_KEY_ID = os.environ['PRIVATE_KEY_ID']
        PROJECT_ID = os.environ['PROJECT_ID']
        CLIENT_ID = os.environ['CLIENT_ID']
        
        service_account_info = {
            "type": "service_account",
            "project_id": PROJECT_ID,
            "private_key_id": PRIVATE_KEY_ID,
            "private_key": SERVICE_ACCOUNT_PRIVATE_KEY.replace('\\n', '\n'),
            "client_email": SERVICE_ACCOUNT_EMAIL,
            "client_id": CLIENT_ID,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/"+SERVICE_ACCOUNT_EMAIL
        }

        credentials = service_account.Credentials.from_service_account_info(
            service_account_info, scopes=['https://www.googleapis.com/auth/spreadsheets.readonly'])

        return build('sheets', 'v4', credentials=credentials)
    
    def get_account(service,sheet_range,gs_key,target_column):
        sheet = service.spreadsheets()
        ggsheet = sheet.values().get(spreadsheetId=gs_key,range=sheet_range).execute().get('values',[])
        adid = pd.DataFrame(ggsheet[1:], columns=ggsheet[0])
        ad_list = adid[adid['status'] == 'active'][target_column].to_list()
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