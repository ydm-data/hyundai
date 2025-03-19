import pandas as pd
import json
import requests

from dotenv import load_dotenv
from datetime import datetime, timedelta, date
import pytz
import logging
from flask import Flask, request
import os

from google.cloud import bigquery
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

from connector_FB import FB_Connector
from connector_GG import GG_Connector
from connector_BQ import BQ_Connector
from connector_TT import TT_connector
from helper_function import h_function

from google.oauth2 import service_account
from google.ads.googleads.client import GoogleAdsClient

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)

""" TIKTOK MEDIA API """

@app.route('/update_tiktok_daily', methods=['POST'])
def update_tiktok_daily():
    service = h_function.get_service()
    advertiser_ids_list = h_function.get_account(service,"Media Account!A1:ZZ",'1S1Ew5r7RL9zvpvZc-Azd8Mc8tkAikitkw2mgAcAb4Ro',"Account ID", "Tiktok")
    metrics_list = TT_connector.get_main_metrics()
    data = TT_connector.get_data(advertiser_ids_list,metrics_list,14)
    
    if len(data) > 0:
        data = TT_connector.convert_main_data(data)    
        
        project_id = 'hmth-448709'
        client = bigquery.Client(project=project_id)   
        BQ_Connector.delete_data(client,"rda_analytics_temp","media_tiktok_main_temp")
        BQ_Connector.load_data(client,"rda_analytics_temp","media_tiktok_main_temp",data)
        
        condition = "ON (ori.ad_id = temp.ad_id AND ori.stat_time_day = temp.stat_time_day AND ori.campaign_id = temp.campaign_id) "
        BQ_Connector.delete_when_match(client,"rda_analytics","media_tiktok_main","rda_analytics_temp","media_tiktok_main_temp",condition)
        BQ_Connector.load_data(client, "rda_analytics", "media_tiktok_main", data)
        
        msg = "🎶 Media: <b>Tiktok Main</b> Executed Successfully on 📅 "
        h_function.send_gg_chat_noti(msg)
    return json.dumps({'success': 'Update Tiktok Completed'}), 200
    
    
@app.route('/update_tiktok_daily_event', methods=['POST'])
def update_tiktok_daily_event():
    service = h_function.get_service()
    advertiser_ids_list = h_function.get_account(service,"Media Account!A1:ZZ",'1S1Ew5r7RL9zvpvZc-Azd8Mc8tkAikitkw2mgAcAb4Ro',"Account ID", "Tiktok")
    metrics_list = TT_connector.get_event_metrics()
    data = TT_connector.get_data(advertiser_ids_list,metrics_list,14)
    
    if len(data) > 0:
        data = TT_connector.convert_event_data(data)
        
        id_columns = ['currency',"campaign_id",'campaign_name',"ad_id",'ad_name','adgroup_id','adgroup_name','advertiser_id','advertiser_name','timezone',"stat_time_day"]
        data = BQ_Connector.pivot_to_nested(data, id_columns,"event")

        project_id = 'hmth-448709'
        client = bigquery.Client(project=project_id)
        BQ_Connector.delete_data(client,"rda_analytics_temp","media_tiktok_event_temp")
        BQ_Connector.load_data(client, "rda_analytics_temp","media_tiktok_event_temp",data)
        condition = "ON (ori.ad_id = temp.ad_id AND ori.stat_time_day = temp.stat_time_day AND ori.campaign_id = temp.campaign_id) "
        BQ_Connector.delete_when_match(client,"rda_analytics","media_tiktok_event","rda_analytics_temp","media_tiktok_event_temp",condition)
        BQ_Connector.load_data(client, "rda_analytics", "media_tiktok_event", data)

        msg = "🎶 Media: <b>Tiktok Event</b> Executed Successfully on 📅 "
        h_function.send_gg_chat_noti(msg)
    return json.dumps({'success': 'Update Tiktok Event Completed'}), 200


@app.route('/update_tiktok_daily_pageevent', methods=['POST'])
def update_tiktok_daily_pageevent():
    service = h_function.get_service()
    advertiser_ids_list = h_function.get_account(service,"Media Account!A1:ZZ",'1S1Ew5r7RL9zvpvZc-Azd8Mc8tkAikitkw2mgAcAb4Ro',"Account ID", "Tiktok")
    metrics_list = TT_connector.get_pageevent_metrics()
    data = TT_connector.get_data(advertiser_ids_list, metrics_list,14)
    
    if len(data) > 0:
        data = TT_connector.convert_pageevent_data(data)
        
        id_columns = ['currency',"campaign_id",'campaign_name',"ad_id",'ad_name','adgroup_id','adgroup_name','advertiser_id','advertiser_name','timezone',"stat_time_day"]
        data = BQ_Connector.pivot_to_nested(data, id_columns,"pageevent")
        
        project_id = 'hmth-448709'
        client = bigquery.Client(project=project_id)   
        BQ_Connector.delete_data(client, "rda_analytics_temp","media_tiktok_pageevent_temp")
        BQ_Connector.load_data(client,"rda_analytics_temp","media_tiktok_pageevent_temp",data)
        condition = "ON (ori.ad_id = temp.ad_id AND ori.stat_time_day = temp.stat_time_day AND ori.campaign_id = temp.campaign_id) "
        BQ_Connector.delete_when_match(client,"rda_analytics","media_tiktok_pageevent","rda_analytics_temp","media_tiktok_pageevent_temp",condition)
        BQ_Connector.load_data(client, "rda_analytics", "media_tiktok_pageevent", data)

        msg = "🎶 Media: <b>Tiktok Page Event</b> Executed Successfully on 📅 "
        h_function.send_gg_chat_noti(msg)
    return json.dumps({'success': 'Update Tiktok Page Event Completed'}), 200


@app.route('/update_tiktok_daily_shopads', methods=['POST'])
def update_tiktok_daily_shopads():
    service = h_function.get_service()
    advertiser_ids_list = h_function.get_account(service,"Media Account!A1:ZZ",'1S1Ew5r7RL9zvpvZc-Azd8Mc8tkAikitkw2mgAcAb4Ro',"Account ID", "Tiktok")
    metrics_list = TT_connector.get_shopads_metrics()
    data = TT_connector.get_data(advertiser_ids_list,metrics_list,14)
    
    if len(data) > 0:
        data = TT_connector.convert_shopads_data(data)
        id_columns = ['currency',"campaign_id",'campaign_name',"ad_id",'ad_name','adgroup_id','adgroup_name','advertiser_id','advertiser_name','timezone',"stat_time_day"]
        data = BQ_Connector.pivot_to_nested(data, id_columns,"shopads")
        
        project_id = 'hmth-448709'
        client = bigquery.Client(project=project_id)   
        BQ_Connector.delete_data(client,"rda_analytics_temp","media_tiktok_shop_ads_temp")
        BQ_Connector.load_data(client, "rda_analytics_temp","media_tiktok_shop_ads_temp",data)
        condition = "ON (ori.ad_id = temp.ad_id AND ori.stat_time_day = temp.stat_time_day AND ori.campaign_id = temp.campaign_id) "
        BQ_Connector.delete_when_match(client,"rda_analytics","media_tiktok_shop_ads","rda_analytics_temp","media_tiktok_shop_ads_temp",condition)
        BQ_Connector.load_data(client, "rda_analytics", "media_tiktok_shop_ads", data)

        msg = "🎶 Media: <b>Tiktok Shop Ads</b> Executed Successfully on 📅 "
        h_function.send_gg_chat_noti(msg)
    return json.dumps({'success': 'Update Tiktok ShopAds Completed'}), 200
        

@app.route('/update_tiktok_lifetime_ad', methods=['POST'])
def update_tiktok_lifetime_ad():
    service = h_function.get_service()
    advertiser_ids_list = h_function.get_account(service,"Media Account!A1:ZZ",'1S1Ew5r7RL9zvpvZc-Azd8Mc8tkAikitkw2mgAcAb4Ro',"Account ID", "Tiktok")
    metrics_list = TT_connector.get_main_metrics_lifetime_ad()
    
    data_level = 'AUCTION_AD'
    dimensions_list = ["ad_id"]
    data = TT_connector.get_data_lifetime(advertiser_ids_list,metrics_list,data_level,dimensions_list)
    
    if len(data) > 0:
        data = TT_connector.convert_main_data_lifetime(data)  
        data['update_date'] = datetime.today()  
        
        project_id = 'hmth-448709'
        client = bigquery.Client(project=project_id)   
        
        BQ_Connector.delete_data(client,"rda_analytics_temp","media_tiktok_lifetime_ad_temp")
        BQ_Connector.load_data(client,"rda_analytics_temp","media_tiktok_lifetime_ad_temp",data)
        BQ_Connector.delete_when_match(client, "rda_analytics","media_tiktok_lifetime_ad","rda_analytics_temp","media_tiktok_lifetime_ad_temp",
                                        "ON (ori.ad_id = temp.ad_id AND ori.update_date = temp.update_date AND ori.advertiser_id = temp.advertiser_id) ")
        BQ_Connector.load_data(client,"rda_analytics","media_tiktok_lifetime_ad",data)
        
        msg = "⌛🎶 Media: <b>Tiktok</b> Lifetime (Ad Level) Executed Successfully on 📅 "
        h_function.send_gg_chat_noti(msg)
    return json.dumps({'success': 'Update Tiktok Ad Lifetime Completed'}), 200
    
    
@app.route('/update_tiktok_lifetime_adgroup', methods=['POST'])
def update_tiktok_lifetime_adgroup():
    service = h_function.get_service()
    advertiser_ids_list = h_function.get_account(service,"Media Account!A1:ZZ",'1S1Ew5r7RL9zvpvZc-Azd8Mc8tkAikitkw2mgAcAb4Ro',"Account ID", "Tiktok")
    metrics_list = TT_connector.get_main_metrics_adgroup()
    
    data_level = 'AUCTION_ADGROUP'
    dimensions_list = ["adgroup_id"]
    data = TT_connector.get_data_lifetime(advertiser_ids_list,metrics_list,data_level,dimensions_list)
    
    if len(data) > 0:
        data = TT_connector.convert_main_data_lifetime(data)  
        data['update_date'] = datetime.today()  
        
        project_id = 'hmth-448709'
        client = bigquery.Client(project=project_id)
        BQ_Connector.delete_data(client,"rda_analytics_temp","media_tiktok_lifetime_adgroup_temp")
        BQ_Connector.load_data(client,"rda_analytics_temp","media_tiktok_lifetime_adgroup_temp",data) 
        BQ_Connector.delete_when_match(client, "rda_analytics","media_tiktok_lifetime_adgroup","rda_analytics_temp","media_tiktok_lifetime_adgroup_temp",
                                        "ON (ori.adgroup_id = temp.adgroup_id AND ori.update_date = temp.update_date AND ori.advertiser_id = temp.advertiser_id) ")
        BQ_Connector.load_data(client,"rda_analytics","media_tiktok_lifetime_adgroup",data)
        
        msg = "⌛🎶 Media: <b>Tiktok</b> Lifetime (AdGroup Level) Executed Successfully on 📅 "
        h_function.send_gg_chat_noti(msg)
    return json.dumps({'success': 'Update Tiktok Adgroup Lifetime Completed'}), 200

    
@app.route('/update_tiktok_lifetime_campaign', methods=['POST'])
def update_tiktok_lifetime_campaign():
    service = h_function.get_service()
    advertiser_ids_list = h_function.get_account(service,"Media Account!A1:ZZ",'1S1Ew5r7RL9zvpvZc-Azd8Mc8tkAikitkw2mgAcAb4Ro',"Account ID", "Tiktok")
    metrics_list = TT_connector.get_main_metrics_campaign()
    
    data_level = 'AUCTION_CAMPAIGN'
    dimensions_list = ["campaign_id"]
    data = TT_connector.get_data_lifetime(advertiser_ids_list,metrics_list,data_level,dimensions_list)
    
    if len(data) > 0:
        data = TT_connector.convert_main_data_lifetime(data)  
        data['update_date'] = datetime.today()  
        
        project_id = 'hmth-448709'
        client = bigquery.Client(project=project_id)  
        BQ_Connector.delete_data(client,"rda_analytics_temp","media_tiktok_lifetime_campaign_temp")
        BQ_Connector.load_data(client,"rda_analytics_temp","media_tiktok_lifetime_campaign_temp",data) 
        BQ_Connector.delete_when_match(client, "rda_analytics","media_tiktok_lifetime_campaign","rda_analytics_temp","media_tiktok_lifetime_campaign_temp",
                                        "ON (ori.campaign_id = temp.campaign_id AND ori.update_date = temp.update_date AND ori.advertiser_id = temp.advertiser_id) ") 
        BQ_Connector.load_data(client,"rda_analytics","media_tiktok_lifetime_campaign",data)
        
        msg = "⌛🎶 Media: <b>Tiktok</b> Lifetime (Campaign Level) Executed Successfully on 📅 "
        h_function.send_gg_chat_noti(msg)
    return json.dumps({'success': 'Update Tiktok Campaign Lifetime Completed'}), 200

    
@app.route('/update_tiktok_lifetime_advertiser', methods=['POST'])
def update_tiktok_lifetime_advertiser(): 
    service = h_function.get_service()
    advertiser_ids_list = h_function.get_account(service,"Media Account!A1:ZZ",'1S1Ew5r7RL9zvpvZc-Azd8Mc8tkAikitkw2mgAcAb4Ro',"Account ID", "Tiktok")
    metrics_list = TT_connector.get_main_metrics_advertiser()
    
    data_level = 'AUCTION_ADVERTISER'
    dimensions_list = ["advertiser_id"]
    data = TT_connector.get_data_lifetime(advertiser_ids_list,metrics_list,data_level,dimensions_list)
    
    if len(data) > 0:
        data = TT_connector.convert_main_data_advertiser_level(data)  
        data['update_date'] = datetime.today()  
        
        project_id = 'hmth-448709'
        client = bigquery.Client(project=project_id)   
        BQ_Connector.delete_data(client,"rda_analytics_temp","media_tiktok_lifetime_advertiser_temp")
        BQ_Connector.load_data(client,"rda_analytics_temp","media_tiktok_lifetime_advertiser_temp",data) 
        BQ_Connector.delete_when_match(client, "rda_analytics","media_tiktok_lifetime_advertiser","rda_analytics_temp","media_tiktok_lifetime_advertiser_temp",
                                        "ON (ori.advertiser_id = temp.advertiser_id AND ori.update_date = temp.update_date) ") 
        BQ_Connector.load_data(client,"rda_analytics","media_tiktok_lifetime_advertiser",data)
        
        msg = "⌛🎶 Media: <b>Tiktok</b> Lifetime (Advertiser Level) Executed Successfully on 📅 "
        h_function.send_gg_chat_noti(msg)
    return json.dumps({'success': 'Update Tiktok Advertiser Lifetime Completed'}), 200


@app.route('/update_tiktok_spark_ads', methods=['POST'])
def update_tiktok_spark_ads():
    service = h_function.get_service()
    advertiser_ids_list = h_function.get_account(service,"Media Account!A1:ZZ",'1S1Ew5r7RL9zvpvZc-Azd8Mc8tkAikitkw2mgAcAb4Ro',"Account ID", "Tiktok")
    access_token = os.environ.get("TIKTOKTOKEN")
    all_video_list = TT_connector.get_spark_ads(access_token, advertiser_ids_list)
    if len(all_video_list) > 0:
        flatten_list = TT_connector.flatten_list_of_dicts(all_video_list)
        spark_video = pd.DataFrame(flatten_list)
        client = bigquery.Client()
        BQ_Connector.delete_data(client, "rda_analytics_temp", "media_tiktok_spark_ads_temp")
        BQ_Connector.load_data(client, "rda_analytics_temp", "media_tiktok_spark_ads_temp", spark_video)
        BQ_Connector.delete_when_match(client,"rda_analytics","media_tiktok_spark_ads","rda_analytics_temp","media_tiktok_spark_ads_temp",
                    "ON (ori.item_id = temp.item_id) ")
        BQ_Connector.load_data(client, "rda_analytics", "media_tiktok_spark_ads", spark_video)
    return json.dumps({'success': 'Update Tiktok Spark Ads'}), 200


@app.route('/update_tiktok_ad_info', methods=['POST'])
def update_tiktok_ad_info():
    service = h_function.get_service()
    advertiser_ids_list = h_function.get_account(service,"Media Account!A1:ZZ",'1S1Ew5r7RL9zvpvZc-Azd8Mc8tkAikitkw2mgAcAb4Ro',"Account ID", "Tiktok")
    url = 'https://ads.tiktok.com/open_api/v1.3/ad/get/'
    headers = {
            "Access-Token": os.environ.get("TIKTOKTOKEN"),
            'Content-Type': 'application/json'
        }

    all_ads = TT_connector.get_ad_info_data(advertiser_ids_list, url, headers)
    all_ads_df = TT_connector.transform_ad_info_data(all_ads)
    
    client = bigquery.Client()
    BQ_Connector.delete_data(client, "rda_analytics_temp", "media_tiktok_ad_info_temp")
    BQ_Connector.load_data(client, "rda_analytics_temp", "media_tiktok_ad_info_temp", all_ads_df)
    BQ_Connector.delete_when_match(client, "rda_analytics", "media_tiktok_ad_info", "rda_analytics_temp", "media_tiktok_ad_info_temp","ON (ori.ad_id = temp.ad_id AND ori.advertiser_id = temp.advertiser_id) ")
    BQ_Connector.load_data(client,"rda_analytics", "media_tiktok_ad_info", all_ads_df)
    
    return json.dumps({'success': 'Update Ad info data'}), 200


@app.route('/update_tiktok_video_ad_info', methods=['POST'])
def update_tiktok_video_ad_info():
    service = h_function.get_service()
    advertiser_ids_list = h_function.get_account(service,"Media Account!A1:ZZ",'1S1Ew5r7RL9zvpvZc-Azd8Mc8tkAikitkw2mgAcAb4Ro',"Account ID", "Tiktok")
    all_video_df = pd.DataFrame()
    client = bigquery.Client()

    for advertiser_id in advertiser_ids_list:
        video_id_list_target = TT_connector.get_target_video_list(client, advertiser_id)
        access_token = os.environ.get("TIKTOKTOKEN")
        if len(video_id_list_target) > 0:
            all_video_df = pd.concat([all_video_df,TT_connector.get_all_video(video_id_list_target, advertiser_id, access_token)],ignore_index=True)
    
    if all_video_df.shape[0] > 0:
        BQ_Connector.delete_data(client, "rda_analytics_temp", "media_tiktok_video_ads_info_temp")
        BQ_Connector.load_data(client, "rda_analytics_temp", "media_tiktok_video_ads_info_temp", all_video_df)
        BQ_Connector.delete_when_match(client,"rda_analytics","media_tiktok_video_ads_info","rda_analytics_temp","media_tiktok_video_ads_info_temp",
                    "ON (ori.video_id = temp.video_id) ")
        BQ_Connector.load_data(client, "rda_analytics", "media_tiktok_video_ads_info", all_video_df)
    
    return json.dumps({'success': 'Update Video Ads Info Complete'}), 200


@app.route('/update_exchange_rate_idr_thb', methods=['POST'])
def update_exchange_rate_idr_thb():
    start_date_obj = datetime.now() - timedelta(days=1)
    rate_list = []
    while start_date_obj <= datetime.now():
        date = start_date_obj.strftime("%Y-%m-%d")
        start_date_obj += timedelta(days=1)
        
        url = f"https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@{date}/v1/currencies/idr.json"
        response = requests.get(url)
        temp = {
            "date": date,
            "thb" : response.json()['idr']['thb'],
            "usd" : response.json()['idr']['usd']
        }
        rate_list.append(temp)
        
    rate_df = pd.DataFrame(rate_list)
    rate_df['date'] = pd.to_datetime(rate_df['date'])
    
    client = bigquery.Client()
    BQ_Connector.delete_data(client, "rda_analytics_temp", "currency_exchange_rate_IDR_THB_temp")
    BQ_Connector.load_data(client, "rda_analytics_temp", "currency_exchange_rate_IDR_THB_temp",rate_df)
    BQ_Connector.delete_when_match(client,"rda_analytics", "currency_exchange_rate_IDR_THB","rda_analytics_temp", "currency_exchange_rate_IDR_THB_temp",
                  "ON (ori.date = temp.date)")
    BQ_Connector.load_data(client, "rda_analytics", "currency_exchange_rate_IDR_THB",rate_df)
    
    msg = "💱 <b>Currency Exchange (IDR -> THB, USD):</b> Executed Successfully on 📅 "
    h_function.send_gg_chat_noti(msg)
    return json.dumps({'success': 'Update Currency Exchange Completed'}), 200

@app.route('/update_exchange_rate_thb_usd', methods=['POST'])
def update_exchange_rate_thb_usd():
    start_date_obj = datetime.now() - timedelta(days=1)
    rate_list = []
    while start_date_obj <= datetime.now():
        date = start_date_obj.strftime("%Y-%m-%d")
        start_date_obj += timedelta(days=1)
        
        url = f"https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@{date}/v1/currencies/thb.json"
        response = requests.get(url)
        temp = {
            "date": date,
            "idr" : response.json()['thb']['idr'],
            "usd" : response.json()['thb']['usd']
        }
        rate_list.append(temp)
        
    rate_df = pd.DataFrame(rate_list)
    rate_df['date'] = pd.to_datetime(rate_df['date'])
    
    client = bigquery.Client()
    BQ_Connector.delete_data(client, "rda_analytics_temp", "currency_exchange_rate_THB_USD_temp")
    BQ_Connector.load_data(client, "rda_analytics_temp", "currency_exchange_rate_THB_USD_temp",rate_df)
    BQ_Connector.delete_when_match(client,"rda_analytics", "currency_exchange_rate_THB_USD","rda_analytics_temp", "currency_exchange_rate_THB_USD_temp",
                  "ON (ori.date = temp.date)")
    BQ_Connector.load_data(client, "rda_analytics", "currency_exchange_rate_THB_USD",rate_df)
    
    msg = "💱 <b>Currency Exchange (THB -> USD, IDR):</b> Executed Successfully on 📅 "
    h_function.send_gg_chat_noti(msg)
    return json.dumps({'success': 'Update Currency Exchange Completed'}), 200


""" GOOGLE MEDIA API """

@app.route('/update_google_adsbasicstats', methods=['POST'])
def update_google_adsbasicstats():
    service = h_function.get_service()
    customer_ids = h_function.get_account(service,"Media Account!A1:ZZ",'1S1Ew5r7RL9zvpvZc-Azd8Mc8tkAikitkw2mgAcAb4Ro',"Account ID", "Google")

    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=20)).strftime('%Y-%m-%d')
    query = GG_Connector.get_basicstat_query(start_date,end_date)
    
    # config_dict = GG_Connector.get_config_dict()
    # Initialize the client using the config dictionary
    
    scopes=['https://www.googleapis.com/auth/adwords']
    service_account_info = json.loads(os.environ.get("hmth-bigquery"))
    credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=scopes)

    # Initialize the GoogleAdsClient with the credentials
    client = GoogleAdsClient(credentials=credentials, developer_token=os.environ.get('GG_DEV_TOKEN'))

    all_data = GG_Connector.get_basicstat_data(client,customer_ids,query)
    df = pd.DataFrame(all_data)
    df['segments_date'] = pd.to_datetime(df['segments_date'])
    df['segments_ad_network_type'] = df['segments_ad_network_type'].apply(GG_Connector.get_ad_network_type_description)
    df['segments_slot'] = df['segments_slot'].apply(GG_Connector.get_slot_description)
    value_mapping = GG_Connector.interaction_event_mapping()
    df['metrics_interaction_event_types'] = df['metrics_interaction_event_types'].apply(lambda x: [value_mapping[val] for val in x if val in value_mapping])
    df['campaign_advertising_channel_type'] = df['campaign_advertising_channel_type'].apply(GG_Connector.get_advertising_channel_type)
    
    project_id = 'hmth-448709'
    client = bigquery.Client(project=project_id)   
    BQ_Connector.delete_data(client,"rda_analytics_temp","media_google_AdBasicStats_temp")
    BQ_Connector.load_data(client, "rda_analytics_temp","media_google_AdBasicStats_temp",df)
    condition = "ON (ori.segments_date = temp.segments_date AND ori.customer_id = temp.customer_id AND ori.ad_group_ad_ad_id = temp.ad_group_ad_ad_id) "
    BQ_Connector.delete_when_match(client,"rda_analytics","media_google_AdBasicStats","rda_analytics_temp","media_google_AdBasicStats_temp",condition)
    BQ_Connector.load_data(client,"rda_analytics","media_google_AdBasicStats",df)

    msg = "🌳 Media: <b>Google AdsBasicStats</b> Executed Successfully on 📅 "
    h_function.send_gg_chat_noti(msg)
    return json.dumps({'success': 'Update Google AdsBasicStats Completed'}), 200


@app.route('/update_google_adgroupbasicstats', methods=['POST'])
def update_google_adgroupbasicstats():
    service = h_function.get_service()
    customer_ids = h_function.get_account(service,"Media Account!A1:ZZ",'1S1Ew5r7RL9zvpvZc-Azd8Mc8tkAikitkw2mgAcAb4Ro',"Account ID", "Google")

    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=20)).strftime('%Y-%m-%d')
    query = GG_Connector.get_adgroup_basicstat_query(start_date,end_date)
    
    scopes=['https://www.googleapis.com/auth/adwords']
    service_account_info = json.loads(os.environ.get("hmth-bigquery"))
    credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=scopes)

    # Initialize the GoogleAdsClient with the credentials
    client = GoogleAdsClient(credentials=credentials, developer_token=os.environ.get('GG_DEV_TOKEN'))
    all_data = GG_Connector.get_adgroup_basicstat_data(client,customer_ids,query)
    df = pd.DataFrame(all_data)
    df['segments_date'] = pd.to_datetime(df['segments_date'])
    df['segments_ad_network_type'] = df['segments_ad_network_type'].apply(GG_Connector.get_ad_network_type_description)
    df['segments_slot'] = df['segments_slot'].apply(GG_Connector.get_slot_description)
    value_mapping = GG_Connector.interaction_event_mapping()
    df['metrics_interaction_event_types'] = df['metrics_interaction_event_types'].apply(lambda x: [value_mapping[val] for val in x if val in value_mapping])
    df['campaign_advertising_channel_type'] = df['campaign_advertising_channel_type'].apply(GG_Connector.get_advertising_channel_type)
    
    project_id = 'hmth-448709'
    client = bigquery.Client(project=project_id)   
    BQ_Connector.delete_data(client,"rda_analytics_temp","media_google_AdGroupBasicStats_temp")
    BQ_Connector.load_data(client, "rda_analytics_temp","media_google_AdGroupBasicStats_temp",df)
    condition = "ON (ori.segments_date = temp.segments_date AND ori.customer_id = temp.customer_id AND ori.ad_group_id = temp.ad_group_id) "
    BQ_Connector.delete_when_match(client,"rda_analytics","media_google_AdGroupBasicStats","rda_analytics_temp","media_google_AdGroupBasicStats_temp",condition)
    BQ_Connector.load_data(client,"rda_analytics","media_google_AdGroupBasicStats",df)

    msg = "🌳 Media: <b>Google AdGroupBasicStats</b> Executed Successfully on 📅 "
    h_function.send_gg_chat_noti(msg)
    return json.dumps({'success': 'Update Google AdGroupBasicStats Completed'}), 200


@app.route('/update_google_campaignbasicstats', methods=['POST'])
def update_google_campaignbasicstats():
    service = h_function.get_service()
    customer_ids = h_function.get_account(service,"Media Account!A1:ZZ",'1S1Ew5r7RL9zvpvZc-Azd8Mc8tkAikitkw2mgAcAb4Ro',"Account ID", "Google")

    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=20)).strftime('%Y-%m-%d')
    query = GG_Connector.get_campaign_basicstat_query(start_date,end_date)
    
    scopes=['https://www.googleapis.com/auth/adwords']
    service_account_info = json.loads(os.environ.get("hmth-bigquery"))
    credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=scopes)

    # Initialize the GoogleAdsClient with the credentials
    client = GoogleAdsClient(credentials=credentials, developer_token=os.environ.get('GG_DEV_TOKEN'))
    all_data = GG_Connector.get_campaign_basicstat_data(client,customer_ids,query)
    df = pd.DataFrame(all_data)
    df['segments_date'] = pd.to_datetime(df['segments_date'])
    df['segments_ad_network_type'] = df['segments_ad_network_type'].apply(GG_Connector.get_ad_network_type_description)
    df['segments_slot'] = df['segments_slot'].apply(GG_Connector.get_slot_description)
    value_mapping = GG_Connector.interaction_event_mapping()
    df['metrics_interaction_event_types'] = df['metrics_interaction_event_types'].apply(lambda x: [value_mapping[val] for val in x if val in value_mapping])
    df['campaign_advertising_channel_type'] = df['campaign_advertising_channel_type'].apply(GG_Connector.get_advertising_channel_type)
    
    project_id = 'hmth-448709'
    client = bigquery.Client(project=project_id)   
    BQ_Connector.delete_data(client,"rda_analytics_temp","media_google_CampaignBasicStats_temp")
    BQ_Connector.load_data(client, "rda_analytics_temp","media_google_CampaignBasicStats_temp",df)
    condition = "ON (ori.segments_date = temp.segments_date AND ori.customer_id = temp.customer_id AND ori.campaign_id = temp.campaign_id) "
    BQ_Connector.delete_when_match(client,"rda_analytics","media_google_CampaignBasicStats","rda_analytics_temp","media_google_CampaignBasicStats_temp",condition)
    BQ_Connector.load_data(client,"rda_analytics","media_google_CampaignBasicStats",df)

    msg = "🌳 Media: <b>Google CampaignBasicStats</b> Executed Successfully on 📅 "
    h_function.send_gg_chat_noti(msg)
    return json.dumps({'success': 'Update Google CampaignBasicStats Completed'}), 200


@app.route('/update_google_keywordbasicstats', methods=['POST'])
def update_google_keywordbasicstats():
    service = h_function.get_service()
    customer_ids = h_function.get_account(service,"Media Account!A1:ZZ",'1S1Ew5r7RL9zvpvZc-Azd8Mc8tkAikitkw2mgAcAb4Ro',"Account ID", "Google")

    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=20)).strftime('%Y-%m-%d')
    query = GG_Connector.get_keyword_basicstat_query(start_date,end_date)
    
    scopes=['https://www.googleapis.com/auth/adwords']
    service_account_info = json.loads(os.environ.get("hmth-bigquery"))
    credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=scopes)

    # Initialize the GoogleAdsClient with the credentials
    client = GoogleAdsClient(credentials=credentials, developer_token=os.environ.get('GG_DEV_TOKEN'))
    all_data = GG_Connector.get_keyword_basicstat_data(client,customer_ids,query)
    df = pd.DataFrame(all_data)
    if len(df) > 0:
        df['segments_date'] = pd.to_datetime(df['segments_date'])
        df['segments_ad_network_type'] = df['segments_ad_network_type'].apply(GG_Connector.get_ad_network_type_description)
        df['ad_group_criterion_keyword_match_type'] = df['ad_group_criterion_keyword_match_type'].apply(GG_Connector.get_keyword_match_type)
        value_mapping = GG_Connector.interaction_event_mapping()
        df['metrics_interaction_event_types'] = df['metrics_interaction_event_types'].apply(lambda x: [value_mapping[val] for val in x if val in value_mapping])
        df['campaign_advertising_channel_type'] = df['campaign_advertising_channel_type'].apply(GG_Connector.get_advertising_channel_type)
        
        project_id = 'hmth-448709'
        client = bigquery.Client(project=project_id)   
        BQ_Connector.delete_data(client,"rda_analytics_temp","media_google_KeywordBasicStats_temp")
        BQ_Connector.load_data(client, "rda_analytics_temp","media_google_KeywordBasicStats_temp",df)
        condition = "ON (ori.segments_date = temp.segments_date AND ori.customer_id = temp.customer_id AND ori.ad_group_criterion_criterion_id = temp.ad_group_criterion_criterion_id AND ori.campaign_id = temp.campaign_id) "
        BQ_Connector.delete_when_match(client,"rda_analytics","media_google_KeywordBasicStats","rda_analytics_temp","media_google_KeywordBasicStats_temp",condition)
        BQ_Connector.load_data(client,"rda_analytics","media_google_KeywordBasicStats",df)

        msg = "🌳 Media: <b>Google KeywordBasicStats</b> Executed Successfully on 📅 "
        h_function.send_gg_chat_noti(msg)
    return json.dumps({'success': 'Update Google KeywordBasicStats Completed'}), 200

@app.route('/update_google_videobasicstats', methods=['POST'])
def update_google_videobasicstats():
    service = h_function.get_service()
    customer_ids = h_function.get_account(service,"Media Account!A1:ZZ",'1S1Ew5r7RL9zvpvZc-Azd8Mc8tkAikitkw2mgAcAb4Ro',"Account ID", "Google")

    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=20)).strftime('%Y-%m-%d')
    query = GG_Connector.get_video_basicstat_query(start_date,end_date)
    
    scopes=['https://www.googleapis.com/auth/adwords']
    service_account_info = json.loads(os.environ.get("hmth-bigquery"))
    credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=scopes)

    # Initialize the GoogleAdsClient with the credentials
    client = GoogleAdsClient(credentials=credentials, developer_token=os.environ.get('GG_DEV_TOKEN'))
    all_data = GG_Connector.get_video_basicstat_data(client,customer_ids,query)
    df = pd.DataFrame(all_data)
    if len(df) > 0:
        df['segments_date'] = pd.to_datetime(df['segments_date'])
        df['segments_ad_network_type'] = df['segments_ad_network_type'].apply(GG_Connector.get_ad_network_type_description)
        df['ad_group_ad_status'] = df['ad_group_ad_status'].apply(GG_Connector.get_ad_group_ad_status)
        df['campaign_advertising_channel_type'] = df['campaign_advertising_channel_type'].apply(GG_Connector.get_advertising_channel_type)
        
        project_id = 'hmth-448709'
        client = bigquery.Client(project=project_id)   
        BQ_Connector.delete_data(client,"rda_analytics_temp","media_google_VideoBasicStats_temp")
        BQ_Connector.load_data(client, "rda_analytics_temp","media_google_VideoBasicStats_temp",df)
        condition = "ON (ori.segments_date = temp.segments_date AND ori.customer_id = temp.customer_id AND ori.ad_group_ad_ad_id = temp.ad_group_ad_ad_id AND ori.video_id = temp.video_id) "
        BQ_Connector.delete_when_match(client,"rda_analytics","media_google_VideoBasicStats","rda_analytics_temp","media_google_VideoBasicStats_temp",condition)
        BQ_Connector.load_data(client,"rda_analytics","media_google_VideoBasicStats",df)

        msg = "🌳 Media: <b>Google VideoBasicStats</b> Executed Successfully on 📅 "
        h_function.send_gg_chat_noti(msg)
    return json.dumps({'success': 'Update Google VideoBasicStats Completed'}), 200


@app.route('/update_google_videoconversionstats', methods=['POST'])
def update_google_videoconversionstats():
    service = h_function.get_service()
    customer_ids = h_function.get_account(service,"Media Account!A1:ZZ",'1S1Ew5r7RL9zvpvZc-Azd8Mc8tkAikitkw2mgAcAb4Ro',"Account ID", "Google")

    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=20)).strftime('%Y-%m-%d')
    query = GG_Connector.get_video_conversion_stat_query(start_date,end_date)
    
    scopes=['https://www.googleapis.com/auth/adwords']
    service_account_info = json.loads(os.environ.get("hmth-bigquery"))
    credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=scopes)

    # Initialize the GoogleAdsClient with the credentials
    client = GoogleAdsClient(credentials=credentials, developer_token=os.environ.get('GG_DEV_TOKEN'))
    all_data = GG_Connector.get_video_conversionstat_data(client,customer_ids,query)
    
    if len(all_data) > 0:
        df = pd.DataFrame(all_data)
        df['segments_date'] = pd.to_datetime(df['segments_date'])
        df['segments_month'] = pd.to_datetime(df['segments_month'])
        df['segments_quarter'] = pd.to_datetime(df['segments_quarter'])
        df['segments_week'] = pd.to_datetime(df['segments_week'])
        
        df['segments_ad_network_type'] = df['segments_ad_network_type'].apply(GG_Connector.get_ad_network_type_description)
        df['ad_group_ad_status'] = df['ad_group_ad_status'].apply(GG_Connector.get_ad_group_ad_status)
        df['campaign_advertising_channel_type'] = df['campaign_advertising_channel_type'].apply(GG_Connector.get_advertising_channel_type)
        df['segments_day_of_week'] = df['segments_day_of_week'].apply(GG_Connector.get_day_of_week)
        df['segments_conversion_action_category'] = df['segments_conversion_action_category'].apply(GG_Connector.get_conversion_action_category)
        
        project_id = 'hmth-448709'
        client = bigquery.Client(project=project_id)   
        BQ_Connector.delete_data(client,"rda_analytics_temp","media_google_VideoConversionStats_temp")
        BQ_Connector.load_data(client, "rda_analytics_temp","media_google_VideoConversionStats_temp",df)
        condition = "ON (ori.segments_date = temp.segments_date AND ori.customer_id = temp.customer_id AND ori.ad_group_ad_ad_id = temp.ad_group_ad_ad_id AND ori.video_id = temp.video_id) "
        BQ_Connector.delete_when_match(client,"rda_analytics","media_google_VideoConversionStats","rda_analytics_temp","media_google_VideoConversionStats_temp",condition)
        BQ_Connector.load_data(client,"rda_analytics","media_google_VideoConversionStats",df)

        msg = "🌳 Media: <b>Google VideoConversionStats</b> Executed Successfully on 📅 "
    else:
        msg = "🌳 Media: <b>Google VideoConversionStats</b> No new records for 📅 "
    
    h_function.send_gg_chat_noti(msg)
    return json.dumps({'success': 'Update Google VideoConversionStats Completed'}), 200


@app.route('/update_google_videononclickstats', methods=['POST'])
def update_google_videononclickstats():
    service = h_function.get_service()
    customer_ids = h_function.get_account(service,"Media Account!A1:ZZ",'1S1Ew5r7RL9zvpvZc-Azd8Mc8tkAikitkw2mgAcAb4Ro',"Account ID", "Google")

    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=20)).strftime('%Y-%m-%d')
    query = GG_Connector.get_video_nonclickstat_query(start_date,end_date)
    
    scopes=['https://www.googleapis.com/auth/adwords']
    service_account_info = json.loads(os.environ.get("hmth-bigquery"))
    credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=scopes)

    # Initialize the GoogleAdsClient with the credentials
    client = GoogleAdsClient(credentials=credentials, developer_token=os.environ.get('GG_DEV_TOKEN'))
    all_data = GG_Connector.get_video_nonclickstat_data(client,customer_ids,query)
    df = pd.DataFrame(all_data)
    
    if len(df) > 0:
        df['segments_date'] = pd.to_datetime(df['segments_date'])
        df['segments_month'] = pd.to_datetime(df['segments_month'])
        df['segments_quarter'] = pd.to_datetime(df['segments_quarter'])
        df['segments_week'] = pd.to_datetime(df['segments_week'])
        
        df['segments_ad_network_type'] = df['segments_ad_network_type'].apply(GG_Connector.get_ad_network_type_description)
        df['ad_group_ad_status'] = df['ad_group_ad_status'].apply(GG_Connector.get_ad_group_ad_status)
        df['segments_day_of_week'] = df['segments_day_of_week'].apply(GG_Connector.get_day_of_week)
        df['campaign_advertising_channel_type'] = df['campaign_advertising_channel_type'].apply(GG_Connector.get_advertising_channel_type)
        
        project_id = 'hmth-448709'
        client = bigquery.Client(project=project_id)   
        BQ_Connector.delete_data(client,"rda_analytics_temp","media_google_VideoNonClickStats_temp")
        BQ_Connector.load_data(client, "rda_analytics_temp","media_google_VideoNonClickStats_temp",df)
        condition = "ON (ori.segments_date = temp.segments_date AND ori.customer_id = temp.customer_id AND ori.ad_group_ad_ad_id = temp.ad_group_ad_ad_id AND ori.video_id = temp.video_id) "
        BQ_Connector.delete_when_match(client,"rda_analytics","media_google_VideoNonClickStats","rda_analytics_temp","media_google_VideoNonClickStats_temp",condition)
        BQ_Connector.load_data(client,"rda_analytics","media_google_VideoNonClickStats",df)

        msg = "🌳 Media: <b>Google VideoNonClickStats</b> Executed Successfully on 📅 "
        h_function.send_gg_chat_noti(msg)
    return json.dumps({'success': 'Update Google VideoNonClickStats Completed'}), 200


@app.route('/update_google_adcrossconversionstats', methods=['POST'])
def update_google_adcrossconversionstats():
    service = h_function.get_service()
    customer_ids = h_function.get_account(service,"Media Account!A1:ZZ",'1S1Ew5r7RL9zvpvZc-Azd8Mc8tkAikitkw2mgAcAb4Ro',"Account ID", "Google")

    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=20)).strftime('%Y-%m-%d')
    query = GG_Connector.get_ad_cross_device_conversion_stat_query(start_date,end_date)
    
    scopes=['https://www.googleapis.com/auth/adwords']
    service_account_info = json.loads(os.environ.get("hmth-bigquery"))
    credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=scopes)

    # Initialize the GoogleAdsClient with the credentials
    client = GoogleAdsClient(credentials=credentials, developer_token=os.environ.get('GG_DEV_TOKEN'))
    all_data = GG_Connector.get_ad_cross_device_conversion_stat_data(client,customer_ids,query)
    df = pd.DataFrame(all_data)
    df['segments_date'] = pd.to_datetime(df['segments_date'])
    df['segments_month'] = pd.to_datetime(df['segments_month'])
    df['segments_quarter'] = pd.to_datetime(df['segments_quarter'])
    df['segments_week'] = pd.to_datetime(df['segments_week'])
    
    df['segments_ad_network_type'] = df['segments_ad_network_type'].apply(GG_Connector.get_ad_network_type_description)
    df['segments_day_of_week'] = df['segments_day_of_week'].apply(GG_Connector.get_day_of_week)
    df['campaign_advertising_channel_type'] = df['campaign_advertising_channel_type'].apply(GG_Connector.get_advertising_channel_type)
    df['segments_conversion_action_category']= df['segments_conversion_action_category'].apply(GG_Connector.get_conversion_action_category)
    df['segments_click_type'] = df['segments_click_type'].apply(GG_Connector.get_click_type)
    
    project_id = 'hmth-448709'
    client = bigquery.Client(project=project_id)   
    BQ_Connector.delete_data(client,"rda_analytics_temp","media_google_AdCrossDeviceConversionStats_temp")
    BQ_Connector.load_data(client, "rda_analytics_temp","media_google_AdCrossDeviceConversionStats_temp",df)
    condition = "ON (ori.segments_date = temp.segments_date AND ori.customer_id = temp.customer_id AND ori.ad_group_ad_ad_id = temp.ad_group_ad_ad_id AND ori.segments_click_type = temp.segments_click_type) "
    BQ_Connector.delete_when_match(client,"rda_analytics","media_google_AdCrossDeviceConversionStats","rda_analytics_temp","media_google_AdCrossDeviceConversionStats_temp",condition)
    BQ_Connector.load_data(client,"rda_analytics","media_google_AdCrossDeviceConversionStats",df)

    msg = "🌳 Media: <b>Google AdCrossDeviceConversionStats</b> Executed Successfully on 📅 "
    h_function.send_gg_chat_noti(msg)
    return json.dumps({'success': 'Update Google AdCrossDeviceConversionStats Completed'}), 200

@app.route('/update_google_adgroupcrossconversionstats', methods=['POST'])
def update_google_adgroupcrossconversionstats():
    service = h_function.get_service()
    customer_ids = h_function.get_account(service,"Media Account!A1:ZZ",'1S1Ew5r7RL9zvpvZc-Azd8Mc8tkAikitkw2mgAcAb4Ro',"Account ID", "Google")

    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=20)).strftime('%Y-%m-%d')
    query = GG_Connector.get_ad_group_cross_device_conversion_stat_query(start_date,end_date)
    
    scopes=['https://www.googleapis.com/auth/adwords']
    service_account_info = json.loads(os.environ.get("hmth-bigquery"))
    credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=scopes)

    # Initialize the GoogleAdsClient with the credentials
    client = GoogleAdsClient(credentials=credentials, developer_token=os.environ.get('GG_DEV_TOKEN'))
    all_data = GG_Connector.get_ad_group_cross_device_conversion_stat_data(client,customer_ids,query)
    df = pd.DataFrame(all_data)
    df['segments_date'] = pd.to_datetime(df['segments_date'])
    df['segments_month'] = pd.to_datetime(df['segments_month'])
    df['segments_quarter'] = pd.to_datetime(df['segments_quarter'])
    df['segments_week'] = pd.to_datetime(df['segments_week'])
    
    df['segments_ad_network_type'] = df['segments_ad_network_type'].apply(GG_Connector.get_ad_network_type_description)
    df['segments_day_of_week'] = df['segments_day_of_week'].apply(GG_Connector.get_day_of_week)
    df['campaign_advertising_channel_type'] = df['campaign_advertising_channel_type'].apply(GG_Connector.get_advertising_channel_type)
    df['segments_conversion_action_category']= df['segments_conversion_action_category'].apply(GG_Connector.get_conversion_action_category)
    df['segments_click_type'] = df['segments_click_type'].apply(GG_Connector.get_click_type)
    
    project_id = 'hmth-448709'
    client = bigquery.Client(project=project_id)   
    BQ_Connector.delete_data(client,"rda_analytics_temp","media_google_AdGroupCrossDeviceConversionStats_temp")
    BQ_Connector.load_data(client, "rda_analytics_temp","media_google_AdGroupCrossDeviceConversionStats_temp",df)
    condition = "ON (ori.segments_date = temp.segments_date AND ori.customer_id = temp.customer_id AND ori.ad_group_id = temp.ad_group_id AND ori.segments_click_type = temp.segments_click_type) "
    BQ_Connector.delete_when_match(client,"rda_analytics","media_google_AdGroupCrossDeviceConversionStats","rda_analytics_temp","media_google_AdGroupCrossDeviceConversionStats_temp",condition)
    BQ_Connector.load_data(client,"rda_analytics","media_google_AdGroupCrossDeviceConversionStats",df)

    msg = "🌳 Media: <b>Google AdGroupCrossDeviceConversionStats</b> Executed Successfully on 📅 "
    h_function.send_gg_chat_noti(msg)
    return json.dumps({'success': 'Update Google AdGroupCrossDeviceConversionStats Completed'}), 200


@app.route('/update_google_campaigncrossconversionstats', methods=['POST'])
def update_google_campaigncrossconversionstats():
    service = h_function.get_service()
    customer_ids = h_function.get_account(service,"Media Account!A1:ZZ",'1S1Ew5r7RL9zvpvZc-Azd8Mc8tkAikitkw2mgAcAb4Ro',"Account ID", "Google")

    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=20)).strftime('%Y-%m-%d')
    query = GG_Connector.get_campaign_cross_device_conversion_stat_query(start_date,end_date)
    
    scopes=['https://www.googleapis.com/auth/adwords']
    service_account_info = json.loads(os.environ.get("hmth-bigquery"))
    credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=scopes)

    # Initialize the GoogleAdsClient with the credentials
    client = GoogleAdsClient(credentials=credentials, developer_token=os.environ.get('GG_DEV_TOKEN'))
    all_data = GG_Connector.get_campaign_cross_device_conversion_stat_data(client,customer_ids,query)
    df = pd.DataFrame(all_data)
    df['segments_date'] = pd.to_datetime(df['segments_date'])
    df['segments_month'] = pd.to_datetime(df['segments_month'])
    df['segments_quarter'] = pd.to_datetime(df['segments_quarter'])
    df['segments_week'] = pd.to_datetime(df['segments_week'])
    
    df['segments_ad_network_type'] = df['segments_ad_network_type'].apply(GG_Connector.get_ad_network_type_description)
    df['segments_day_of_week'] = df['segments_day_of_week'].apply(GG_Connector.get_day_of_week)
    df['campaign_advertising_channel_type'] = df['campaign_advertising_channel_type'].apply(GG_Connector.get_advertising_channel_type)
    df['segments_conversion_action_category']= df['segments_conversion_action_category'].apply(GG_Connector.get_conversion_action_category)
    df['segments_conversion_attribution_event_type']= df['segments_conversion_attribution_event_type'].apply(GG_Connector.get_conversion_attribution_event_type)
    
    project_id = 'hmth-448709'
    client = bigquery.Client(project=project_id)   
    BQ_Connector.delete_data(client,"rda_analytics_temp","media_google_CampaignCrossDeviceConversionStats_temp")
    BQ_Connector.load_data(client, "rda_analytics_temp","media_google_CampaignCrossDeviceConversionStats_temp",df)
    condition = "ON (ori.segments_date = temp.segments_date AND ori.customer_id = temp.customer_id AND ori.campaign_id = temp.campaign_id) "
    BQ_Connector.delete_when_match(client,"rda_analytics","media_google_CampaignCrossDeviceConversionStats","rda_analytics_temp","media_google_CampaignCrossDeviceConversionStats_temp",condition)
    BQ_Connector.load_data(client,"rda_analytics","media_google_CampaignCrossDeviceConversionStats",df)

    msg = "🌳 Media: <b>Google CampaignCrossDeviceConversionStats</b> Executed Successfully on 📅 "
    h_function.send_gg_chat_noti(msg)
    return json.dumps({'success': 'Update Google CampaignCrossDeviceConversionStats Completed'}), 200

@app.route('/update_google_keywordcrossconversionstats', methods=['POST'])
def update_google_keywordcrossconversionstats():
    service = h_function.get_service()
    customer_ids = h_function.get_account(service,"Media Account!A1:ZZ",'1S1Ew5r7RL9zvpvZc-Azd8Mc8tkAikitkw2mgAcAb4Ro',"Account ID", "Google")

    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=20)).strftime('%Y-%m-%d')
    query = GG_Connector.get_keyword_cross_device_conversion_stat_query(start_date,end_date)
    
    scopes=['https://www.googleapis.com/auth/adwords']
    service_account_info = json.loads(os.environ.get("hmth-bigquery"))
    credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=scopes)

    # Initialize the GoogleAdsClient with the credentials
    client = GoogleAdsClient(credentials=credentials, developer_token=os.environ.get('GG_DEV_TOKEN'))
    all_data = GG_Connector.get_keyword_cross_device_conversion_stat_data(client,customer_ids,query)
    df = pd.DataFrame(all_data)
    if len(df) > 0:
        df['segments_date'] = pd.to_datetime(df['segments_date'])
        df['segments_month'] = pd.to_datetime(df['segments_month'])
        df['segments_quarter'] = pd.to_datetime(df['segments_quarter'])
        df['segments_week'] = pd.to_datetime(df['segments_week'])
        
        df['segments_ad_network_type'] = df['segments_ad_network_type'].apply(GG_Connector.get_ad_network_type_description)
        df['segments_day_of_week'] = df['segments_day_of_week'].apply(GG_Connector.get_day_of_week)
        df['campaign_advertising_channel_type'] = df['campaign_advertising_channel_type'].apply(GG_Connector.get_advertising_channel_type)
        df['segments_conversion_action_category']= df['segments_conversion_action_category'].apply(GG_Connector.get_conversion_action_category)
        df['ad_group_criterion_keyword_match_type']= df['ad_group_criterion_keyword_match_type'].apply(GG_Connector.get_keyword_match_type)
        
        project_id = 'hmth-448709'
        client = bigquery.Client(project=project_id)
        BQ_Connector.delete_data(client,"rda_analytics_temp","media_google_KeywordCrossDeviceConversionStats_temp")
        BQ_Connector.load_data(client, "rda_analytics_temp","media_google_KeywordCrossDeviceConversionStats_temp",df)
        condition = "ON (ori.segments_date = temp.segments_date AND ori.customer_id = temp.customer_id AND ori.ad_group_criterion_criterion_id = temp.ad_group_criterion_criterion_id) "
        BQ_Connector.delete_when_match(client,"rda_analytics","media_google_KeywordCrossDeviceConversionStats","rda_analytics_temp","media_google_KeywordCrossDeviceConversionStats_temp",condition)
        BQ_Connector.load_data(client,"rda_analytics","media_google_KeywordCrossDeviceConversionStats",df)

        msg = "🌳 Media: <b>Google KeywordCrossDeviceConversionStats</b> Executed Successfully on 📅 "
        h_function.send_gg_chat_noti(msg)
    return json.dumps({'success': 'Update Google KeywordCrossDeviceConversionStats Completed'}), 200


@app.route('/update_google_adgrouplabel', methods=['POST'])
def update_google_adgrouplabel():
    service = h_function.get_service()
    customer_ids = h_function.get_account(service,"Media Account!A1:ZZ",'1S1Ew5r7RL9zvpvZc-Azd8Mc8tkAikitkw2mgAcAb4Ro',"Account ID", "Google")

    query = GG_Connector.get_adgroup_label_query()
    scopes=['https://www.googleapis.com/auth/adwords']
    service_account_info = json.loads(os.environ.get("hmth-bigquery"))
    credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=scopes)

    # Initialize the GoogleAdsClient with the credentials
    client = GoogleAdsClient(credentials=credentials, developer_token=os.environ.get('GG_DEV_TOKEN'))
    all_data = GG_Connector.get_adgroup_label_data(client,customer_ids,query)
    df = pd.DataFrame(all_data)
    
    if len(df) > 0:
        project_id = 'hmth-448709'
        client = bigquery.Client(project=project_id)
        BQ_Connector.delete_data(client,"rda_analytics","media_google_AdGroupLabel")
        BQ_Connector.load_data(client, "rda_analytics","media_google_AdGroupLabel",df)

        msg = "🌳 Media: <b>Google AdGroupLabel</b> Executed Successfully on 📅 "
        h_function.send_gg_chat_noti(msg)
    return json.dumps({'success': 'Update Google AdGroupLabel Completed'}), 200

"""" FB """
@app.route('/update_fb_daily', methods=['POST'])
def update_fb_daily():
    access_token = os.environ['FBTOKEN']
    my_accounts = FB_Connector.get_myaccount(access_token)
    service = h_function.get_service()
    target_account = h_function.get_account(service,"Media Account!A1:ZZ",'1S1Ew5r7RL9zvpvZc-Azd8Mc8tkAikitkw2mgAcAb4Ro',"Account ID", "Facebook")
    target_account = ['act_' + item for item in target_account]
    fields = FB_Connector.get_fb_main_fields()
    asyn_job_list = FB_Connector.get_asynjob(my_accounts, target_account, fields, 15, "ad")    
    ads_data = FB_Connector.clean_ads_data(asyn_job_list)
    ads_data = FB_Connector.transform_fb_main(ads_data)
    
    project_id = 'hmth-448709'
    client = bigquery.Client(project=project_id)
    
    BQ_Connector.delete_data(client,'rda_analytics_temp', 'media_facebook_main_temp')
    BQ_Connector.load_data(client, 'rda_analytics_temp', 'media_facebook_main_temp', ads_data)
    condition = "ON (ori.date_start = temp.date_start AND ori.account_id = temp.account_id AND ori.ad_id = temp.ad_id) "
    BQ_Connector.delete_when_match(client,"rda_analytics","media_facebook_main","rda_analytics_temp","media_facebook_main_temp",condition)
    BQ_Connector.load_data(client,"rda_analytics","media_facebook_main",ads_data)

    msg = f"🔷 Media: <b>Facebook Main</b> Executed Successfully on 📅 "
    h_function.send_gg_chat_noti(msg)
    return json.dumps({'success': 'Update Facebook Main Completed'}), 200


@app.route('/update_fb_daily_action', methods=['POST'])
def update_fb_daily_action():
    access_token = os.environ['FBTOKEN']
    my_accounts = FB_Connector.get_myaccount(access_token)
    service = h_function.get_service()
    target_account = h_function.get_account(service,"Media Account!A1:ZZ",'1S1Ew5r7RL9zvpvZc-Azd8Mc8tkAikitkw2mgAcAb4Ro',"Account ID", "Facebook")
    target_account = ['act_' + item for item in target_account]
    
    fields = FB_Connector.get_fb_action_fields()
    asyn_job_list = FB_Connector.get_asynjob(my_accounts, target_account, fields, 15, "ad")    
    ads_data = FB_Connector.clean_ads_action_data(asyn_job_list)
    ads_data = ads_data[~((ads_data['actions'].apply(lambda x: x == [])) & (ads_data['action_values'].apply(lambda x: x == [])))]
    
    custom_conversion_list = FB_Connector.get_conversion_list(ads_data)
    custom_conversion_df = pd.DataFrame(custom_conversion_list)
    custom_conversion_df = custom_conversion_df.replace(" ", "", regex=True)
    
    ads_data = FB_Connector.clean_action_data(custom_conversion_df,ads_data)
    ads_data = FB_Connector.clean_nested_action_data(ads_data)
    
    ads_data['date_start'] = pd.to_datetime(ads_data['date_start'])
    ads_data['date_stop'] = pd.to_datetime(ads_data['date_stop'])
    ads_data['created_time'] = pd.to_datetime(ads_data['created_time'])
    
    project_id = 'hmth-448709'
    client = bigquery.Client(project=project_id)
    BQ_Connector.delete_data(client,'rda_analytics_temp', 'media_facebook_action_temp')
    BQ_Connector.load_data(client, 'rda_analytics_temp', 'media_facebook_action_temp', ads_data)
    condition = "ON (ori.date_start = temp.date_start AND ori.account_id = temp.account_id AND ori.ad_id = temp.ad_id) "
    BQ_Connector.delete_when_match(client,"rda_analytics","media_facebook_action","rda_analytics_temp","media_facebook_action_temp",condition)
    BQ_Connector.load_data(client,"rda_analytics","media_facebook_action",ads_data)

    msg = f"🔷 Media: <b>Facebook Actions</b> Executed Successfully on 📅 "
    h_function.send_gg_chat_noti(msg)
    return json.dumps({'success': 'Update Facebook Actions Completed'}), 200

@app.route('/update_fb_adcreative', methods=['POST'])
def update_fb_adcreative():
    access_token = os.environ['FBTOKEN']
    service = h_function.get_service()
    target_accounts = h_function.get_account(service,"Media Account!A1:ZZ",'1S1Ew5r7RL9zvpvZc-Azd8Mc8tkAikitkw2mgAcAb4Ro',"Account ID", "Facebook")
    target_accounts_str = ", ".join(f"'{target_account}'" for target_account in target_accounts)
    
    client = bigquery.Client()
    df = FB_Connector.get_all_ad_data(client,target_accounts_str)
    adcreative_df = FB_Connector.get_all_adcreative_data(client,target_accounts_str)
    target_row = df.merge(adcreative_df, on=df.columns.tolist(), how='left', indicator=True)
    target_df = target_row[target_row['_merge']=="left_only"]
    
    AdCreative_list = FB_Connector.get_adcreative_from_ad_id(access_token,target_df)
    adcreative_df = pd.DataFrame(AdCreative_list)
    
    if len(adcreative_df) > 0:
        merged_df = pd.merge(df,adcreative_df,how="right",on=['account_id','ad_id'])
        
        project_id = 'hmth-448709'
        client = bigquery.Client(project=project_id)
        BQ_Connector.delete_data(client,'rda_analytics_temp', 'media_facebook_adcreative_temp')
        BQ_Connector.load_data(client, 'rda_analytics_temp', 'media_facebook_adcreative_temp', merged_df)
        condition = "ON ori.id = temp.id AND ori.ad_id = temp.ad_id AND ori.account_id = temp.account_id "
        BQ_Connector.delete_when_match(client,"rda_analytics","media_facebook_adcreative","rda_analytics_temp","media_facebook_adcreative_temp",condition)
        BQ_Connector.load_data(client,"rda_analytics","media_facebook_adcreative",merged_df)

        msg = f"🔷 Media: <b>Facebook AdCreatives</b> Executed Successfully on 📅 "
        h_function.send_gg_chat_noti(msg)
    return json.dumps({'success': 'Update Facebook AdCreatives Completed'}), 200
    
    
@app.route('/update_fb_adimage', methods=['POST'])
def update_fb_adimage():
    access_token = os.environ['FBTOKEN']
    service = h_function.get_service()
    target_accounts = h_function.get_account(service,"Media Account!A1:ZZ",'1S1Ew5r7RL9zvpvZc-Azd8Mc8tkAikitkw2mgAcAb4Ro',"Account ID", "Facebook")
    account_list = ['act_' + item for item in target_accounts]
    
    adimage_list = FB_Connector.get_adimage(account_list,access_token)
    image_df = pd.DataFrame(adimage_list)
    
    if len(image_df) > 0:
        project_id = 'hmth-448709'
        client = bigquery.Client(project=project_id)
        BQ_Connector.delete_data(client,'rda_analytics_temp', 'media_facebook_adimage_temp')
        BQ_Connector.load_data(client, 'rda_analytics_temp', 'media_facebook_adimage_temp', image_df)
        condition = "ON ori.id = temp.id AND ori.ad_id = temp.ad_id AND ori.account_id = temp.account_id "
        BQ_Connector.delete_when_match(client,"rda_analytics","media_facebook_adimage","rda_analytics_temp","media_facebook_adimage_temp",condition)
        BQ_Connector.load_data(client,"rda_analytics","media_facebook_adimage",image_df)

        msg = f"🔷 Media: <b>Facebook AdImage</b> Executed Successfully on 📅 "
        h_function.send_gg_chat_noti(msg)
    return json.dumps({'success': 'Update Facebook AdImage Completed'}), 200
    
    
@app.route('/update_fb_daily_catalog_segments', methods=['POST'])
def update_fb_daily_catalog_segments():
    access_token = os.environ['FBTOKEN']
    my_accounts = FB_Connector.get_myaccount(access_token)
    service = h_function.get_service()
    target_account = h_function.get_account(service,"Media Account!A1:ZZ",'1S1Ew5r7RL9zvpvZc-Azd8Mc8tkAikitkw2mgAcAb4Ro',"Account ID", "Facebook")
    target_account = ['act_' + item for item in target_account]
    
    fields = FB_Connector.get_fb_catalog_fields()
    asyn_job_list = FB_Connector.get_asynjob(my_accounts, target_account, fields, 15, "ad")    
    if len(asyn_job_list) > 0:
        ads_data = FB_Connector.clean_ads_catalog_segment_data(asyn_job_list)
        
        ads_data = ads_data[~((ads_data['catalog_segment_actions'].apply(lambda x: x == [])) & (ads_data['catalog_segment_values'].apply(lambda x: x == [])))]
        ads_data = FB_Connector.clean_nested_catalog_segment_data(ads_data)
        
        ads_data['date_start'] = pd.to_datetime(ads_data['date_start'])
        ads_data['date_stop'] = pd.to_datetime(ads_data['date_stop'])
        ads_data['created_time'] = pd.to_datetime(ads_data['created_time'])
        
        project_id = 'hmth-448709'
        client = bigquery.Client(project=project_id)
        BQ_Connector.delete_data(client,'rda_analytics_temp', 'media_facebook_catalog_segment_temp')
        BQ_Connector.load_data(client, 'rda_analytics_temp', 'media_facebook_catalog_segment_temp', ads_data)
        condition = "ON (ori.date_start = temp.date_start AND ori.account_id = temp.account_id AND ori.ad_id = temp.ad_id) "
        BQ_Connector.delete_when_match(client,"rda_analytics","media_facebook_catalog_segment","rda_analytics_temp","media_facebook_catalog_segment_temp",condition)
        BQ_Connector.load_data(client,"rda_analytics","media_facebook_catalog_segment",ads_data)

        msg = f"🔷 Media: <b>Facebook Catalog Segment</b> Executed Successfully on 📅 "
        h_function.send_gg_chat_noti(msg)
    return json.dumps({'success': 'Update Facebook Catalog Segment Completed'}), 200


@app.route('/update_fb_page_insight', methods=['POST'])
def update_fb_page_insight():
    
    pages = FB_Connector.get_all_page()
    metric = FB_Connector.get_page_insight_metric()
    rows = FB_Connector.get_page_insight(pages,metric)
    
    page_insight_metric = pd.DataFrame(rows)
    pivoted_df = FB_Connector.clean_page_insight(page_insight_metric)
    pivoted_df = FB_Connector.page_insight_nested(pivoted_df)

    client = bigquery.Client()
    BQ_Connector.delete_data(client,"rda_analytics_temp","media_facebook_page_insight_temp")
    BQ_Connector.load_data(client, "rda_analytics_temp", "media_facebook_page_insight_temp",pivoted_df)
    BQ_Connector.delete_when_match(client,"rda_analytics","media_facebook_page_insight","rda_analytics_temp","media_facebook_page_insight_temp","ON ori.page_id = temp.page_id AND ori.date = temp.date ")
    BQ_Connector.load_data(client,"rda_analytics","media_facebook_page_insight",pivoted_df)
        
    msg = f"🔷🔖 Content: <b>Facebook Page Insight</b> Executed Successfully on 📅 "
    h_function.send_gg_chat_noti(msg)
    return json.dumps({'success': msg}), 200


@app.route('/update_content_fb_page_videoview', methods=['POST'])
def update_content_fb_page_videoview():
    pages = FB_Connector.get_all_page()
    metric = [
            'page_video_views','page_video_views_by_uploaded_hosted','page_video_views_paid','page_video_views_organic',
            'page_video_views_by_paid_non_paid','page_video_views_autoplayed','page_video_views_click_to_play','page_video_views_unique',
            'page_video_repeat_views','page_video_complete_views_30s','page_video_complete_views_30s_paid','page_video_complete_views_30s_organic',
            'page_video_complete_views_30s_autoplayed','page_video_complete_views_30s_click_to_play','page_video_complete_views_30s_unique',
            'page_video_complete_views_30s_repeat_views','page_video_view_time'
        ]
    rows = FB_Connector.get_page_insight(pages,metric)
    page_insight_metric = pd.DataFrame(rows)
    pivoted_df = FB_Connector.clean_page_insight_pivot_needed(page_insight_metric)
    client = bigquery.Client()
    BQ_Connector.delete_data(client,"rda_analytics_temp","media_facebook_page_videoview_temp")
    BQ_Connector.load_data(client,"rda_analytics_temp","media_facebook_page_videoview_temp",pivoted_df)
    BQ_Connector.delete_when_match(client,'rda_analytics','media_facebook_page_videoview','rda_analytics_temp','media_facebook_page_videoview_temp',"ON (ori.date = temp.date AND ori.page_id = temp.page_id) ")
    BQ_Connector.load_data(client, "rda_analytics","media_facebook_page_videoview",pivoted_df)
    msg = f"🔷📽️ Content: <b>Facebook Page Video View</b> Executed Successfully on 📅 "
    h_function.send_gg_chat_noti(msg)
    return json.dumps({'success': msg}), 200


@app.route('/update_content_fb_pagepost', methods=['POST'])
def update_content_fb_pagepost():
    pages = FB_Connector.get_all_page()
    rows = FB_Connector.get_fb_pagepost(pages)
    df = pd.DataFrame(rows)
    if len(df) > 0:
        df['created_time'] = pd.to_datetime(df['created_time'])
        df['updated_time'] = pd.to_datetime(df['updated_time'])
        client = bigquery.Client()
        BQ_Connector.delete_data(client,"rda_analytics_temp","media_facebook_page_feed_temp")
        BQ_Connector.load_data(client,"rda_analytics_temp","media_facebook_page_feed_temp",df)
        BQ_Connector.delete_when_match(client,"rda_analytics","media_facebook_page_feed","rda_analytics_temp","media_facebook_page_feed_temp",
                                    "ON ori.page_id = temp.page_id AND ori.post_id = temp.post_id ")
        BQ_Connector.load_data(client, "rda_analytics","media_facebook_page_feed",df)
        msg = f"🔷🔖 Content: <b>Facebook Page Post</b> Executed Successfully on 📅 "
        h_function.send_gg_chat_noti(msg)
    return json.dumps({'success': "Update FB Page Post Succesfully"}), 200

@app.route('/update_facebook_ad_preview', methods=['POST'])
def update_content_fb_pagepost():
    service = h_function.get_service()
    target_account = h_function.get_account(service,"Media Account!A1:ZZ",'1S1Ew5r7RL9zvpvZc-Azd8Mc8tkAikitkw2mgAcAb4Ro',"Account ID", "Facebook")
    for account_id in target_account:
        ad_id_df = FB_Connector.get_ad_id_list(account_id)
        ad_id_df = ad_id_df.drop_duplicates(subset=['ad_name'],keep='first')
        ad_preview_list = FB_Connector.get_ad_preview_list(ad_id_df)
        
        df = pd.DataFrame(ad_preview_list)
        if len(df) > 0:
            df['ad_preview'] = df['ad_preview'].str.replace('amp;','')
            
            client = bigquery.Client()
            BQ_Connector.delete_data(client,"rda_analytics_temp","media_facebook_ad_preview_map_name_temp")
            BQ_Connector.load_data(client,"rda_analytics_temp","media_facebook_ad_preview_map_name_temp",df)
            BQ_Connector.delete_when_match(client,"rda_analytics","media_facebook_ad_preview_map_name","rda_analytics_temp","media_facebook_ad_preview_map_name_temp",
                                        "ON ori.ad_name = temp.ad_name ")
            BQ_Connector.load_data(client, "rda_analytics","media_facebook_ad_preview_map_name",df)
            msg = f"🔷🔖 Content: <b>Facebook Ad Preview</b> Executed Successfully on 📅 "
            h_function.send_gg_chat_noti(msg)
    return json.dumps({'success': "Update FB Ad Preview Succesfully"}), 200


@app.route('/update_content_fb_pagepost_insight', methods=['POST'])
def update_content_fb_pagepost_insight():
    pages = FB_Connector.get_all_page()
    client = bigquery.Client(project="hmth-448709")
    
    rows = FB_Connector.get_page_post_insight(pages)
    df = pd.DataFrame(rows)
    if len(df) > 0:
        df = FB_Connector.clean_page_post_insight(df)
        BQ_Connector.delete_data(client,"rda_analytics_temp","media_facebook_page_post_insight_temp")
        BQ_Connector.load_data(client,"rda_analytics_temp","media_facebook_page_post_insight_temp",df)
        BQ_Connector.delete_when_match(client,"rda_analytics","media_facebook_page_post_insight","rda_analytics_temp","media_facebook_page_post_insight_temp",
                                    "ON ori.page_id = temp.page_id AND ori.post_id = temp.post_id AND ori.updated_time = temp.updated_time ")
        BQ_Connector.load_data(client, "rda_analytics","media_facebook_page_post_insight",df)
        msg = f"🔷🔖 Content: <b>Facebook Page Post I</b> Executed Successfully on 📅 "
        h_function.send_gg_chat_noti(msg)
    return json.dumps({'success': "Update FB Page Post Succesfully"}), 200


@app.route('/check_updated_media_data', methods=['POST'])
def check_updated_media_data():

    client = bigquery.Client()
    
    gg_AdBasics = BQ_Connector.get_recent_date(client,"segments_date", "rda_analytics", "media_google_AdBasicStats")
    gg_AdCrossDevice = BQ_Connector.get_recent_date(client,"segments_date", "rda_analytics", "media_google_AdCrossDeviceConversionStats")
    gg_AdGroupBasics = BQ_Connector.get_recent_date(client,"segments_date", "rda_analytics", "media_google_AdGroupBasicStats")
    gg_AdGroupCrossDevice = BQ_Connector.get_recent_date(client,"segments_date", "rda_analytics", "media_google_AdGroupCrossDeviceConversionStats")
    gg_CampaignBasic = BQ_Connector.get_recent_date(client,"segments_date", "rda_analytics", "media_google_CampaignBasicStats")
    gg_CampaignCrossDevice = BQ_Connector.get_recent_date(client,"segments_date", "rda_analytics", "media_google_CampaignCrossDeviceConversionStats")
    # gg_KeywordBasic = BQ_Connector.get_recent_date(client,"segments_date", "rda_analytics", "media_google_KeywordBasicStats")
    # gg_KeywordCrossDevice = BQ_Connector.get_recent_date(client,"segments_date", "rda_analytics", "media_google_KeywordCrossDeviceConversionStats")
    gg_VideoBasicStats = BQ_Connector.get_recent_date(client,"segments_date", "rda_analytics", "media_google_VideoBasicStats")
    # gg_VideoConversionStats = BQ_Connector.get_recent_date(client,"segments_date", "rda_analytics", "media_google_VideoConversionStats")
    gg_VideoNonClickStats = BQ_Connector.get_recent_date(client,"segments_date", "rda_analytics", "media_google_VideoNonClickStats")
    
    tiktok_main = BQ_Connector.get_recent_date(client, "stat_time_day","rda_analytics", "media_tiktok_main")
    tiktok_event = BQ_Connector.get_recent_date(client, "stat_time_day","rda_analytics", "media_tiktok_event")
    tiktok_pageevent = BQ_Connector.get_recent_date(client, "stat_time_day","rda_analytics", "media_tiktok_pageevent")
    tiktok_shopads = BQ_Connector.get_recent_date(client, "stat_time_day","rda_analytics", "media_tiktok_shop_ads")
    
    # rtb_data = BQ_Connector.get_recent_date(client, "day","rda_analytics", "media_rtb_house")
    
    fb_main = BQ_Connector.get_recent_date(client, "date_start","rda_analytics", "media_facebook_main")
    fb_actions = BQ_Connector.get_recent_date(client, "date_start","rda_analytics", "media_facebook_action")
    fb_catalog = BQ_Connector.get_recent_date(client, "date_start","rda_analytics", "media_facebook_catalog_segment")
    
    # line_main = BQ_Connector.get_recent_date(client, "date","rda_analytics", "media_line")
    
    msg1 = f"🌳 <b>Google</b> \n GG AdBasicsStats: 📅{gg_AdBasics}\n  GG AdCrossDevice: 📅{gg_AdCrossDevice}\n GG AdGroupBasicsStats: 📅{gg_AdGroupBasics}\n  GG AdGroupCrossDevice: 📅{gg_AdGroupCrossDevice}\n  GG CampaignBasicStats: 📅{gg_CampaignBasic}\n  GG CampaignCrossDevice: 📅{gg_CampaignCrossDevice}\n  GG VideoBasicStats: 📅{gg_VideoBasicStats}\n GG VideoNonClickStats: 📅{gg_VideoNonClickStats}"
    msg2 = f"🎶 <b>Tiktok</b> \n TT Main: 📅{tiktok_main}\n TT Event: 📅{tiktok_event}\n TT Page Event: 📅{tiktok_pageevent}\n TT Shop Ads: 📅{tiktok_shopads}"
    # msg3 = f"🌻 <b>RTB</b> \n RTB: 📅{rtb_data}"
    msg3 = f"🔷 <b>Facebook</b> \n FB Main: 📅{fb_main}\n FB Actions: 📅{fb_actions}\n FB Catalog Segment: 📅{fb_catalog}"
    # msg5 = f"💬 <b>Line</b> \n Line: 📅{line_main}"
    
    h_function.send_gg_chat_noti_with_divider(msg1,msg2,msg3)
    return json.dumps({'success': 'Check Media Recent Date'}), 200

@app.route('/update_google_search_console', methods=['POST'])
def update_google_search_console():
    oriDataset = "rda_analytics"
    tempDataset = "rda_analytics_temp"

    oriTable_main = "google_google_search_console_main"
    tempTable_main = "google_google_search_console_main_temp"
    
    oriTable_keyword = "google_google_search_console_keyword"
    tempTable_keyword = "google_google_search_console_keyword_temp"

    oriTable_page = "google_google_search_console_page"
    tempTable_page = "google_google_search_console_page_temp"

    oriTable_keyword_page = "google_google_search_console_keyword_page"
    tempTable_keyword_page = "google_google_search_console_keyword_page_temp"


    site = "https://www.hyundai.com/th/"

    try:
        #Keyword
        scopes=['https://www.googleapis.com/auth/webmasters.readonly']
        service_account_info = json.loads(os.environ.get("search-console-account"))
        creds = Credentials.from_service_account_info(service_account_info, scopes=scopes)
        service = build('searchconsole', 'v1', credentials=creds)

        bangkok_tz = pytz.timezone('Asia/Bangkok')
        dateNow = datetime.now(bangkok_tz)
        dateNowBack3Days = dateNow - timedelta(days=3)
        dateNowBack10Days = dateNow - timedelta(days=10)

        dateStartFormated = dateNowBack10Days.strftime('%Y-%m-%d')
        dateEndFOrmated = dateNowBack3Days.strftime('%Y-%m-%d')

        dimensions_main = ['date', 'country', 'device']
        dimensions_keyword = ['date', 'country', 'device', 'query']
        dimensions_page = ['date', 'page']
        dimensions_keyword_page = ['date', 'country', 'device', 'query', 'page']

        data = GG_Connector.query_search_analytics(service, site, dateStartFormated, dateEndFOrmated, dimensions_keyword)
        main = GG_Connector.query_search_analytics(service, site, dateStartFormated, dateEndFOrmated, dimensions_main)

        page = GG_Connector.query_search_analytics(service, site, dateStartFormated, dateEndFOrmated, dimensions_page)
        keyword_page = GG_Connector.query_search_analytics(service, site, dateStartFormated, dateEndFOrmated, dimensions_keyword_page)

        if 'rows' in data:
            logging.info("In if data")
            listData = data['rows']
            transformed_data = GG_Connector.transform_data_keyword(listData, site)
            df = pd.DataFrame(transformed_data)
            df['date'] = pd.to_datetime(df['date'])
            client = bigquery.Client(project="hmth-448709")
            BQ_Connector.delete_data(client,tempDataset,tempTable_keyword)
            logging.info("Delete temp Done")
            BQ_Connector.load_data(client,tempDataset,tempTable_keyword,df)
            logging.info("Load temp Done")
            condition = "ON ori.date = temp.date AND ori.url = temp.url AND ori.country = temp.country AND ori.device = temp.device AND ori.keyword = temp.keyword "
            BQ_Connector.delete_when_match(client,oriDataset,oriTable_keyword,tempDataset,tempTable_keyword,condition)
            logging.info("Delete temp  match Done")
            BQ_Connector.load_data(client,oriDataset,oriTable_keyword,df)

            logging.info("Load Done")
        
            msg = f"🌳 Content: <b>Google Search Console [Keyword]</b> Executed Successfully on 📅 "
            notires, noticode = h_function.send_gg_chat_noti(msg)
            # return json.dumps({'success': msg}), 200
        
        if 'rows' in main:
            logging.info("In if main")
            listMain = main['rows']
            transformed_data_main = GG_Connector.transform_data_main(listMain, site)
            df_main = pd.DataFrame(transformed_data_main)
            df_main['date'] = pd.to_datetime(df_main['date'])
            client = bigquery.Client(project="hmth-448709")
            BQ_Connector.delete_data(client,tempDataset,tempTable_main)
            logging.info("Delete temp Done")
            BQ_Connector.load_data(client,tempDataset,tempTable_main,df_main)
            logging.info("Load temp Done")
            condition = "ON ori.date = temp.date AND ori.url = temp.url AND ori.country = temp.country AND ori.device = temp.device "
            BQ_Connector.delete_when_match(client,oriDataset,oriTable_main,tempDataset,tempTable_main,condition)
            logging.info("Delete temp  match Done")
            BQ_Connector.load_data(client,oriDataset,oriTable_main,df_main)

            logging.info("Load Done")
        
            msg = f"🌳 Content: <b>Google Search Console [Main]</b> Executed Successfully on 📅 "
            notires, noticode = h_function.send_gg_chat_noti(msg)    
            # return json.dumps({'success': msg}), 200

        if 'rows' in page:
            logging.info("In if page")
            listPage = page['rows']
            transformed_data_page = GG_Connector.transform_data_page(listPage, site)
            df_page = pd.DataFrame(transformed_data_page)
            df_page['date'] = pd.to_datetime(df_page['date'])
            client = bigquery.Client(project="hmth-448709")
            BQ_Connector.delete_data(client,tempDataset,tempTable_page)
            logging.info("Delete temp Done")
            BQ_Connector.load_data(client,tempDataset,tempTable_page,df_page)
            logging.info("Load temp Done")
            condition = "ON ori.date = temp.date AND ori.url = temp.url AND ori.page = temp.page "
            BQ_Connector.delete_when_match(client,oriDataset,oriTable_page,tempDataset,tempTable_page,condition)
            logging.info("Delete temp  match Done")
            BQ_Connector.load_data(client,oriDataset,oriTable_page,df_page)

            logging.info("Load Done")
        
            msg = f"🌳 Content: <b>Google Search Console [Page]</b> Executed Successfully on 📅 "
            notires, noticode = h_function.send_gg_chat_noti(msg)

        if 'rows' in keyword_page:
            logging.info("In if page")
            listKeywordPage = keyword_page['rows']
            transformed_data_keyword_page = GG_Connector.transform_data_page(listKeywordPage, site)
            df_kw_page = pd.DataFrame(transformed_data_keyword_page)
            df_kw_page['date'] = pd.to_datetime(df_kw_page['date'])
            client = bigquery.Client(project="hmth-448709")
            BQ_Connector.delete_data(client,tempDataset,tempTable_keyword_page)
            logging.info("Delete temp Done")
            BQ_Connector.load_data(client,tempDataset,tempTable_keyword_page,df_kw_page)
            logging.info("Load temp Done")
            condition =  "ON ori.date = temp.date AND ori.url = temp.url AND ori.country = temp.country AND ori.device = temp.device AND ori.keyword = temp.keyword AND ori.page = temp.page "
            BQ_Connector.delete_when_match(client,oriDataset,oriTable_keyword_page,tempDataset,tempTable_keyword_page,condition)
            logging.info("Delete temp  match Done")
            BQ_Connector.load_data(client,oriDataset,oriTable_keyword_page,df_kw_page)

            logging.info("Load Done")
        
            msg = f"🌳 Content: <b>Google Search Console [Keyword page]</b> Executed Successfully on 📅 "
            notires, noticode = h_function.send_gg_chat_noti(msg)

        return json.dumps({'message': "no data"}), 200
                
    except Exception as e:
        logging.error(e)
        msgError = f"🌳 Content: <b>Google Search Console</b> Executed Google Search console Error on 📅 "
        notires, noticode = h_function.send_gg_chat_noti(msgError)
        return json.dumps({'error': f"{e}"}), 500
    
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))