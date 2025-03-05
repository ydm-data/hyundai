import os
import ast
import time
import random
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone, date

from facebook_business.adobjects.user import User
from facebook_business.adobjects.page import Page
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.api import FacebookAdsApi
from connector_BQ import BQ_Connector

from google.cloud import bigquery

class FB_Connector:
    
    def get_myaccount(access_token):
        FacebookAdsApi.init(access_token=access_token)
        user_id = 'me'
        me = User(user_id)
        my_accounts = me.get_ad_accounts(
            fields=[
                AdAccount.Field.name,
                AdAccount.Field.created_time
            ],
            params={'limit': 20000}
        ) 
        return my_accounts  
    
    def get_asynjob(my_accounts,target_account,fields, days, level):
        utc_timezone = timezone.utc
        aware_datetime = datetime.now().replace(tzinfo=utc_timezone)
        days = days
        asyn_job_list = []
        start_date = aware_datetime - timedelta(days=days)

        for each_account in my_accounts:
            if each_account['id'] in target_account:
                params={
                    'level':level,
                    'time_range': {
                        'since': start_date.strftime('%Y-%m-%d'),
                        'until': datetime.now().strftime('%Y-%m-%d')
                    },
                    'time_increment': 1
                }

                async_job = AdAccount(each_account['id']).get_insights(fields=fields, params=params, is_async=True)
                async_job.api_get()

                while async_job[AdReportRun.Field.async_status]!= 'Job Completed':
                    time.sleep(1)
                    async_job.api_get()

                asyn_job_list = asyn_job_list + list(async_job.get_result())
        
        return asyn_job_list
    
    def get_asynjob_month(my_accounts,target_account,fields):
        today = datetime.today()
        first_day_of_this_month = today.replace(day=1)
        last_day_of_last_month = first_day_of_this_month - timedelta(days=1)
        first_day_of_last_month = last_day_of_last_month.replace(day=1)
        
        asyn_job_list = []

        for each_account in my_accounts:
            if each_account['id'] in target_account:
                params={
                    'level':'ad',
                    'time_range': {
                        'since': first_day_of_last_month.strftime('%Y-%m-%d'),
                        'until': last_day_of_last_month.strftime('%Y-%m-%d')
                    },
                    'time_increment': 1
                }

                async_job = AdAccount(each_account['id']).get_insights(fields=fields, params=params, is_async=True)
                async_job.api_get()

                while async_job[AdReportRun.Field.async_status]!= 'Job Completed':
                    time.sleep(1)
                    async_job.api_get()

                asyn_job_list = asyn_job_list + list(async_job.get_result())
            
            return asyn_job_list
    
    def get_conversion_list(action_data):
        custom_conversion_list = []
        for each_account in action_data['account_id'].unique():
            ad_account_for_customconversion = AdAccount('act_'+each_account)
            conversion_list = ad_account_for_customconversion.get_custom_conversions(
                fields=[
                    "id",
                    "name"
                ]
            )
            if len(conversion_list) > 0:
                for each_custom_conversion in conversion_list:
                    temp = {
                        "id": each_custom_conversion['id'],
                        "name" : each_custom_conversion['name']
                    }
                    custom_conversion_list.append(temp)
        return custom_conversion_list
    
    def clean_action_data(custom_conversion_df,ads_data):
        ads_data['actions'] = ads_data['actions'].astype(str).str.replace(".", "_")
        ads_data['action_values'] = ads_data['action_values'].astype(str).str.replace(".", "_")
        
        if len(custom_conversion_df) > 0:
            ads_data['actions'] = ads_data['actions'].replace(custom_conversion_df['id'].to_list(),custom_conversion_df['name'].to_list(), regex=True)
            ads_data['action_values'] = ads_data['action_values'].replace(custom_conversion_df['id'].to_list(),custom_conversion_df['name'].to_list(), regex=True)

        ads_data['actions'] = ads_data['actions'].str.replace(".","_")
        ads_data['actions'] = ads_data['actions'].str.replace("-","_")
        ads_data['actions'] = ads_data['actions'].str.replace("(ce)","_ce")
            
        ads_data['action_values'] = ads_data['action_values'].str.replace(".","_")
        ads_data['action_values'] = ads_data['action_values'].str.replace("-","_")
        ads_data['action_values'] = ads_data['action_values'].str.replace("(ce)","_ce")
        return ads_data
        
    def transform_load_action_data(custom_conversion_list,action_data):
        if len(custom_conversion_list) > 0:
    
            custom_conversion_df = pd.DataFrame(custom_conversion_list)
            custom_conversion_df = custom_conversion_df.replace(" ", "", regex=True)

            if len(action_data) > 0:
                action_data = action_data.replace(custom_conversion_df['id'].to_list(),custom_conversion_df['name'].to_list(), regex=True)
            
        if len(action_data) > 0:
        
            action_data = FB_Connector.clean_action_data(action_data)
            project_id = 'hmth-448709'
            client = bigquery.Client(project=project_id)
            
            BQ_Connector.delete_data(client,"media_data_facebook","facebook_action_unpivot_temp")
            BQ_Connector.load_data(client, 'media_data_facebook', 'facebook_action_unpivot_temp', action_data)
            
            condition = "ON (ori.ad_id = temp.ad_id AND ori.date_start = temp.date_start AND ori.action_type = temp.action_type) "
            BQ_Connector.delete_when_match(client,"media_data_google",'media_facebook_action_unpivot','media_data_facebook','facebook_action_unpivot_temp',condition)
            BQ_Connector.load_data(client, 'media_data_google', 'media_facebook_action_unpivot', action_data)
            
    def clean_ads_data(asyn_job_list):
        ads_data_list = []

        for each_record in asyn_job_list:
            
            if "video_p100_watched_actions" in each_record: p100_wactched = each_record['video_p100_watched_actions'][0]['value'] 
            else: p100_wactched = None
            
            if "video_p25_watched_actions" in each_record: p25_wactched = each_record['video_p25_watched_actions'][0]['value'] 
            else: p25_wactched = None
            
            if "video_p50_watched_actions" in each_record: p50_wactched = each_record['video_p50_watched_actions'][0]['value'] 
            else: p50_wactched = None
            
            if "video_p75_watched_actions" in each_record: p75_wactched = each_record['video_p75_watched_actions'][0]['value'] 
            else: p75_wactched = None
            
            if "video_p95_watched_actions" in each_record: p95_wactched = each_record['video_p95_watched_actions'][0]['value'] 
            else: p95_wactched = None
            
            if "video_play_actions" in each_record: video_play_actions = each_record['video_play_actions'][0]['value'] 
            else: video_play_actions = None
            
            if "video_thruplay_watched_actions" in each_record: video_thruplay_watched_actions = each_record['video_thruplay_watched_actions'][0]['value']
            else: video_thruplay_watched_actions = None
            
            if "cost_per_outbound_click" in each_record: cost_per_outbound_click = each_record['cost_per_outbound_click'][0]['value']
            else: cost_per_outbound_click = None
            
            if "cost_per_unique_outbound_click" in each_record: cost_per_unique_outbound_click = each_record['cost_per_unique_outbound_click'][0]['value']
            else: cost_per_unique_outbound_click = None
            
            if "outbound_clicks" in each_record: outbound_clicks = each_record['outbound_clicks'][0]['value']
            else: outbound_clicks = None
            
            if "outbound_clicks_ctr" in each_record: outbound_clicks_ctr = each_record['outbound_clicks_ctr'][0]['value']
            else: outbound_clicks_ctr = None
            
            temp_data = {
                "account_id" : each_record['account_id'],
                "account_name": each_record['account_name'],
                "campaign_id" : each_record['campaign_id'],
                "campaign_name" : each_record['campaign_name'],
                "adset_name" : each_record['adset_name'], 
                "adset_id" : each_record['adset_id'],
                "ad_name": each_record['ad_name'],
                "ad_id" : each_record['ad_id'],
                "spend" : each_record['spend'],
                "impressions" : each_record['impressions'],
                "reach" : each_record['reach'],
                "clicks" : each_record['clicks'],
                "conversion_rate_ranking" : each_record['conversion_rate_ranking'],
                "cost_per_dda_countby_convs" : each_record.get('cost_per_dda_countby_convs',None),
                "cost_per_inline_link_click" : each_record.get('cost_per_inline_link_click',None),
                "cost_per_inline_post_engagement" : each_record.get('cost_per_inline_post_engagement',None),
                "cpc" : each_record.get('cpc',None),
                "cpm" : each_record.get('cpm',None),
                "cpp" : each_record.get('cpp',None),
                "ctr" : each_record.get('ctr',None),
                "created_time" : each_record['created_time'],
                "date_start" : each_record['date_start'],
                "date_stop" : each_record['date_stop'],
                "frequency" : each_record['frequency'],
                "objective" : each_record['objective'],
                "video_p100_watched_actions" : p100_wactched,
                "video_p25_watched_actions" : p25_wactched,
                "video_p50_watched_actions" : p50_wactched,
                "video_p75_watched_actions" : p75_wactched,
                "video_p95_watched_actions" : p95_wactched,
                "video_play_actions": video_play_actions,
                "video_thruplay_watched_actions" : video_thruplay_watched_actions,
                "cost_per_outbound_click": cost_per_outbound_click,
                "cost_per_unique_outbound_click" : cost_per_unique_outbound_click,
                "outbound_clicks" : outbound_clicks,
                "outbound_clicks_ctr" : outbound_clicks_ctr
            }
            
            ads_data_list.append(temp_data)
            ads_data = pd.DataFrame(ads_data_list)
        return ads_data
    
    
    def clean_ads_action_data(asyn_job_list):
        ads_data_list = []

        for each_record in asyn_job_list:
            temp_data = {
                "account_id" : each_record['account_id'],
                "account_name": each_record['account_name'],
                "campaign_id" : each_record['campaign_id'],
                "campaign_name" : each_record['campaign_name'],
                "adset_name" : each_record['adset_name'], 
                "adset_id" : each_record['adset_id'],
                "ad_name": each_record['ad_name'],
                "ad_id" : each_record['ad_id'],
                "created_time" : each_record['created_time'],
                "date_start" : each_record['date_start'],
                "date_stop" : each_record['date_stop'],
                "objective" : each_record['objective'],
                "actions": each_record.get('actions',[]),
                "action_values": each_record.get('action_values',[])
            }
            ads_data_list.append(temp_data)

        ads_data = pd.DataFrame(ads_data_list)
        return ads_data
    
    def clean_ads_catalog_segment_data(asyn_job_list):
        ads_data_list = []

        for each_record in asyn_job_list:
            temp_data = {
                "account_id" : each_record['account_id'],
                "account_name": each_record['account_name'],
                "campaign_id" : each_record['campaign_id'],
                "campaign_name" : each_record['campaign_name'],
                "adset_name" : each_record['adset_name'], 
                "adset_id" : each_record['adset_id'],
                "ad_name": each_record['ad_name'],
                "ad_id" : each_record['ad_id'],
                "created_time" : each_record['created_time'],
                "date_start" : each_record['date_start'],
                "date_stop" : each_record['date_stop'],
                "objective" : each_record['objective'],
                "catalog_segment_actions": each_record.get('catalog_segment_actions',[]),
                "catalog_segment_values": each_record.get('catalog_segment_value',[])
            }
            ads_data_list.append(temp_data)
            
        ads_data = pd.DataFrame(ads_data_list)
        return ads_data


    def transform_fb_main(ads_data):

        ads_data[['spend', 'impressions', 'reach', 'clicks','cost_per_dda_countby_convs',
            'cost_per_inline_link_click','cost_per_inline_post_engagement',
            'cpc', 'cpm', 'cpp', 'ctr','frequency','video_p100_watched_actions',
            'video_p25_watched_actions', 'video_p50_watched_actions', 'video_play_actions',
            'video_p75_watched_actions', 'video_p95_watched_actions', 'video_thruplay_watched_actions',
            'cost_per_outbound_click','cost_per_unique_outbound_click','outbound_clicks',
            'outbound_clicks_ctr']] = ads_data[['spend', 'impressions', 'reach', 'clicks','cost_per_dda_countby_convs',
            'cost_per_inline_link_click','cost_per_inline_post_engagement',
            'cpc', 'cpm', 'cpp', 'ctr','frequency','video_p100_watched_actions',
            'video_p25_watched_actions', 'video_p50_watched_actions', 'video_play_actions',
            'video_p75_watched_actions', 'video_p95_watched_actions', 'video_thruplay_watched_actions',
            'cost_per_outbound_click','cost_per_unique_outbound_click','outbound_clicks','outbound_clicks_ctr']].astype(float)
            
        ads_data['created_time'] = pd.to_datetime(ads_data['created_time'])
        ads_data['date_start'] = pd.to_datetime(ads_data['date_start'])
        ads_data['date_stop'] = pd.to_datetime(ads_data['date_stop'])
        return ads_data
    
    def get_fb_main_fields():
        fields=[
                AdsInsights.Field.account_id,AdsInsights.Field.account_name,AdsInsights.Field.campaign_id,AdsInsights.Field.campaign_name,AdsInsights.Field.adset_name,
                AdsInsights.Field.adset_id,AdsInsights.Field.ad_name,AdsInsights.Field.ad_id,AdsInsights.Field.spend,AdsInsights.Field.ad_impression_actions, AdsInsights.Field.objective,
                AdsInsights.Field.impressions,AdsInsights.Field.reach,AdsInsights.Field.clicks,AdsInsights.Field.video_play_actions,
                AdsInsights.Field.ad_click_actions,AdsInsights.Field.conversion_rate_ranking, AdsInsights.Field.video_thruplay_watched_actions,
                AdsInsights.Field.converted_product_quantity,AdsInsights.Field.converted_product_value,AdsInsights.Field.cost_per_2_sec_continuous_video_view,
                AdsInsights.Field.cost_per_ad_click,AdsInsights.Field.cost_per_conversion,AdsInsights.Field.cost_per_dda_countby_convs,
                AdsInsights.Field.cost_per_estimated_ad_recallers,AdsInsights.Field.cost_per_inline_link_click,AdsInsights.Field.date_start,
                AdsInsights.Field.cpc,AdsInsights.Field.cpm,AdsInsights.Field.cpp,AdsInsights.Field.ctr,AdsInsights.Field.created_time,
                AdsInsights.Field.date_stop,AdsInsights.Field.frequency,AdsInsights.Field.purchase_roas,AdsInsights.Field.video_continuous_2_sec_watched_actions,
                AdsInsights.Field.video_p100_watched_actions,AdsInsights.Field.video_p25_watched_actions,AdsInsights.Field.video_p50_watched_actions,AdsInsights.Field.video_p75_watched_actions,
                AdsInsights.Field.video_p95_watched_actions,AdsInsights.Field.video_play_retention_0_to_15s_actions,
                AdsInsights.Field.video_play_retention_20_to_60s_actions,AdsInsights.Field.video_play_retention_graph_actions,
                AdsInsights.Field.video_time_watched_actions,
                AdsInsights.Field.cost_per_outbound_click,AdsInsights.Field.cost_per_unique_outbound_click,AdsInsights.Field.outbound_clicks,AdsInsights.Field.outbound_clicks_ctr
        ]
        return fields
    
    def get_fb_action_fields():
        fields=[
            AdsInsights.Field.account_id,AdsInsights.Field.account_name,AdsInsights.Field.campaign_id,AdsInsights.Field.campaign_name,
            AdsInsights.Field.adset_id,AdsInsights.Field.adset_name,AdsInsights.Field.ad_id,AdsInsights.Field.ad_name,AdsInsights.Field.objective,
            AdsInsights.Field.actions,AdsInsights.Field.action_values,AdsInsights.Field.created_time
        ]   
        return fields
    
    def get_fb_catalog_fields():
        fields=[
                AdsInsights.Field.account_id,AdsInsights.Field.account_name,AdsInsights.Field.campaign_id,AdsInsights.Field.campaign_name,
                AdsInsights.Field.adset_id,AdsInsights.Field.adset_name,AdsInsights.Field.ad_id,AdsInsights.Field.ad_name,AdsInsights.Field.objective,
                AdsInsights.Field.catalog_segment_actions, AdsInsights.Field.catalog_segment_value,AdsInsights.Field.created_time
        ]
        return fields
    
    def sanitize_nested_list(nested_list):
        if isinstance(nested_list, list):
            return [
                {
                    "action_type": item.get("action_type", None),
                    "value": float(item.get("value", 0.0)) if item.get("value") not in [None, ""] else None
                }
                for item in nested_list
            ]
        return []

    def clean_nested_action_data(ads_data):
        ads_data["actions"] = ads_data["actions"].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)
        ads_data['action_values'] = ads_data['action_values'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)
        
        def sanitize_nested_list(nested_list):
            if isinstance(nested_list, list):
                return [
                    {
                        "action_type": item.get("action_type", None),
                        "value": float(item.get("value", 0.0)) if item.get("value") not in [None, ""] else None
                    }
                    for item in nested_list
                ]
            return []

        # Apply the function to sanitize data
        ads_data["actions"] = ads_data["actions"].apply(sanitize_nested_list)
        ads_data["action_values"] = ads_data["action_values"].apply(sanitize_nested_list)
        return ads_data
    
    
    def clean_nested_catalog_segment_data(ads_data):
        ads_data['catalog_segment_actions'] = ads_data['catalog_segment_actions'].astype(str).str.replace(".", "_")
        ads_data['catalog_segment_values'] = ads_data['catalog_segment_values'].astype(str).str.replace(".", "_")

        ads_data['catalog_segment_actions'] = ads_data['catalog_segment_actions'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)
        ads_data['catalog_segment_values'] = ads_data['catalog_segment_values'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)

        # make the value of action_type become float
        def sanitize_nested_list(nested_list):
            if isinstance(nested_list, list):
                return [
                    {
                        "action_type": item.get("action_type", None),
                        "value": float(item.get("value", 0.0)) if item.get("value") not in [None, ""] else None
                    }
                    for item in nested_list
                ]
            return []

        # Apply the function to sanitize data
        ads_data["catalog_segment_actions"] = ads_data["catalog_segment_actions"].apply(sanitize_nested_list)
        ads_data["catalog_segment_values"] = ads_data["catalog_segment_values"].apply(sanitize_nested_list)
        
        return ads_data
    
    
    def get_all_page():
        access_token = os.getenv('FB_PAGE_TOKEN')
        FacebookAdsApi.init(access_token=access_token)
        me = User('me')
        fields = ['id', 'name','access_token']
        params = {'limit': 1000}
        pages = me.get_accounts(fields=fields, params=params)
        return pages
    
    
    def get_page_insight(pages, metric):
        rows = []
        for page in pages:
            page_id = page['id']
            page_name = page['name']
            page_access_token = page.get('access_token')

            FacebookAdsApi.init(access_token=page_access_token)
            page_obj = Page(page_id)
            params = {
                'metric': metric,
                'period': 'day',
                'since': (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d'),
                'until': datetime.now().strftime("%Y-%m-%d")
            }
            insights = page_obj.get_insights(params=params)
            
            for entry in insights:
                for value in entry['values']:
                    rows.append({
                        'page_id': page_id,
                        'page_name': page_name,
                        'metric': entry['name'],
                        'date': value['end_time'],
                        'value': value['value'],
                        'updated_time': datetime.now().strftime('%Y-%m-%d')
                    })         
        return rows
    
    def extract_value(d):
        if isinstance(d, dict):
            return list(d.values())[0] 
        return d
    
    def clean_page_insight(df):
        df = df.pivot_table(index=['page_id', 'page_name', 'date', 'updated_time'], columns='metric', values='value', aggfunc='first').reset_index()
        df['date'] = pd.to_datetime(df['date'])
        df['updated_time'] = pd.to_datetime(df['date'])
        return df
    
    def get_page_insight_metric():
        metric = [
            'page_total_actions','page_post_engagements','page_fan_adds_by_paid_non_paid_unique','page_lifetime_engaged_followers_unique',
            'page_daily_follows','page_daily_follows_unique','page_daily_unfollows_unique','page_follows',
            'page_impressions','page_impressions_unique','page_impressions_paid','page_impressions_paid_unique','page_impressions_viral',
            'page_impressions_viral_unique','page_impressions_nonviral','page_impressions_nonviral_unique',
            'page_posts_impressions','page_posts_impressions_unique','page_posts_impressions_paid','page_posts_impressions_paid_unique',
            'page_posts_impressions_organic','page_posts_impressions_organic_unique','page_posts_served_impressions_organic_unique',
            'page_posts_impressions_viral','page_posts_impressions_viral_unique','page_posts_impressions_nonviral','page_posts_impressions_nonviral_unique',
            'page_actions_post_reactions_like_total','page_actions_post_reactions_love_total','page_actions_post_reactions_wow_total',
            'page_actions_post_reactions_haha_total','page_actions_post_reactions_sorry_total','page_actions_post_reactions_anger_total',
            'page_fan_adds','page_fan_adds_unique','page_fan_removes','page_fan_removes_unique','page_views_total',
            'page_fans','page_fans_locale','page_fans_city','page_fans_country','page_fan_adds',
            'page_daily_video_ad_break_ad_impressions_by_crosspost_status','page_daily_video_ad_break_cpm_by_crosspost_status',
            'post_video_ad_break_ad_impressions','post_video_ad_break_earnings','post_video_ad_break_ad_cpm','creator_monetization_qualified_views'
        ]
        return metric
    
    def page_insight_nested(pivoted_df):
        pivoted_df['page_fans_city'] = pivoted_df['page_fans_city'].apply(
        lambda row: [{'city': city, 'value': value} for city, value in row.items()] if isinstance(row, dict) else []
        )
        pivoted_df['page_fans_country'] = pivoted_df['page_fans_country'].apply(
            lambda row: [{'country': country, 'value': value} for country, value in row.items()] if isinstance(row, dict) else []
        )
        pivoted_df['page_fans_locale'] = pivoted_df['page_fans_locale'].apply(
            lambda row: [{'locale': locale, 'value': value} for locale, value in row.items()] if isinstance(row, dict) else []
        )
        return pivoted_df
    
    def fetch_ad_creatives(ad_id):
        # This function fetches creatives for a single ad_id with retry logic and exponential backoff
        retry_attempts = 5
        for attempt in range(retry_attempts):
            creatives = Ad(ad_id).get_ad_creatives(fields=[
                'id', 'account_id', 'actor_id', 'adlabels', 
                'authorization_category', 'auto_update', 'body','branded_content_sponsor_page_id',
                'bundle_folder_id', 'categorization_criteria', 'category_media_source', 'collaborative_ads_lsb_image_bank_id',
                'creative_sourcing_spec', 'degrees_of_freedom_spec', 'destination_set_id', 'dynamic_ad_voice',
                'effective_authorization_category', 'effective_instagram_media_id', 'effective_instagram_story_id', 
                'effective_object_story_id', 'enable_direct_install', 'enable_launch_instant_app', 
                'id', 'image_crops', 'image_hash', 'image_url', 'instagram_actor_id',
                'instagram_permalink_url', 'instagram_story_id', 'instagram_user_id', 'interactive_components_spec',
                'link_deep_link_url', 'link_destination_display_url', 'name',
                'object_story_id', 'object_type', 'object_url', 'omnichannel_link_spec',
                'photo_album_source_object_story_id', 'place_page_set_id', 'platform_customizations', 'playable_asset_id',
                'portrait_customizations', 'status',
                'template_url', 'template_url_spec', 'thumbnail_id', 'thumbnail_url', 'title', 'url_tags', 
                'use_page_actor_override', 'video_id', 'image_file', 'is_dco_internal'
            ], params={"thumbnail_height":500,"thumbnail_width":500})
            return creatives
        
    def get_all_ad_data(client,account_ids):
        query = f"""
            SELECT distinct account_id,account_name,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name 
            FROM `hmth-448709.rda_analytics.media_facebook` 
            WHERE account_id IN ({account_ids})
        """

        query_job = client.query(query)
        df = query_job.to_dataframe()
        return df
    
    def get_all_adcreative_data(client,account_ids):
        query = f"""
            SELECT distinct account_id,account_name,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name 
            FROM `hmth-448709.rda_analytics.media_facebook_adcreative` 
            WHERE account_id IN ({account_ids})
        """

        query_job = client.query(query)
        df = query_job.to_dataframe()
        return df
    
    def get_adcreative_from_ad_id(target_df):
        AdCreative_list = []
        index = 0
        for ad_id in target_df['ad_id'][0:]:
            if ad_id.isdigit():
                print(ad_id, end=": ")
                print(index, end=",")
                # Fetch the ad creatives, with retry and exponential backoff in case of rate limit
                try:
                    creatives = FB_Connector.fetch_ad_creatives(ad_id)
                except Exception as e:
                    pass
                
                if creatives:
                    for creative in creatives:
                        creative_data = creative.export_all_data()
                        creative_data['ad_id'] = ad_id
                        AdCreative_list.append(creative_data)

                index += 1
                # Add a random delay to reduce the likelihood of hitting rate limits
                time.sleep(random.uniform(1, 3))
        return AdCreative_list