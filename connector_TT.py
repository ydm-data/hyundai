import os
import json
import math
import requests
import numpy as np
import pandas as pd

from six import string_types
from datetime import datetime, timedelta
from six.moves.urllib.parse import urlencode, urlunparse  # noqa

class TT_connector:
    
    def build_url(path, query=""):
        scheme, netloc = "https", "business-api.tiktok.com"
        return urlunparse((scheme, netloc, path, "", query, ""))
    
    def get(json_str, PATH):
        args = json.loads(json_str)
        query_string = urlencode({k: v if isinstance(v, string_types) else json.dumps(v) for k, v in args.items()})
        url = TT_connector.build_url(PATH, query_string)
        headers = {
            "Access-Token": os.environ.get("TIKTOKTOKEN"),
        }
        rsp = requests.get(url, headers=headers)
        return rsp.json()
    
    def get_data(advertiser_ids_list, metrics_list,day):
        advertiser_ids = json.dumps(advertiser_ids_list)
        report_type = 'BASIC'
        data_level = 'AUCTION_AD'
        dimensions_list = ["ad_id", 'stat_time_day']
        dimensions = json.dumps(dimensions_list)

        metrics = json.dumps(metrics_list)
        page_size = 1000
        window = day
        ads_metrics_list = []
        start_date_obj = datetime.now() - timedelta(days=window)
        end_date_obj = datetime.now() - timedelta(days=window-1)
        start_date = start_date_obj.strftime('%Y-%m-%d')
        end_date = end_date_obj.strftime('%Y-%m-%d')
        
        PATH = "/open_api/v1.3/report/integrated/get/"

        for i in range(window):
            for j in range (len(advertiser_ids_list)):

                advertiser_ids_1 = [advertiser_ids_list[j]] # OR ADVERTISER_IDS, BC_ID
                advertiser_ids = json.dumps(advertiser_ids_1)

                my_args = "{\"data_level\": \"%s\", \"advertiser_ids\": %s, \"report_type\": \"%s\", \"dimensions\": %s, \"start_date\": \"%s\", \"end_date\": \"%s\", \"metrics\": %s, \"page_size\" : \"%s\"}" % (data_level, advertiser_ids, report_type, dimensions, start_date, end_date, metrics, page_size)
                response = TT_connector.get(my_args, PATH)
                
                for each_record in response['data']['list']:
                    metric = each_record['metrics']
                    dimension = each_record['dimensions']
                    merge_dict = metric.copy()
                    merge_dict.update(dimension)
                    ads_metrics_list.append(merge_dict)

            start_date_obj += timedelta(days=1)
            end_date_obj += timedelta(days=1)
            start_date = start_date_obj.strftime('%Y-%m-%d')
            end_date = end_date_obj.strftime('%Y-%m-%d')
                
        df = pd.DataFrame(ads_metrics_list)
        df = df.drop_duplicates()
        return df    
    
    def get_data_lifetime(advertiser_ids_list, metrics_list,data_level,dimensions_list):
        report_type = 'BASIC'
        page_size = 1000
        query_lifetime = 'true'
        
        dimensions = json.dumps(dimensions_list)
        metrics = json.dumps(metrics_list)
        ads_metrics_list = []
        PATH = "/open_api/v1.3/report/integrated/get/"

        for each_account in advertiser_ids_list:
            page = 1
            print(each_account)
            while True:
                print(page)
                my_args = "{\"data_level\": \"%s\", \"advertiser_id\": %s, \"report_type\": \"%s\", \"dimensions\": %s, \"metrics\": %s, \"page_size\" : \"%s\",\"page\": \"%s\", \"query_lifetime\": \"%s\"}" % (data_level, each_account, report_type, dimensions, metrics, page_size,page, query_lifetime)
                response2 = TT_connector.get(my_args, PATH)
                for each_record in response2['data']['list']:
                    metric = each_record['metrics']
                    dimension = each_record['dimensions']
                    merge_dict = metric.copy()
                    merge_dict.update(dimension)
                    ads_metrics_list.append(merge_dict)
                page += 1
                if response2['data']['page_info']['total_page'] == 0:
                    break
                if response2['data']['page_info']['page'] / response2['data']['page_info']['total_page'] == 1:
                    break
                        
        df = pd.DataFrame(ads_metrics_list)
        df = df.drop_duplicates()
        return df  
            
    def get_data_month(advertiser_ids_list,metrics_list):
        advertiser_ids = json.dumps(advertiser_ids_list)
        report_type = 'BASIC'
        data_level = 'AUCTION_AD'
        dimensions_list = ["ad_id", 'stat_time_day']
        dimensions = json.dumps(dimensions_list)

        metrics = json.dumps(metrics_list)
        page_size = 1000
        ads_metrics_list = []
        today = datetime.today()
        first_day_of_this_month = today.replace(day=1)
        last_day_of_last_month = first_day_of_this_month - timedelta(days=1)
        first_day_of_last_month = last_day_of_last_month.replace(day=1)
        window = (last_day_of_last_month - first_day_of_last_month).days + 1
        
        start_date_obj = first_day_of_last_month
        end_date_obj = start_date_obj + timedelta(days=1)

        start_date = start_date_obj.strftime('%Y-%m-%d')
        end_date = end_date_obj.strftime('%Y-%m-%d')
        
        PATH = "/open_api/v1.3/report/integrated/get/"

        for i in range(window):
            for j in range (len(advertiser_ids_list)):

                advertiser_ids_1 = [advertiser_ids_list[j]] # OR ADVERTISER_IDS, BC_ID
                advertiser_ids = json.dumps(advertiser_ids_1)

                my_args = "{\"data_level\": \"%s\", \"advertiser_ids\": %s, \"report_type\": \"%s\", \"dimensions\": %s, \"start_date\": \"%s\", \"end_date\": \"%s\", \"metrics\": %s, \"page_size\" : \"%s\"}" % (data_level, advertiser_ids, report_type, dimensions, start_date, end_date, metrics, page_size)
                response = TT_connector.get(my_args, PATH)
                
                for each_record in response['data']['list']:
                    metric = each_record['metrics']
                    dimension = each_record['dimensions']
                    merge_dict = metric.copy()
                    merge_dict.update(dimension)
                    ads_metrics_list.append(merge_dict)

            start_date_obj += timedelta(days=1)
            end_date_obj += timedelta(days=1)
            start_date = start_date_obj.strftime('%Y-%m-%d')
            end_date = end_date_obj.strftime('%Y-%m-%d')
                
        df = pd.DataFrame(ads_metrics_list)
        df = df.drop_duplicates()
        return df
    
    def get_main_metrics():
        metrics_list = ['campaign_name','campaign_id','adgroup_name', 'adgroup_id', 'ad_name', 'ad_id', 'advertiser_name','advertiser_id','objective_type',
                        'impressions', 'reach', 'spend', 'billed_cost', 'video_play_actions', 'video_watched_6s', 'video_views_p25','video_views_p50', 
                        'video_views_p75', 'video_views_p100', 'skan_total_sales_lead','video_watched_2s', 'frequency', 'ad_profile_image','campaign_budget',
                        'app_promotion_type','cash_spend','voucher_spend','cpc','cpm','clicks','ctr','conversion','cost_per_conversion','conversion_rate',
                        'real_time_conversion','real_time_conversion_rate','result','cost_per_result','result_rate','real_time_result','is_aco',
                        'is_smart_creative','engagements','profile_visits','profile_visits_rate','likes','comments','shares','follows','clicks_on_music_disc',
                        'duet_clicks','stitch_clicks','sound_usage_clicks','anchor_clicks','anchor_click_rate','real_time_app_install','app_install',
                        'registration','total_registration','purchase','total_purchase','app_event_add_to_cart','complete_payment_roas','complete_payment',
                        'total_landing_page_view','total_pageview','page_browse_view','button_click','user_registration','page_content_view_events',
                        'web_event_add_to_cart','on_web_order','initiate_checkout','add_billing','page_event_search','form','download_start',
                        'on_web_add_to_wishlist','on_web_subscribe','skan_result']
        return metrics_list
    
    def get_main_metrics_lifetime_ad():
        metrics_list = ['campaign_name','campaign_id','adgroup_name', 'adgroup_id', 'ad_name', 'ad_id','advertiser_name','advertiser_id',
                    'objective_type','impressions', 'reach', 'spend', 'billed_cost', 'video_play_actions', 'video_watched_6s', 'video_views_p25', 
                    'video_views_p50', 'video_views_p75', 'video_views_p100', 'skan_total_sales_lead','video_watched_2s', 'frequency', 'ad_profile_image',
                    'campaign_budget','app_promotion_type','cpc','cpm','clicks','ctr','conversion','cost_per_conversion','conversion_rate',
                    'real_time_conversion','real_time_conversion_rate','result','cost_per_result','result_rate','real_time_result','is_aco','is_smart_creative','engagements',
                    'profile_visits','profile_visits_rate','likes','comments','shares','follows','clicks_on_music_disc','duet_clicks','stitch_clicks','sound_usage_clicks',
                    'anchor_clicks','anchor_click_rate','real_time_app_install','app_install','registration','total_registration','purchase','total_purchase','app_event_add_to_cart',
                    'complete_payment_roas','complete_payment','total_landing_page_view','total_pageview','page_browse_view','button_click','user_registration',
                    'page_content_view_events','web_event_add_to_cart','on_web_order','initiate_checkout','add_billing','page_event_search','form','download_start',
                    'on_web_add_to_wishlist','on_web_subscribe','skan_result']
        return metrics_list
    
    def get_main_metrics_adgroup():
        metrics_list = ['campaign_name','campaign_id','adgroup_name','advertiser_name','advertiser_id',
                    'objective_type','impressions', 'reach', 'spend', 'billed_cost', 'video_play_actions', 'video_watched_6s', 'video_views_p25', 
                    'video_views_p50', 'video_views_p75', 'video_views_p100', 'skan_total_sales_lead','video_watched_2s', 'frequency',
                    'campaign_budget','app_promotion_type','cpc','cpm','clicks','ctr','conversion','cost_per_conversion','conversion_rate',
                    'real_time_conversion','real_time_conversion_rate','result','cost_per_result','result_rate','real_time_result','is_aco','engagements',
                    'profile_visits','profile_visits_rate','likes','comments','shares','follows','clicks_on_music_disc','duet_clicks','stitch_clicks','sound_usage_clicks',
                    'anchor_clicks','anchor_click_rate','real_time_app_install','app_install','registration','total_registration','purchase','total_purchase','app_event_add_to_cart',
                    'complete_payment_roas','complete_payment','total_landing_page_view','total_pageview','page_browse_view','button_click','user_registration',
                    'page_content_view_events','web_event_add_to_cart','on_web_order','initiate_checkout','add_billing','page_event_search','form','download_start',
                    'on_web_add_to_wishlist','on_web_subscribe','skan_result']
        return metrics_list
    
    def get_main_metrics_campaign():
        metrics_list = ['campaign_name','advertiser_name','advertiser_id',
                    'objective_type','impressions', 'reach', 'spend', 'billed_cost', 'video_play_actions', 'video_watched_6s', 'video_views_p25', 
                    'video_views_p50', 'video_views_p75', 'video_views_p100', 'skan_total_sales_lead','video_watched_2s', 'frequency',
                    'campaign_budget','app_promotion_type','cpc','cpm','clicks','ctr','conversion','cost_per_conversion','conversion_rate',
                    'real_time_conversion','real_time_conversion_rate','result','cost_per_result','result_rate','real_time_result','is_aco','engagements',
                    'profile_visits','profile_visits_rate','likes','comments','shares','follows','clicks_on_music_disc','duet_clicks','stitch_clicks','sound_usage_clicks',
                    'anchor_clicks','anchor_click_rate','real_time_app_install','app_install','registration','total_registration','purchase','total_purchase','app_event_add_to_cart',
                    'complete_payment_roas','complete_payment','total_landing_page_view','total_pageview','page_browse_view','button_click','user_registration',
                    'page_content_view_events','web_event_add_to_cart','on_web_order','initiate_checkout','add_billing','page_event_search','form','download_start',
                    'on_web_add_to_wishlist','on_web_subscribe','skan_result']
        return metrics_list
    
    def get_main_metrics_advertiser():
        metrics_list = ['advertiser_name','advertiser_id',
                    'objective_type','impressions', 'reach', 'spend', 'billed_cost', 'video_play_actions', 'video_watched_6s', 'video_views_p25', 
                    'video_views_p50', 'video_views_p75', 'video_views_p100', 'skan_total_sales_lead','video_watched_2s', 'frequency',
                    'app_promotion_type','cpc','cpm','clicks','ctr','conversion','cost_per_conversion','conversion_rate',
                    'real_time_conversion','real_time_conversion_rate','result','cost_per_result','result_rate','real_time_result','is_aco','engagements',
                    'profile_visits','profile_visits_rate','likes','comments','shares','follows','clicks_on_music_disc','duet_clicks','stitch_clicks','sound_usage_clicks',
                    'anchor_clicks','anchor_click_rate','real_time_app_install','app_install','registration','total_registration','purchase','total_purchase','app_event_add_to_cart',
                    'complete_payment_roas','complete_payment','total_landing_page_view','total_pageview','page_browse_view','button_click','user_registration',
                    'page_content_view_events','web_event_add_to_cart','on_web_order','initiate_checkout','add_billing','page_event_search','form','download_start',
                    'on_web_add_to_wishlist','on_web_subscribe','skan_result']
        return metrics_list
    
    def get_event_metrics():
        metrics_list = ['campaign_name','campaign_id','adgroup_name', 'adgroup_id', 'ad_name', 'ad_id','advertiser_name','advertiser_id',
                    'cost_per_app_install','cost_per_registration','registration_rate','cost_per_total_registration','cost_per_purchase',
                    'purchase_rate','cost_per_total_purchase','value_per_total_purchase','total_purchase_value','total_active_pay_roas',
                    'cost_per_app_event_add_to_cart','app_event_add_to_cart_rate','total_app_event_add_to_cart','cost_per_total_app_event_add_to_cart',
                    'value_per_total_app_event_add_to_cart','total_app_event_add_to_cart_value','checkout','cost_per_checkout','checkout_rate',
                    'total_checkout','cost_per_total_checkout','value_per_checkout','total_checkout_value','view_content', 'cost_per_view_content',
                    'view_content_rate','total_view_content','cost_per_total_view_content','value_per_total_view_content','total_view_content_value',
                    'next_day_open','cost_per_next_day_open','next_day_open_rate','total_next_day_open','cost_per_next_day_open','add_payment_info',
                    'cost_per_add_payment_info','add_payment_info_rate','total_add_payment_info','cost_total_add_payment_info','add_to_wishlist',
                    'cost_per_add_to_wishlist','add_to_wishlist_rate','total_add_to_wishlist','cost_per_total_add_to_wishlist','value_per_total_add_to_wishlist',
                    'total_add_to_wishlist_value','launch_app','cost_per_launch_app','launch_app_rate','total_launch_app','cost_per_total_launch_app',
                    'complete_tutorial','cost_per_complete_tutorial','complete_tutorial_rate','total_complete_tutorial','cost_per_total_complete_tutorial',
                    'value_per_total_complete_tutorial','total_complete_tutorial_value','create_group','cost_per_create_group','create_group_rate',
                    'total_create_group','cost_per_total_create_group','value_per_total_create_group','total_create_group_value','join_group',
                    'cost_per_join_group','join_group_rate','total_join_group','cost_per_total_join_group','value_per_total_join_group','total_join_group_value',
                    'create_gamerole','cost_per_create_gamerole','create_gamerole_rate','total_create_gamerole','cost_per_total_create_gamerole',
                    'value_per_total_create_gamerole','total_create_gamerole_value','spend_credits','cost_per_spend_credits','spend_credits_rate',
                    'total_spend_credits','cost_per_total_spend_credits','value_per_total_spend_credits','total_spend_credits_value','achieve_level',
                    'cost_per_achieve_level','achieve_level_rate','total_achieve_level','cost_per_total_achieve_level','value_per_total_achieve_level',
                    'total_achieve_level_value','unlock_achievement','cost_per_unlock_achievement','unlock_achievement_rate','total_unlock_achievement',
                    'cost_per_total_unlock_achievement','value_per_total_unlock_achievement','total_unlock_achievement_value','sales_lead','cost_per_sales_lead',
                    'sales_lead_rate','total_sales_lead','cost_per_total_sales_lead','value_per_total_sales_lead','total_sales_lead_value','in_app_ad_click',
                    'cost_per_in_app_ad_click','in_app_ad_click_rate','total_in_app_ad_click','cost_per_total_in_app_ad_click','value_per_total_in_app_ad_click',
                    'value_per_total_in_app_ad_click','total_in_app_ad_click_value','in_app_ad_impr','cost_per_in_app_ad_impr','in_app_ad_impr_rate',
                    'total_in_app_ad_impr','cost_per_total_in_app_ad_impr','value_per_total_in_app_ad_impr','total_in_app_ad_impr_value','unique_ad_impression_events',
                    'cost_per_unique_ad_impression_event','customized_ad_impression_event_rate','ads_impression_events','cost_pre_ad_impression_event',
                    'value_per_ad_impression_event','total_ad_impression_events_value','total_ad_impression_roas','loan_apply','cost_per_loan_apply',
                    'loan_apply_rate','total_loan_apply','cost_per_total_loan_apply','loan_credit','cost_per_loan_credit','loan_credit_rate','total_loan_credit',
                    'cost_per_total_loan_credit','loan_disbursement','cost_per_loan_disbursement','loan_disbursement_rate','total_loan_disbursement',
                    'cost_per_total_loan_disbursement','login','cost_per_login','login_rate','total_login','cost_per_total_login','ratings','cost_per_ratings',
                    'ratings_rate','total_ratings','cost_per_total_ratings','value_per_total_ratings','total_ratings_value','search','cost_per_search',
                    'search_rate','total_search','cost_per_total_search','start_trial','cost_per_start_trial','start_trial_rate','total_start_trial',
                    'cost_per_total_start_trial','subscribe','cost_per_subscribe','subscribe_rate','total_subscribe','cost_per_total_subscribe',
                    'value_per_total_subscribe','total_subscribe_value','unique_custom_app_events','cost_per_unique_custom_app_event','custom_app_event_rate',
                    'custom_app_events','cost_per_custom_app_event','value_per_custom_app_event','custom_app_events_value']
        return metrics_list
    
    def get_pageevent_metrics():
        metrics_list = ['campaign_name','campaign_id','adgroup_name', 'adgroup_id', 'ad_name', 'ad_id', 'advertiser_name','advertiser_id',
                'complete_payment_roas','complete_payment','cost_per_complete_payment','complete_payment_rate','value_per_complete_payment','total_complete_payment_rate',
                'total_landing_page_view','cost_per_landing_page_view','landing_page_view_rate','total_pageview','cost_per_pageview','pageview_rate',
                'avg_value_per_pageview','total_value_per_pageview','page_browse_view','cost_per_page_browse_view','page_browse_view_rate',
                'total_page_browse_view_value','value_per_page_browse_view','button_click','cost_per_button_click','button_click_rate',
                'value_per_button_click','total_button_click_value','online_consult','cost_per_online_consult','online_consult_rate',
                'value_per_online_consult','total_online_consult_value','user_registration','cost_per_user_registration','user_registration_rate',
                'value_per_user_registration','total_user_registration_value','page_content_view_events','cost_per_page_content_view_event',
                'page_content_view_event_rate','value_per_page_content_view_event','total_page_view_content_events_value','product_details_page_browse',
                'cost_per_product_details_page_browse','product_details_page_browse_rate','value_per_product_details_page_browse',
                'total_product_details_page_browse_value','web_event_add_to_cart','cost_per_web_event_add_to_cart','web_event_add_to_cart_rate',
                'value_per_web_event_add_to_cart','total_web_event_add_to_cart_value','on_web_order','cost_per_on_web_order','on_web_order_rate',
                'value_per_on_web_order','total_on_web_order_value','initiate_checkout','cost_per_initiate_checkout','initiate_checkout_rate',
                'value_per_initiate_checkout','total_initiate_checkout_value','add_billing','cost_per_add_billing','add_billing_rate','value_per_add_billing',
                'total_add_billing_value','page_event_search','cost_per_page_event_search','page_event_search_rate','value_per_page_event_search',
                'total_page_event_search_value','form','cost_per_form','form_rate','value_per_form','total_form_value','download_start','cost_per_download_start',
                'download_start_rate','value_per_download_start','total_download_start_value','on_web_add_to_wishlist','cost_per_on_web_add_to_wishlist',
                'on_web_add_to_wishlist_per_click','value_per_on_web_add_to_wishlist','total_on_web_add_to_wishlist_value','on_web_subscribe',
                'cost_per_on_web_subscribe','on_web_subscribe_per_click','value_per_on_web_subscribe','total_on_web_subscribe_value',
                'custom_page_events','cost_per_custom_page_event','custom_page_event_rate','value_per_custom_page_event','custom_page_events_value']
        return metrics_list
    
    def get_shopads_metrics():
        metrics_list = ['campaign_name','campaign_id','adgroup_name', 'adgroup_id', 'ad_name', 'ad_id','advertiser_name','advertiser_id','onsite_shopping_roas',
                        'onsite_shopping','cost_per_onsite_shopping','onsite_shopping_rate','value_per_onsite_shopping','total_onsite_shopping_value',
                        'onsite_initiate_checkout_count','cost_per_onsite_initiate_checkout_count','onsite_initiate_checkout_count_rate',
                        'value_per_onsite_initiate_checkout_count','total_onsite_initiate_checkout_count_value','onsite_on_web_detail',
                        'cost_per_onsite_on_web_detail','onsite_on_web_detail_rate','value_per_onsite_on_web_detail','total_onsite_on_web_detail_value',
                        'onsite_on_web_cart','cost_per_onsite_on_web_cart','onsite_on_web_cart_rate','value_per_onsite_on_web_cart','total_onsite_on_web_cart_value']
        return metrics_list
    
    def convert_main_data(data):
        data['stat_time_day'] = pd.to_datetime(data['stat_time_day'])
        data[['complete_payment_roas','cost_per_result','result_rate','total_landing_page_view','billed_cost','comments','total_registration', 'real_time_conversion', 'video_views_p25', 'campaign_budget','stitch_clicks','duet_clicks',
            'real_time_app_install', 'anchor_clicks','page_content_view_events', 'real_time_conversion_rate','reach','video_views_p50','web_event_add_to_cart', 'frequency', 'real_time_result','cost_per_conversion', 'on_web_order', 'on_web_add_to_wishlist',
            'on_web_subscribe','app_event_add_to_cart','cpc','spend','shares','skan_total_sales_lead', 'form', 'complete_payment', 'video_views_p100','conversion_rate', 'cash_spend', 'total_purchase', 'likes','initiate_checkout', 'download_start','follows',
            'video_watched_6s', 'registration','purchase','video_views_p75', 'user_registration', 'result', 'conversion','video_play_actions','engagements', 'voucher_spend', 'clicks','profile_visits_rate','total_pageview','impressions','anchor_click_rate', 
            'video_watched_2s','cpm','clicks_on_music_disc', 'app_install', 'add_billing', 'button_click','page_event_search', 'skan_result','ctr','page_browse_view', 'profile_visits', 'sound_usage_clicks']] = data[['complete_payment_roas','cost_per_result',
            'result_rate','total_landing_page_view','billed_cost','comments','total_registration', 'real_time_conversion', 'video_views_p25', 'campaign_budget','stitch_clicks','duet_clicks','real_time_app_install', 'anchor_clicks','page_content_view_events', 
            'real_time_conversion_rate','reach','video_views_p50','web_event_add_to_cart', 'frequency', 'real_time_result','cost_per_conversion', 'on_web_order', 'on_web_add_to_wishlist','on_web_subscribe','app_event_add_to_cart','cpc','spend','shares',
            'skan_total_sales_lead', 'form', 'complete_payment', 'video_views_p100','conversion_rate', 'cash_spend', 'total_purchase', 'likes','initiate_checkout', 'download_start','follows','video_watched_6s', 'registration','purchase','video_views_p75', 
            'user_registration', 'result', 'conversion','video_play_actions','engagements', 'voucher_spend', 'clicks','profile_visits_rate','total_pageview','impressions','anchor_click_rate', 'video_watched_2s','cpm','clicks_on_music_disc', 'app_install', 
            'add_billing', 'button_click','page_event_search', 'skan_result','ctr','page_browse_view', 'profile_visits', 'sound_usage_clicks']].astype(float)
        return data
    
    def convert_main_data_lifetime(data):
        data['campaign_budget'] = data['campaign_budget'].replace('-',np.nan)
        data[['complete_payment_roas','cost_per_result','result_rate','total_landing_page_view','billed_cost','comments',
            'total_registration', 'real_time_conversion', 'video_views_p25', 'campaign_budget','stitch_clicks','duet_clicks',
            'real_time_app_install', 'anchor_clicks','page_content_view_events', 'real_time_conversion_rate','reach', 
            'video_views_p50','web_event_add_to_cart', 'frequency', 'real_time_result','cost_per_conversion', 'on_web_order', 'on_web_add_to_wishlist',
            'on_web_subscribe','app_event_add_to_cart','cpc','spend','shares','skan_total_sales_lead', 'form', 'complete_payment', 'video_views_p100',
            'conversion_rate', 'total_purchase', 'likes','initiate_checkout', 'download_start','follows',
            'video_watched_6s', 'registration','purchase','video_views_p75', 'user_registration', 'result', 'conversion','video_play_actions', 
            'engagements','clicks','profile_visits_rate','total_pageview','impressions','anchor_click_rate', 'video_watched_2s', 
            'cpm','clicks_on_music_disc', 'app_install', 'add_billing', 'button_click','page_event_search', 'skan_result','ctr',
            'page_browse_view', 'profile_visits', 'sound_usage_clicks']] = data[['complete_payment_roas','cost_per_result','result_rate','total_landing_page_view','billed_cost','comments',
            'total_registration', 'real_time_conversion', 'video_views_p25', 'campaign_budget','stitch_clicks','duet_clicks',
            'real_time_app_install', 'anchor_clicks','page_content_view_events', 'real_time_conversion_rate','reach', 
            'video_views_p50','web_event_add_to_cart', 'frequency', 'real_time_result','cost_per_conversion', 'on_web_order', 'on_web_add_to_wishlist',
            'on_web_subscribe','app_event_add_to_cart','cpc','spend','shares','skan_total_sales_lead', 'form', 'complete_payment', 'video_views_p100',
            'conversion_rate', 'total_purchase', 'likes','initiate_checkout', 'download_start','follows',
            'video_watched_6s', 'registration','purchase','video_views_p75', 'user_registration', 'result', 'conversion','video_play_actions', 
            'engagements', 'clicks','profile_visits_rate','total_pageview','impressions','anchor_click_rate', 'video_watched_2s', 
            'cpm','clicks_on_music_disc', 'app_install', 'add_billing', 'button_click','page_event_search', 'skan_result','ctr',
            'page_browse_view', 'profile_visits', 'sound_usage_clicks']].astype(float)
        return data
    
    def convert_main_data_advertiser_level(data):
        data[['complete_payment_roas','cost_per_result','result_rate','total_landing_page_view','billed_cost','comments','total_registration', 'real_time_conversion', 
              'video_views_p25','stitch_clicks','duet_clicks','real_time_app_install', 'anchor_clicks','page_content_view_events', 'real_time_conversion_rate','reach',
              'video_views_p50','web_event_add_to_cart', 'frequency', 'real_time_result','cost_per_conversion', 'on_web_order', 'on_web_add_to_wishlist','on_web_subscribe',
              'app_event_add_to_cart','cpc','spend','shares','skan_total_sales_lead', 'form', 'complete_payment', 'video_views_p100','conversion_rate', 'total_purchase', 
              'likes','initiate_checkout', 'download_start','follows','video_watched_6s', 'registration','purchase','video_views_p75', 'user_registration', 'result', 'conversion',
              'video_play_actions','engagements','clicks','profile_visits_rate','total_pageview','impressions','anchor_click_rate', 'video_watched_2s','cpm','clicks_on_music_disc', 
              'app_install', 'add_billing', 'button_click','page_event_search', 'skan_result','ctr','page_browse_view', 'profile_visits', 'sound_usage_clicks']] = data[['complete_payment_roas',
              'cost_per_result','result_rate','total_landing_page_view','billed_cost','comments','total_registration', 'real_time_conversion', 'video_views_p25','stitch_clicks','duet_clicks',
              'real_time_app_install', 'anchor_clicks','page_content_view_events', 'real_time_conversion_rate','reach','video_views_p50','web_event_add_to_cart', 'frequency', 'real_time_result',
              'cost_per_conversion', 'on_web_order', 'on_web_add_to_wishlist','on_web_subscribe','app_event_add_to_cart','cpc','spend','shares','skan_total_sales_lead', 'form', 'complete_payment', 
              'video_views_p100','conversion_rate', 'total_purchase', 'likes','initiate_checkout', 'download_start','follows','video_watched_6s', 'registration','purchase','video_views_p75', 
              'user_registration', 'result', 'conversion','video_play_actions','engagements', 'clicks','profile_visits_rate','total_pageview','impressions','anchor_click_rate', 'video_watched_2s',
              'cpm','clicks_on_music_disc', 'app_install', 'add_billing', 'button_click','page_event_search', 'skan_result','ctr','page_browse_view', 'profile_visits', 'sound_usage_clicks']].astype(float)
        return data
    
    def convert_event_data(data):
        data['stat_time_day'] = pd.to_datetime(data['stat_time_day'])
        data[['purchase_rate','value_per_total_join_group','cost_per_create_gamerole','total_in_app_ad_click_value','spend_credits_rate','loan_apply','total_loan_credit','total_app_event_add_to_cart','value_per_total_achieve_level','total_create_group_value','cost_per_ratings',
            'loan_disbursement_rate','create_group','join_group','add_payment_info','total_achieve_level_value','next_day_open','total_spend_credits','total_join_group','spend_credits','cost_per_total_achieve_level','cost_per_launch_app','cost_per_total_create_gamerole',
            'total_spend_credits_value','subscribe_rate','achieve_level_rate','value_per_total_app_event_add_to_cart','total_create_gamerole','complete_tutorial_rate','total_unlock_achievement','cost_per_spend_credits','in_app_ad_click_rate','cost_per_total_registration',
            'cost_per_unlock_achievement','add_to_wishlist_rate','total_achieve_level','cost_per_total_view_content','registration_rate','cost_per_checkout','cost_per_unique_custom_app_event','total_ad_impression_roas','launch_app','total_subscribe_value','cost_per_purchase',
            'unique_ad_impression_events','value_per_total_subscribe','loan_credit_rate','cost_per_total_start_trial','cost_per_total_create_group','total_add_payment_info','login_rate','total_in_app_ad_click','cost_pre_ad_impression_event','cost_per_total_loan_apply','total_search',
            'cost_per_loan_apply','total_in_app_ad_impr','value_per_total_add_to_wishlist','cost_per_total_checkout','total_create_gamerole_value','checkout','cost_per_complete_tutorial','create_gamerole_rate','total_sales_lead','cost_per_total_loan_credit','cost_total_add_payment_info',
            'total_checkout_value','cost_per_total_loan_disbursement','cost_per_total_add_to_wishlist','cost_per_login','value_per_total_create_group','loan_apply_rate','app_event_add_to_cart_rate','custom_app_event_rate','value_per_total_sales_lead','custom_app_events','add_to_wishlist',
            'cost_per_add_to_wishlist','cost_per_total_subscribe','unique_custom_app_events','total_login','start_trial','cost_per_search','cost_per_custom_app_event','cost_per_loan_credit','total_add_to_wishlist','next_day_open_rate','total_subscribe','cost_per_total_join_group',
            'cost_per_add_payment_info','value_per_total_create_gamerole','in_app_ad_impr','achieve_level','value_per_ad_impression_event','unlock_achievement','unlock_achievement_rate','sales_lead_rate','total_unlock_achievement_value','value_per_total_ratings',
            'custom_app_events_value','cost_per_unique_ad_impression_event','create_group_rate','cost_per_app_install','cost_per_app_event_add_to_cart','total_sales_lead_value','cost_per_view_content','total_complete_tutorial','total_launch_app','total_loan_apply','customized_ad_impression_event_rate',
            'add_payment_info_rate','total_start_trial','ratings_rate','cost_per_total_spend_credits','join_group_rate','view_content_rate','checkout_rate','total_join_group_value','cost_per_total_sales_lead','complete_tutorial','cost_per_in_app_ad_impr',
            'value_per_total_unlock_achievement','subscribe','cost_per_join_group','total_view_content_value','total_purchase_value','total_view_content','launch_app_rate','sales_lead','in_app_ad_impr_rate','loan_credit','cost_per_total_ratings','total_checkout',
            'cost_per_registration','start_trial_rate','total_loan_disbursement','total_active_pay_roas','total_complete_tutorial_value','total_in_app_ad_impr_value','total_add_to_wishlist_value','total_ratings','search_rate','total_next_day_open','ads_impression_events',
            'ratings','cost_per_subscribe','cost_per_next_day_open','total_app_event_add_to_cart_value','value_per_total_in_app_ad_impr','cost_per_total_app_event_add_to_cart','create_gamerole','cost_per_create_group','cost_per_total_unlock_achievement','total_ratings_value',
            'cost_per_sales_lead','cost_per_achieve_level','search','cost_per_total_in_app_ad_click','value_per_checkout','cost_per_total_launch_app','cost_per_total_in_app_ad_impr','value_per_total_view_content','in_app_ad_click','value_per_total_complete_tutorial',
            'total_ad_impression_events_value','value_per_total_in_app_ad_click','cost_per_total_complete_tutorial','cost_per_total_search','value_per_total_purchase','login','view_content','loan_disbursement','cost_per_total_purchase','cost_per_loan_disbursement',
            'cost_per_start_trial','value_per_custom_app_event','cost_per_in_app_ad_click','cost_per_total_login','value_per_total_spend_credits','total_create_group']] = data[['purchase_rate','value_per_total_join_group','cost_per_create_gamerole','total_in_app_ad_click_value','spend_credits_rate','loan_apply',
            'total_loan_credit','total_app_event_add_to_cart','value_per_total_achieve_level','total_create_group_value','cost_per_ratings','loan_disbursement_rate','create_group','join_group','add_payment_info','total_achieve_level_value','next_day_open','total_spend_credits',
            'total_join_group','spend_credits','cost_per_total_achieve_level','cost_per_launch_app','cost_per_total_create_gamerole','total_spend_credits_value','subscribe_rate','achieve_level_rate','value_per_total_app_event_add_to_cart','total_create_gamerole',
            'complete_tutorial_rate','total_unlock_achievement','cost_per_spend_credits','in_app_ad_click_rate','cost_per_total_registration','cost_per_unlock_achievement','add_to_wishlist_rate','total_achieve_level','cost_per_total_view_content','registration_rate',
            'cost_per_checkout','cost_per_unique_custom_app_event','total_ad_impression_roas','launch_app','total_subscribe_value','cost_per_purchase','unique_ad_impression_events','value_per_total_subscribe','loan_credit_rate','cost_per_total_start_trial','cost_per_total_create_group',
            'total_add_payment_info','login_rate','total_in_app_ad_click','cost_pre_ad_impression_event','cost_per_total_loan_apply','total_search','cost_per_loan_apply','total_in_app_ad_impr','value_per_total_add_to_wishlist','cost_per_total_checkout','total_create_gamerole_value',
            'checkout','cost_per_complete_tutorial','create_gamerole_rate','total_sales_lead','cost_per_total_loan_credit','cost_total_add_payment_info','total_checkout_value','cost_per_total_loan_disbursement','cost_per_total_add_to_wishlist','cost_per_login','value_per_total_create_group',
            'loan_apply_rate','app_event_add_to_cart_rate','custom_app_event_rate','value_per_total_sales_lead','custom_app_events','add_to_wishlist','cost_per_add_to_wishlist','cost_per_total_subscribe','unique_custom_app_events','total_login','start_trial','cost_per_search',
            'cost_per_custom_app_event','cost_per_loan_credit','total_add_to_wishlist','next_day_open_rate','total_subscribe','cost_per_total_join_group','cost_per_add_payment_info','value_per_total_create_gamerole','in_app_ad_impr','achieve_level','value_per_ad_impression_event',
            'unlock_achievement','unlock_achievement_rate','sales_lead_rate','total_unlock_achievement_value','value_per_total_ratings','custom_app_events_value','cost_per_unique_ad_impression_event','create_group_rate','cost_per_app_install','cost_per_app_event_add_to_cart',
            'total_sales_lead_value','cost_per_view_content','total_complete_tutorial','total_launch_app','total_loan_apply','customized_ad_impression_event_rate','add_payment_info_rate','total_start_trial','ratings_rate','cost_per_total_spend_credits','join_group_rate','view_content_rate',
            'checkout_rate','total_join_group_value','cost_per_total_sales_lead','complete_tutorial','cost_per_in_app_ad_impr','value_per_total_unlock_achievement','subscribe','cost_per_join_group','total_view_content_value','total_purchase_value',
            'total_view_content','launch_app_rate','sales_lead','in_app_ad_impr_rate','loan_credit','cost_per_total_ratings','total_checkout','cost_per_registration','start_trial_rate','total_loan_disbursement','total_active_pay_roas','total_complete_tutorial_value',
            'total_in_app_ad_impr_value','total_add_to_wishlist_value','total_ratings','search_rate','total_next_day_open','ads_impression_events','ratings','cost_per_subscribe','cost_per_next_day_open','total_app_event_add_to_cart_value','value_per_total_in_app_ad_impr',
            'cost_per_total_app_event_add_to_cart','create_gamerole','cost_per_create_group','cost_per_total_unlock_achievement','total_ratings_value','cost_per_sales_lead','cost_per_achieve_level','search','cost_per_total_in_app_ad_click','value_per_checkout','cost_per_total_launch_app',
            'cost_per_total_in_app_ad_impr','value_per_total_view_content','in_app_ad_click','value_per_total_complete_tutorial','total_ad_impression_events_value','value_per_total_in_app_ad_click','cost_per_total_complete_tutorial','cost_per_total_search',
            'value_per_total_purchase','login','view_content','loan_disbursement','cost_per_total_purchase','cost_per_loan_disbursement','cost_per_start_trial','value_per_custom_app_event','cost_per_in_app_ad_click','cost_per_total_login','value_per_total_spend_credits',
            'total_create_group']].astype(float)
        return data
    
    def convert_pageevent_data(data):
        data['stat_time_day'] = pd.to_datetime(data['stat_time_day'])
        data[['value_per_initiate_checkout','page_browse_view','value_per_download_start','total_online_consult_value','total_page_view_content_events_value','total_user_registration_value','total_on_web_subscribe_value','total_web_event_add_to_cart_value','cost_per_page_browse_view','page_browse_view_rate','cost_per_pageview','on_web_add_to_wishlist','download_start_rate','page_content_view_event_rate','value_per_page_browse_view','cost_per_on_web_order',
            'form','avg_value_per_pageview','total_value_per_pageview','on_web_subscribe','cost_per_online_consult','complete_payment_roas','total_form_value','total_page_browse_view_value','form_rate','total_page_event_search_value','download_start','on_web_order_rate','value_per_page_content_view_event','value_per_on_web_add_to_wishlist','cost_per_web_event_add_to_cart','cost_per_on_web_add_to_wishlist','value_per_on_web_order','total_pageview',
            'cost_per_complete_payment','total_download_start_value','total_complete_payment_rate','cost_per_on_web_subscribe','web_event_add_to_cart','value_per_product_details_page_browse','cost_per_page_event_search','value_per_on_web_subscribe','cost_per_download_start','complete_payment_rate','button_click_rate','custom_page_events_value','cost_per_initiate_checkout','value_per_complete_payment','cost_per_custom_page_event','custom_page_event_rate',
            'button_click','product_details_page_browse_rate','total_add_billing_value','on_web_add_to_wishlist_per_click','value_per_add_billing','product_details_page_browse','user_registration','cost_per_page_content_view_event','pageview_rate','total_on_web_add_to_wishlist_value','web_event_add_to_cart_rate','value_per_form','custom_page_events','total_initiate_checkout_value','value_per_button_click','online_consult_rate','value_per_custom_page_event',
            'on_web_order','landing_page_view_rate','initiate_checkout','value_per_online_consult','cost_per_button_click','value_per_page_event_search','cost_per_form','on_web_subscribe_per_click','online_consult','page_event_search','value_per_user_registration','add_billing_rate','cost_per_add_billing','cost_per_landing_page_view','total_landing_page_view','initiate_checkout_rate','total_button_click_value','total_on_web_order_value','cost_per_user_registration',
            'page_content_view_events','value_per_web_event_add_to_cart','cost_per_product_details_page_browse','complete_payment','add_billing','total_product_details_page_browse_value','page_event_search_rate','user_registration_rate']] = data[['value_per_initiate_checkout','page_browse_view','value_per_download_start','total_online_consult_value','total_page_view_content_events_value','total_user_registration_value','total_on_web_subscribe_value',
            'total_web_event_add_to_cart_value','cost_per_page_browse_view','page_browse_view_rate','cost_per_pageview','on_web_add_to_wishlist','download_start_rate','page_content_view_event_rate','value_per_page_browse_view','cost_per_on_web_order','form','avg_value_per_pageview','total_value_per_pageview','on_web_subscribe','cost_per_online_consult','complete_payment_roas','total_form_value','total_page_browse_view_value','form_rate','total_page_event_search_value',
            'download_start','on_web_order_rate','value_per_page_content_view_event','value_per_on_web_add_to_wishlist','cost_per_web_event_add_to_cart','cost_per_on_web_add_to_wishlist','value_per_on_web_order','total_pageview','cost_per_complete_payment','total_download_start_value','total_complete_payment_rate','cost_per_on_web_subscribe','web_event_add_to_cart','value_per_product_details_page_browse','cost_per_page_event_search','value_per_on_web_subscribe',
            'cost_per_download_start','complete_payment_rate','button_click_rate','custom_page_events_value','cost_per_initiate_checkout','value_per_complete_payment','cost_per_custom_page_event','custom_page_event_rate','button_click','product_details_page_browse_rate','total_add_billing_value','on_web_add_to_wishlist_per_click','value_per_add_billing','product_details_page_browse','user_registration','cost_per_page_content_view_event','pageview_rate',
            'total_on_web_add_to_wishlist_value','web_event_add_to_cart_rate','value_per_form','custom_page_events','total_initiate_checkout_value','value_per_button_click','online_consult_rate','value_per_custom_page_event','on_web_order','landing_page_view_rate','initiate_checkout','value_per_online_consult','cost_per_button_click','value_per_page_event_search','cost_per_form','on_web_subscribe_per_click','online_consult','page_event_search','value_per_user_registration',
            'add_billing_rate','cost_per_add_billing','cost_per_landing_page_view','total_landing_page_view','initiate_checkout_rate','total_button_click_value','total_on_web_order_value','cost_per_user_registration','page_content_view_events','value_per_web_event_add_to_cart','cost_per_product_details_page_browse','complete_payment','add_billing','total_product_details_page_browse_value','page_event_search_rate','user_registration_rate']].astype(float)
        return data
    
    def convert_shopads_data(data):
        data['stat_time_day'] = pd.to_datetime(data['stat_time_day'])
        data[['total_onsite_on_web_detail_value','total_onsite_initiate_checkout_count_value','onsite_shopping_rate','cost_per_onsite_initiate_checkout_count',
            'onsite_shopping_roas','onsite_initiate_checkout_count','onsite_on_web_detail_rate','onsite_initiate_checkout_count_rate','onsite_on_web_cart','onsite_shopping',
            'cost_per_onsite_on_web_cart','value_per_onsite_initiate_checkout_count','value_per_onsite_on_web_cart','cost_per_onsite_on_web_detail','cost_per_onsite_shopping',
            'onsite_on_web_detail','value_per_onsite_on_web_detail','onsite_on_web_cart_rate','total_onsite_on_web_cart_value','total_onsite_shopping_value',
            'value_per_onsite_shopping']] = data[['total_onsite_on_web_detail_value','total_onsite_initiate_checkout_count_value','onsite_shopping_rate','cost_per_onsite_initiate_checkout_count',
            'onsite_shopping_roas','onsite_initiate_checkout_count','onsite_on_web_detail_rate','onsite_initiate_checkout_count_rate','onsite_on_web_cart','onsite_shopping',
            'cost_per_onsite_on_web_cart','value_per_onsite_initiate_checkout_count','value_per_onsite_on_web_cart','cost_per_onsite_on_web_detail','cost_per_onsite_shopping',
            'onsite_on_web_detail','value_per_onsite_on_web_detail','onsite_on_web_cart_rate','total_onsite_on_web_cart_value','total_onsite_shopping_value',
            'value_per_onsite_shopping']].astype(float)
        return data
    
    def flatten_list_of_dicts(lst):
        flattened_list = []
        for d in lst:
            flattened_dict = {}
            for key, value in d.items():
                if isinstance(value, dict):
                    flattened_dict.update(value)
                else:
                    flattened_dict[key] = value
            flattened_list.append(flattened_dict)
        return flattened_list
    
    def get_ad_info_data(advertiser_ids_list, url, headers):
        page = 1 # Pagination
        page_size = 1000  # Number of ads to fetch per request
        all_ads = []
        for advertiser_id in advertiser_ids_list:
            print(f"Advertiser ID: {advertiser_id}", end = ": ")
            while True:
                payload = {
                    "advertiser_id": advertiser_id,
                    "page": page,
                    "page_size": page_size
                }
                response = requests.get(url, headers=headers, json=payload)
                if response.status_code == 200:
                    data = response.json()
                    ads = data.get('data', {}).get('list', [])
                    if not ads:
                        break
                    all_ads.extend(ads)
                    page += 1
                else:
                    print(f"Error: {response.status_code} - {response.text}")
                    break
            page = 1
        return all_ads
    
    def transform_ad_info_data(all_ads):
        all_ads_df = pd.DataFrame(all_ads)
        all_ads_df[['tracking_pixel_id','brand_safety_vast_url', 'ad_texts', 'creative_type', 'page_id', 'viewability_vast_url', 
                'music_id', 'landing_page_urls', 'carousel_image_labels','call_to_action_id', 'card_id', 
                'impression_tracking_url', 'click_tracking_url']] = all_ads_df[['tracking_pixel_id','brand_safety_vast_url', 
                                                                                'ad_texts', 'creative_type', 'page_id', 'viewability_vast_url', 
                                                                                'music_id', 'landing_page_urls', 'carousel_image_labels',
                                                                                'call_to_action_id', 'card_id', 'impression_tracking_url', 
                                                                                'click_tracking_url']].astype(str)
        return all_ads_df
    
    def get_target_video_list(client, advertiser_id):
        project_id = "ydmdashboard"
        dataset_id = "media_data_google"
        table = "media_tiktok_ad_info"
        target_table = "media_tiktok_video_ad_info"
        
        source_query = f"""
        SELECT DISTINCT video_id
        FROM `{project_id}.{dataset_id}.{table}`
        WHERE advertiser_id = '{advertiser_id}' AND video_id IS NOT NULL
        """
        target_query = f"""
        SELECT DISTINCT
        video_id
        FROM `{project_id}.{dataset_id}.{target_table}`
        """
        video_id = client.query(source_query).to_dataframe()
        to_delete_video_id = client.query(target_query).to_dataframe()
        video_id_list = video_id['video_id'].to_list()
        to_delete_video_id_list = to_delete_video_id['video_id'].to_list()
        video_id_list_target = [item for item in video_id_list  if item not in to_delete_video_id_list]
        return video_id_list_target
    
    def get_all_video(video_id_list_target,advertiser_id,access_token):
        all_video_df = pd.DataFrame()
        url = 'https://business-api.tiktok.com/open_api/v1.3/file/video/ad/info/'
        headers = {
            'Access-Token': access_token,
            'Content-Type': 'application/json',
        }   
        if len(video_id_list_target) > 60:
            start_index = 0
            end_index = start_index + 60
            for i in range(math.ceil(len(video_id_list_target) / 60)):
                video_id_list = video_id_list_target[start_index:end_index]
                payload = {
                    "advertiser_id" : advertiser_id,
                    "video_ids" : video_id_list 
                }
                start_index += 60
                if end_index + 60 > len(video_id_list_target):
                    end_index += 60
                else:
                    end_index = len(video_id_list_target)
                response = requests.get(url, headers=headers, json=payload)
                video_detail = pd.DataFrame(response.json()['data']['list'])
                all_video_df = pd.concat([all_video_df,video_detail], ignore_index=True)
        else:
            payload = {
                    "advertiser_id" : advertiser_id,
                    "video_ids" : video_id_list_target 
                }
            response = requests.get(url, headers=headers, json=payload)
            video_detail = pd.DataFrame(response.json()['data']['list'])
            all_video_df = pd.concat([all_video_df,video_detail], ignore_index=True)
        
        return all_video_df
    
    def get_spark_ads(access_token, advertiser_ids_list):
        page = 1
        all_video_list = []
        headers = {
            'Access-Token': access_token,
            'Content-Type': 'application/json',
        }

        for advertiser_id in advertiser_ids_list:
            while True:
                url = f'https://business-api.tiktok.com/open_api/v1.3/tt_video/list/?advertiser_id={advertiser_id}&page={page}'
                response = requests.get(url, headers=headers)

                if response.status_code == 200:
                    data = response.json()
                    tt_video = data.get('data', {}).get('list', [])
                    if not tt_video:
                        break
                    all_video_list.extend(tt_video)
                    page += 1
                else:
                    print(f"Error: {response.status_code} - {response.text}")
                    break
            page = 1
        return all_video_list