import os
import json
from google.oauth2 import service_account
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

class GG_Connector:
    def get_basicstat_query(start_date,end_date):
        query = f"""
            SELECT
                ad_group_ad.ad.id,
                ad_group_ad.ad.name,
                ad_group_ad.ad_group,
                ad_group.id,
                ad_group.name,
                ad_group.base_ad_group,
                campaign.id,
                campaign.name,
                campaign.base_campaign,
                campaign.advertising_channel_type,
                customer.id,
                customer.descriptive_name,
                segments.ad_network_type,
                segments.date,
                segments.slot,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                metrics.all_conversions,
                metrics.conversions_value,
                metrics.interaction_event_types,
                metrics.interactions,
                metrics.view_through_conversions
            FROM
                ad_group_ad
            WHERE
                segments.date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY
                segments.date
        """
        return query
    
    def get_adgroup_basicstat_query(start_date,end_date):
        query = f"""
            SELECT
                ad_group.id,
                ad_group.name,
                ad_group.base_ad_group,
                campaign.id,
                campaign.name,
                campaign.base_campaign,
                campaign.advertising_channel_type,
                customer.id,
                customer.descriptive_name,
                segments.ad_network_type,
                segments.date,
                segments.slot,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                metrics.conversions_value,
                metrics.interaction_event_types,
                metrics.interactions,
                metrics.view_through_conversions
            FROM
                ad_group
            WHERE
                segments.date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY
                segments.date
        """
        return query
    
    
    def get_campaign_basicstat_query(start_date,end_date):
        query = f"""
            SELECT
                campaign.id,
                campaign.name,
                campaign.advertising_channel_type,
                campaign.base_campaign,
                customer.id,
                customer.descriptive_name,
                segments.ad_network_type,
                segments.date,
                segments.slot,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                metrics.all_conversions,
                metrics.conversions_value,
                metrics.interaction_event_types,
                metrics.interactions,
                metrics.view_through_conversions
            FROM
                campaign
            WHERE
                segments.date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY
                segments.date
        """
        return query
    
    def get_keyword_basicstat_query(start_date,end_date):
        query = f"""
            SELECT
                ad_group_criterion.criterion_id,
                ad_group_criterion.keyword.text,
                ad_group_criterion.keyword.match_type,
                ad_group.id,
                ad_group.name,
                ad_group.base_ad_group,
                campaign.id,
                campaign.name,
                campaign.base_campaign,
                campaign.advertising_channel_type,
                customer.id,
                customer.descriptive_name,
                segments.ad_network_type,
                segments.date,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                metrics.all_conversions,
                metrics.conversions_value,
                metrics.interaction_event_types,
                metrics.interactions,
                metrics.view_through_conversions
            FROM
                keyword_view
            WHERE
                segments.date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY
                segments.date
        """
        return query
    
    def get_video_basicstat_query(start_date,end_date):
        query = f"""
            SELECT
                ad_group_ad.ad.id,
                ad_group_ad.ad.name,
                ad_group.id,
                ad_group.name,
                campaign.id,
                campaign.name,
                campaign.advertising_channel_type,
                customer.id,
                customer.descriptive_name,
                video.channel_id,
                video.id,
                video.resource_name,
                video.title,
                ad_group_ad.status,
                segments.ad_network_type,
                segments.date,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                metrics.conversions_value,
                metrics.view_through_conversions
            FROM
                video
            WHERE
                segments.date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY
                segments.date
        """
        return query
    
    def get_video_conversion_stat_query(start_date,end_date):
        query = f"""
            SELECT
                ad_group_ad.ad.id,
                ad_group_ad.ad.name,
                ad_group.id,
                ad_group.name,
                campaign.id,
                campaign.name,
                campaign.advertising_channel_type,
                customer.id,
                customer.descriptive_name,
                video.channel_id,
                video.id,
                video.resource_name,
                video.title,
                ad_group_ad.status,
                segments.ad_network_type,
                segments.click_type,
                segments.conversion_action,
                segments.conversion_action_category,
                segments.conversion_action_name,
                segments.date,
                segments.day_of_week,
                segments.month,
                segments.quarter,
                segments.week,
                segments.year,
                metrics.conversions,
                metrics.conversions_value
            FROM
                video
            WHERE
                segments.date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY
                segments.date
        """
        return query
        
    def get_video_nonclickstat_query(start_date,end_date):
        query = f"""
            SELECT
                ad_group_ad.ad.id,
                ad_group_ad.ad.name,
                ad_group.id,
                ad_group.name,
                campaign.id,
                campaign.name,
                campaign.advertising_channel_type,
                customer.id,
                customer.descriptive_name,
                video.channel_id,
                video.id,
                video.resource_name,
                video.title,
                ad_group_ad.status,
                segments.ad_network_type,
                segments.date,
                segments.day_of_week,
                segments.month,
                segments.quarter,
                segments.week,
                segments.year,
                metrics.all_conversions,
                metrics.all_conversions_from_interactions_rate,
                metrics.all_conversions_value,
                metrics.average_cpv,
                metrics.cost_per_all_conversions,
                metrics.cross_device_conversions,
                metrics.engagement_rate,
                metrics.engagements,
                metrics.value_per_all_conversions,
                metrics.video_quartile_p100_rate,
                metrics.video_quartile_p25_rate,
                metrics.video_quartile_p50_rate,
                metrics.video_quartile_p75_rate,
                metrics.video_view_rate,
                metrics.video_views
            FROM
                video
            WHERE
                segments.date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY
                segments.date
        """
        return query
    
    def get_ad_cross_device_conversion_stat_query(start_date,end_date):
        query = f"""
            SELECT
                ad_group_ad.ad.id,
                ad_group_ad.ad.name,
                ad_group_ad.ad_group,
                ad_group.id,
                ad_group.name,
                ad_group.base_ad_group,
                campaign.id,
                campaign.name,
                campaign.base_campaign,
                campaign.advertising_channel_type,
                customer.id,
                customer.descriptive_name,
                segments.ad_network_type,
                segments.conversion_action,
                segments.conversion_action_category,
                segments.conversion_action_name,
                segments.click_type,
                segments.date,
                segments.day_of_week,
                segments.month,
                segments.quarter,
                segments.week,
                segments.year,
                metrics.all_conversions,
                metrics.all_conversions_value,
                metrics.cross_device_conversions,
                metrics.value_per_all_conversions
            FROM
                ad_group_ad
            WHERE
                segments.date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY
                segments.date
        """
        return query
    
    def get_ad_group_cross_device_conversion_stat_query(start_date,end_date):
        query = f"""
            SELECT
                ad_group.id,
                ad_group.name,
                ad_group.base_ad_group,
                campaign.id,
                campaign.name,
                campaign.base_campaign,
                campaign.advertising_channel_type,
                customer.id,
                customer.descriptive_name,
                segments.ad_network_type,
                segments.conversion_action,
                segments.conversion_action_category,
                segments.conversion_action_name,
                segments.click_type,
                segments.date,
                segments.day_of_week,
                segments.month,
                segments.quarter,
                segments.week,
                segments.year,
                metrics.all_conversions,
                metrics.all_conversions_value,
                metrics.cross_device_conversions,
                metrics.value_per_all_conversions
            FROM
                ad_group
            WHERE
                segments.date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY
                segments.date
        """
        return query
    
    def get_campaign_cross_device_conversion_stat_query(start_date,end_date):
        query = f"""
            SELECT
                campaign.id,
                campaign.name,
                campaign.base_campaign,
                campaign.advertising_channel_type,
                customer.id,
                customer.descriptive_name,
                segments.ad_network_type,
                segments.conversion_action,
                segments.conversion_action_category,
                segments.conversion_action_name,
                segments.conversion_attribution_event_type,
                segments.date,
                segments.day_of_week,
                segments.month,
                segments.quarter,
                segments.week,
                segments.year,
                metrics.all_conversions,
                metrics.all_conversions_value,
                metrics.cross_device_conversions,
                metrics.value_per_all_conversions
            FROM
                campaign
            WHERE
                segments.date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY
                segments.date
        """
        return query
    
    def get_keyword_cross_device_conversion_stat_query(start_date,end_date):
        query = f"""
            SELECT
                ad_group_criterion.criterion_id,
                ad_group_criterion.keyword.match_type,
                ad_group_criterion.keyword.text,
                ad_group.id,
                ad_group.name,
                ad_group.base_ad_group,
                campaign.id,
                campaign.name,
                campaign.base_campaign,
                campaign.advertising_channel_type,
                customer.id,
                customer.descriptive_name,
                segments.ad_network_type,
                segments.conversion_action,
                segments.conversion_action_category,
                segments.conversion_action_name,
                segments.date,
                segments.day_of_week,
                segments.month,
                segments.quarter,
                segments.week,
                segments.year,
                metrics.all_conversions,
                metrics.all_conversions_value,
                metrics.cross_device_conversions,
                metrics.value_per_all_conversions
            FROM
                keyword_view
            WHERE
                segments.date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY
                segments.date
        """
        return query
    
    def get_adgroup_label_query():
        query = f"""
            SELECT
                ad_group.name,
                ad_group.id,
                label.id,
                ad_group_label.ad_group,
                ad_group_label.label,
                label.name,
                label.resource_name
            FROM
                ad_group_label
        """
        return query
    
    def get_service_dev_token(customer_id):
        if customer_id == "7498651531":
            service_account_info = json.loads(os.environ.get("hmth-bigquery"))
            developer_token=os.environ.get('GG_DEV_TOKEN')
            login_customer_id = "4993931236"
        else:
            service_account_info = json.loads(os.environ.get("hmth-sem"))
            developer_token=os.environ.get('GG_DEV_TOKEN_SEM')
            login_customer_id = "2685941715"
        return service_account_info, developer_token, login_customer_id
                    
    def get_basicstat_data(customer_ids,query):
        all_data = []
        for customer_id in customer_ids:
            try:
                scopes=['https://www.googleapis.com/auth/adwords']
                service_account_info, developer_token,login_customer_id = GG_Connector.get_service_dev_token(customer_id) 
                credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=scopes)

                # Initialize the GoogleAdsClient with the credentials
                client = GoogleAdsClient(credentials=credentials, developer_token=developer_token,login_customer_id=login_customer_id)
                # Execute the query for the current customer ID
                ga_service = client.get_service("GoogleAdsService")
                response = ga_service.search(customer_id=customer_id, query=query)
                
                # Extract data from the response and append to all_data
                for row in response:
                    all_data.append({
                        "ad_group_ad_ad_id": row.ad_group_ad.ad.id,
                        "ad_group_ad_ad_name": row.ad_group_ad.ad.name,
                        "ad_group_id": row.ad_group.id,
                        "ad_group_name": row.ad_group.name,
                        "campaign_id": row.campaign.id,
                        "campaign_name": row.campaign.name,
                        "customer_id": row.customer.id,
                        "campaign_advertising_channel_type": row.campaign.advertising_channel_type,
                        "customer_descriptive_name": row.customer.descriptive_name,
                        "ad_group_ad_ad_group": row.ad_group_ad.ad_group,
                        "ad_group_base_ad_group": row.ad_group.base_ad_group,
                        "campaign_base_campaign": row.campaign.base_campaign,
                        "metrics_clicks": row.metrics.clicks,
                        "metrics_conversions": row.metrics.conversions,
                        "metrics_conversions_value": row.metrics.conversions_value,
                        "metrics_cost_micros": row.metrics.cost_micros,
                        "metrics_impressions": row.metrics.impressions,
                        "metrics_interaction_event_types": row.metrics.interaction_event_types,
                        "metrics_interactions": row.metrics.interactions,
                        "metrics_view_through_conversions": row.metrics.view_through_conversions,
                        "segments_ad_network_type": row.segments.ad_network_type,
                        "segments_date": row.segments.date,
                        "segments_slot": row.segments.slot,
                    })

            except GoogleAdsException as ex:
                print(f"Request failed for customer ID {customer_id} with error: {ex}")
                for error in ex.failure.errors:
                    print(f"Error message: {error.message}")
        return all_data
    
    
    def get_adgroup_basicstat_data(customer_ids,query):
        all_data = []
        for customer_id in customer_ids:
            try:
                scopes=['https://www.googleapis.com/auth/adwords']
                service_account_info, developer_token,login_customer_id = GG_Connector.get_service_dev_token(customer_id) 
                credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=scopes)

                # Initialize the GoogleAdsClient with the credentials
                client = GoogleAdsClient(credentials=credentials, developer_token=developer_token,login_customer_id=login_customer_id)
                
                # Execute the query for the current customer ID
                ga_service = client.get_service("GoogleAdsService")
                response = ga_service.search(customer_id=customer_id, query=query)
                
                # Extract data from the response and append to all_data
                for row in response:
                    all_data.append({
                        "ad_group_id": row.ad_group.id,
                        "ad_group_name": row.ad_group.name,
                        "campaign_id": row.campaign.id,
                        "campaign_name": row.campaign.name,
                        "customer_id": row.customer.id,
                        "campaign_advertising_channel_type": row.campaign.advertising_channel_type,
                        "customer_descriptive_name": row.customer.descriptive_name,
                        "ad_group_ad_ad_group": row.ad_group_ad.ad_group,
                        "ad_group_base_ad_group": row.ad_group.base_ad_group,
                        "campaign_base_campaign": row.campaign.base_campaign,
                        "metrics_clicks": row.metrics.clicks,
                        "metrics_conversions": row.metrics.conversions,
                        "metrics_conversions_value": row.metrics.conversions_value,
                        "metrics_cost_micros": row.metrics.cost_micros,
                        "metrics_impressions": row.metrics.impressions,
                        "metrics_interaction_event_types": row.metrics.interaction_event_types,
                        "metrics_interactions": row.metrics.interactions,
                        "metrics_view_through_conversions": row.metrics.view_through_conversions,
                        "segments_ad_network_type": row.segments.ad_network_type,
                        "segments_date": row.segments.date,
                        "segments_slot": row.segments.slot,
                    })

            except GoogleAdsException as ex:
                print(f"Request failed for customer ID {customer_id} with error: {ex}")
                for error in ex.failure.errors:
                    print(f"Error message: {error.message}")
        return all_data
    
    def get_campaign_basicstat_data(customer_ids,query):
        all_data = []
        for customer_id in customer_ids:
            try:
                scopes=['https://www.googleapis.com/auth/adwords']
                service_account_info, developer_token,login_customer_id = GG_Connector.get_service_dev_token(customer_id) 
                credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=scopes)

                # Initialize the GoogleAdsClient with the credentials
                client = GoogleAdsClient(credentials=credentials, developer_token=developer_token,login_customer_id=login_customer_id)
                
                # Execute the query for the current customer ID
                ga_service = client.get_service("GoogleAdsService")
                response = ga_service.search(customer_id=customer_id, query=query)
                
                # Extract data from the response and append to all_data
                for row in response:
                    all_data.append({
                        "campaign_id": row.campaign.id,
                        "campaign_name": row.campaign.name,
                        "customer_id": row.customer.id,
                        "customer_descriptive_name": row.customer.descriptive_name,
                        "campaign_base_campaign": row.campaign.base_campaign,
                        "campaign_advertising_channel_type": row.campaign.advertising_channel_type,
                        "metrics_clicks": row.metrics.clicks,
                        "metrics_conversions": row.metrics.conversions,
                        "metrics_conversions_value": row.metrics.conversions_value,
                        "metrics_cost_micros": row.metrics.cost_micros,
                        "metrics_impressions": row.metrics.impressions,
                        "metrics_interaction_event_types": row.metrics.interaction_event_types,
                        "metrics_interactions": row.metrics.interactions,
                        "metrics_view_through_conversions": row.metrics.view_through_conversions,
                        "segments_ad_network_type": row.segments.ad_network_type,
                        "segments_date": row.segments.date,
                        "segments_slot": row.segments.slot,
                    })

            except GoogleAdsException as ex:
                print(f"Request failed for customer ID {customer_id} with error: {ex}")
                for error in ex.failure.errors:
                    print(f"Error message: {error.message}")
        return all_data
    
    def get_keyword_basicstat_data(customer_ids,query):
        all_data = []
        for customer_id in customer_ids:
            try:
                scopes=['https://www.googleapis.com/auth/adwords']
                service_account_info, developer_token,login_customer_id = GG_Connector.get_service_dev_token(customer_id) 
                credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=scopes)

                # Initialize the GoogleAdsClient with the credentials
                client = GoogleAdsClient(credentials=credentials, developer_token=developer_token,login_customer_id=login_customer_id)
                # Execute the query for the current customer ID
                ga_service = client.get_service("GoogleAdsService")
                response = ga_service.search(customer_id=customer_id, query=query)
                
                # Extract data from the response and append to all_data
                for row in response:
                    all_data.append({
                        "ad_group_criterion_criterion_id": row.ad_group_criterion.criterion_id,
                        "ad_group_criterion_keyword_match_type": row.ad_group_criterion.keyword.match_type,
                        "ad_group_criterion_keyword_text": row.ad_group_criterion.keyword.text,
                        "ad_group_id": row.ad_group.id,
                        "ad_group_name": row.ad_group.name,
                        "ad_group_base_ad_group": row.ad_group.base_ad_group,
                        "campaign_id": row.campaign.id,
                        "campaign_name": row.campaign.name,
                        "customer_id": row.customer.id,
                        "customer_descriptive_name": row.customer.descriptive_name,
                        "campaign_base_campaign": row.campaign.base_campaign,
                        "campaign_advertising_channel_type": row.campaign.advertising_channel_type,
                        "metrics_clicks": row.metrics.clicks,
                        "metrics_conversions": row.metrics.conversions,
                        "metrics_conversions_value": row.metrics.conversions_value,
                        "metrics_cost_micros": row.metrics.cost_micros,
                        "metrics_impressions": row.metrics.impressions,
                        "metrics_interaction_event_types": row.metrics.interaction_event_types,
                        "metrics_interactions": row.metrics.interactions,
                        "metrics_view_through_conversions": row.metrics.view_through_conversions,
                        "segments_ad_network_type": row.segments.ad_network_type,
                        "segments_date": row.segments.date
                    })

            except GoogleAdsException as ex:
                print(f"Request failed for customer ID {customer_id} with error: {ex}")
                for error in ex.failure.errors:
                    print(f"Error message: {error.message}")
        return all_data
    
    def get_video_basicstat_data(customer_ids,query):
        all_data = []
        for customer_id in customer_ids:
            try:
                scopes=['https://www.googleapis.com/auth/adwords']
                service_account_info, developer_token,login_customer_id = GG_Connector.get_service_dev_token(customer_id) 
                credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=scopes)

                # Initialize the GoogleAdsClient with the credentials
                client = GoogleAdsClient(credentials=credentials, developer_token=developer_token,login_customer_id=login_customer_id)
                
                # Execute the query for the current customer ID
                ga_service = client.get_service("GoogleAdsService")
                response = ga_service.search(customer_id=customer_id, query=query)
                
                # Extract data from the response and append to all_data
                for row in response:
                    all_data.append({
                        "ad_group_ad_ad_id": row.ad_group_ad.ad.id,
                        "ad_group_ad_ad_name": row.ad_group_ad.ad.name,
                        "ad_group_id": row.ad_group.id,
                        "ad_group_name": row.ad_group.name,
                        "campaign_id": row.campaign.id,
                        "campaign_name": row.campaign.name,
                        "campaign_advertising_channel_type": row.campaign.advertising_channel_type,
                        "customer_id": row.customer.id,
                        "customer_descriptive_name": row.customer.descriptive_name,
                        "video_channel_id": row.video.channel_id,
                        "video_id": row.video.id,
                        "video_resource_name": row.video.resource_name,
                        "video_title": row.video.title,
                        "ad_group_ad_status": row.ad_group_ad.status,
                        "metrics_clicks": row.metrics.clicks,
                        "metrics_conversions": row.metrics.conversions,
                        "metrics_conversions_value": row.metrics.conversions_value,
                        "metrics_cost_micros": row.metrics.cost_micros,
                        "metrics_impressions": row.metrics.impressions,
                        "metrics_view_through_conversions": row.metrics.view_through_conversions,
                        "segments_ad_network_type": row.segments.ad_network_type,
                        "segments_date": row.segments.date
                    })

            except GoogleAdsException as ex:
                print(f"Request failed for customer ID {customer_id} with error: {ex}")
                for error in ex.failure.errors:
                    print(f"Error message: {error.message}")
        return all_data
    
    def get_video_conversionstat_data(customer_ids,query):
        all_data = []
        for customer_id in customer_ids:
            try:
                scopes=['https://www.googleapis.com/auth/adwords']
                service_account_info, developer_token,login_customer_id = GG_Connector.get_service_dev_token(customer_id) 
                credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=scopes)

                # Initialize the GoogleAdsClient with the credentials
                client = GoogleAdsClient(credentials=credentials, developer_token=developer_token,login_customer_id=login_customer_id)
                
                # Execute the query for the current customer ID
                ga_service = client.get_service("GoogleAdsService")
                response = ga_service.search(customer_id=customer_id, query=query)
                
                # Extract data from the response and append to all_data
                for row in response:
                    all_data.append({
                        "ad_group_ad_ad_id": row.ad_group_ad.ad.id,
                        "ad_group_ad_ad_name": row.ad_group_ad.ad.name,
                        "ad_group_id": row.ad_group.id,
                        "ad_group_name": row.ad_group.name,
                        "campaign_id": row.campaign.id,
                        "campaign_name": row.campaign.name,
                        "campaign_advertising_channel_type": row.campaign.advertising_channel_type,
                        "customer_id": row.customer.id,
                        "customer_descriptive_name": row.customer.descriptive_name,
                        "video_channel_id": row.video.channel_id,
                        "video_id": row.video.id,
                        "video_resource_name": row.video.resource_name,
                        "video_title": row.video.title,
                        "ad_group_ad_status": row.ad_group_ad.status,
                        "metrics_conversions": row.metrics.conversions,
                        "metrics_conversions_value": row.metrics.conversions_value,
                        "segments_conversion_action": row.segments.conversion_action,
                        "segments_conversion_action_category": row.segments.conversion_action_category,
                        "segments_conversion_action_name": row.segments.conversion_action_name,
                        "segments_ad_network_type": row.segments.ad_network_type,
                        "segments_date": row.segments.date,
                        "segments_day_of_week": row.segments.day_of_week,
                        "segments_month": row.segments.month,
                        "segments_quarter": row.segments.quarter,
                        "segments_week": row.segments.week,
                        "segments_year": row.segments.year
                    })

            except GoogleAdsException as ex:
                print(f"Request failed for customer ID {customer_id} with error: {ex}")
                for error in ex.failure.errors:
                    print(f"Error message: {error.message}")            
        return all_data
    
    
    def get_video_nonclickstat_data(customer_ids,query):
        all_data = []
        for customer_id in customer_ids:
            try:
                scopes=['https://www.googleapis.com/auth/adwords']
                service_account_info, developer_token,login_customer_id = GG_Connector.get_service_dev_token(customer_id) 
                credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=scopes)

                # Initialize the GoogleAdsClient with the credentials
                client = GoogleAdsClient(credentials=credentials, developer_token=developer_token,login_customer_id=login_customer_id)
                
                # Execute the query for the current customer ID
                ga_service = client.get_service("GoogleAdsService")
                response = ga_service.search(customer_id=customer_id, query=query)
                
                # Extract data from the response and append to all_data
                for row in response:
                    all_data.append({
                        "ad_group_ad_ad_id": row.ad_group_ad.ad.id,
                        "ad_group_ad_ad_name": row.ad_group_ad.ad.name,
                        "ad_group_id": row.ad_group.id,
                        "ad_group_name": row.ad_group.name,
                        "campaign_id": row.campaign.id,
                        "campaign_name": row.campaign.name,
                        "campaign_advertising_channel_type": row.campaign.advertising_channel_type,
                        "customer_id": row.customer.id,
                        "customer_descriptive_name": row.customer.descriptive_name,
                        "video_channel_id": row.video.channel_id,
                        "video_id": row.video.id,
                        "video_resource_name": row.video.resource_name,
                        "video_title": row.video.title,
                        "ad_group_ad_status": row.ad_group_ad.status,
                        "metrics_all_conversions": row.metrics.all_conversions,
                        "metrics_all_conversions_from_interactions_rate": row.metrics.all_conversions_from_interactions_rate,
                        "metrics_all_conversions_value": row.metrics.all_conversions_value,
                        "metrics_average_cpv": row.metrics.average_cpv,
                        "metrics_cost_per_all_conversions": row.metrics.cost_per_all_conversions,
                        "metrics_cross_device_conversions": row.metrics.cross_device_conversions,
                        "metrics_engagement_rate": row.metrics.engagement_rate,
                        "metrics_engagements": row.metrics.engagements,
                        "metrics_value_per_all_conversions": row.metrics.value_per_all_conversions,
                        "metrics_video_quartile_p100_rate": row.metrics.video_quartile_p100_rate,
                        "metrics_video_quartile_p75_rate": row.metrics.video_quartile_p75_rate,
                        "metrics_video_quartile_p50_rate": row.metrics.video_quartile_p50_rate,
                        "metrics_video_quartile_p25_rate": row.metrics.video_quartile_p25_rate,
                        "metrics_video_view_rate": row.metrics.video_view_rate,
                        "metrics_video_views": row.metrics.video_views,
                        "segments_ad_network_type": row.segments.ad_network_type,
                        "segments_date": row.segments.date,
                        "segments_day_of_week": row.segments.day_of_week,
                        "segments_month": row.segments.month,
                        "segments_quarter": row.segments.quarter,
                        "segments_week": row.segments.week,
                        "segments_year": row.segments.year
                    })

            except GoogleAdsException as ex:
                print(f"Request failed for customer ID {customer_id} with error: {ex}")
                for error in ex.failure.errors:
                    print(f"Error message: {error.message}")
        return all_data
    
    
    def get_ad_cross_device_conversion_stat_data(customer_ids,query):
        all_data = []
        for customer_id in customer_ids:
            try:
                scopes=['https://www.googleapis.com/auth/adwords']
                service_account_info, developer_token,login_customer_id = GG_Connector.get_service_dev_token(customer_id) 
                credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=scopes)

                # Initialize the GoogleAdsClient with the credentials
                client = GoogleAdsClient(credentials=credentials, developer_token=developer_token,login_customer_id=login_customer_id)
                
                # Execute the query for the current customer ID
                ga_service = client.get_service("GoogleAdsService")
                response = ga_service.search(customer_id=customer_id, query=query)
                
                # Extract data from the response and append to all_data
                for row in response:
                    all_data.append({
                        "ad_group_ad_ad_id": row.ad_group_ad.ad.id,
                        "ad_group_ad_ad_name": row.ad_group_ad.ad.name,
                        "ad_group_id": row.ad_group.id,
                        "ad_group_name": row.ad_group.name,
                        "campaign_id": row.campaign.id,
                        "campaign_name": row.campaign.name,
                        "customer_id": row.customer.id,
                        "campaign_advertising_channel_type": row.campaign.advertising_channel_type,
                        "customer_descriptive_name": row.customer.descriptive_name,
                        "ad_group_ad_ad_group": row.ad_group_ad.ad_group,
                        "ad_group_base_ad_group": row.ad_group.base_ad_group,
                        "campaign_base_campaign": row.campaign.base_campaign,
                        "metrics_all_conversions": row.metrics.all_conversions,
                        "metrics_all_conversions_value": row.metrics.all_conversions_value,
                        "metrics_cross_device_conversions": row.metrics.cross_device_conversions,
                        "metrics_value_per_all_conversions": row.metrics.value_per_all_conversions,
                        "segments_ad_network_type": row.segments.ad_network_type,
                        "segments_date": row.segments.date,
                        "segments_conversion_action": row.segments.conversion_action,
                        "segments_conversion_action_category": row.segments.conversion_action_category,
                        "segments_conversion_action_name": row.segments.conversion_action_name,
                        "segments_click_type": row.segments.click_type,
                        "segments_day_of_week": row.segments.day_of_week,
                        "segments_month": row.segments.month,
                        "segments_quarter": row.segments.quarter,
                        "segments_week": row.segments.week,
                        "segments_year": row.segments.year
                    })

            except GoogleAdsException as ex:
                print(f"Request failed for customer ID {customer_id} with error: {ex}")
                for error in ex.failure.errors:
                    print(f"Error message: {error.message}")
        return all_data
    
    def get_ad_group_cross_device_conversion_stat_data(customer_ids,query):
        all_data = []
        for customer_id in customer_ids:
            try:
                scopes=['https://www.googleapis.com/auth/adwords']
                service_account_info, developer_token,login_customer_id = GG_Connector.get_service_dev_token(customer_id) 
                credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=scopes)

                # Initialize the GoogleAdsClient with the credentials
                client = GoogleAdsClient(credentials=credentials, developer_token=developer_token,login_customer_id=login_customer_id)
                
                # Execute the query for the current customer ID
                ga_service = client.get_service("GoogleAdsService")
                response = ga_service.search(customer_id=customer_id, query=query)
                
                # Extract data from the response and append to all_data
                for row in response:
                    all_data.append({
                        "ad_group_id": row.ad_group.id,
                        "ad_group_name": row.ad_group.name,
                        "campaign_id": row.campaign.id,
                        "campaign_name": row.campaign.name,
                        "customer_id": row.customer.id,
                        "campaign_advertising_channel_type": row.campaign.advertising_channel_type,
                        "customer_descriptive_name": row.customer.descriptive_name,
                        "ad_group_base_ad_group": row.ad_group.base_ad_group,
                        "campaign_base_campaign": row.campaign.base_campaign,
                        "metrics_all_conversions": row.metrics.all_conversions,
                        "metrics_all_conversions_value": row.metrics.all_conversions_value,
                        "metrics_cross_device_conversions": row.metrics.cross_device_conversions,
                        "metrics_value_per_all_conversions": row.metrics.value_per_all_conversions,
                        "segments_ad_network_type": row.segments.ad_network_type,
                        "segments_date": row.segments.date,
                        "segments_conversion_action": row.segments.conversion_action,
                        "segments_conversion_action_category": row.segments.conversion_action_category,
                        "segments_conversion_action_name": row.segments.conversion_action_name,
                        "segments_click_type": row.segments.click_type,
                        "segments_day_of_week": row.segments.day_of_week,
                        "segments_month": row.segments.month,
                        "segments_quarter": row.segments.quarter,
                        "segments_week": row.segments.week,
                        "segments_year": row.segments.year
                    })

            except GoogleAdsException as ex:
                print(f"Request failed for customer ID {customer_id} with error: {ex}")
                for error in ex.failure.errors:
                    print(f"Error message: {error.message}")
        return all_data
    
    def get_campaign_cross_device_conversion_stat_data(customer_ids,query):
        all_data = []
        for customer_id in customer_ids:
            try:
                scopes=['https://www.googleapis.com/auth/adwords']
                service_account_info, developer_token,login_customer_id = GG_Connector.get_service_dev_token(customer_id) 
                credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=scopes)

                # Initialize the GoogleAdsClient with the credentials
                client = GoogleAdsClient(credentials=credentials, developer_token=developer_token,login_customer_id=login_customer_id)
                
                # Execute the query for the current customer ID
                ga_service = client.get_service("GoogleAdsService")
                response = ga_service.search(customer_id=customer_id, query=query)
                
                # Extract data from the response and append to all_data
                for row in response:
                    all_data.append({
                        "campaign_id": row.campaign.id,
                        "campaign_name": row.campaign.name,
                        "customer_id": row.customer.id,
                        "campaign_advertising_channel_type": row.campaign.advertising_channel_type,
                        "customer_descriptive_name": row.customer.descriptive_name,
                        "campaign_base_campaign": row.campaign.base_campaign,
                        "metrics_all_conversions": row.metrics.all_conversions,
                        "metrics_all_conversions_value": row.metrics.all_conversions_value,
                        "metrics_cross_device_conversions": row.metrics.cross_device_conversions,
                        "metrics_value_per_all_conversions": row.metrics.value_per_all_conversions,
                        "segments_ad_network_type": row.segments.ad_network_type,
                        "segments_date": row.segments.date,
                        "segments_conversion_action": row.segments.conversion_action,
                        "segments_conversion_action_category": row.segments.conversion_action_category,
                        "segments_conversion_action_name": row.segments.conversion_action_name,
                        "segments_conversion_attribution_event_type": row.segments.conversion_attribution_event_type,
                        "segments_day_of_week": row.segments.day_of_week,
                        "segments_month": row.segments.month,
                        "segments_quarter": row.segments.quarter,
                        "segments_week": row.segments.week,
                        "segments_year": row.segments.year
                    })

            except GoogleAdsException as ex:
                print(f"Request failed for customer ID {customer_id} with error: {ex}")
                for error in ex.failure.errors:
                    print(f"Error message: {error.message}")
        return all_data
    
    def get_keyword_cross_device_conversion_stat_data(customer_ids,query):
        all_data = []
        for customer_id in customer_ids:
            try:
                scopes=['https://www.googleapis.com/auth/adwords']
                service_account_info, developer_token,login_customer_id = GG_Connector.get_service_dev_token(customer_id) 
                credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=scopes)

                # Initialize the GoogleAdsClient with the credentials
                client = GoogleAdsClient(credentials=credentials, developer_token=developer_token,login_customer_id=login_customer_id)
                
                # Execute the query for the current customer ID
                ga_service = client.get_service("GoogleAdsService")
                response = ga_service.search(customer_id=customer_id, query=query)
                
                # Extract data from the response and append to all_data
                for row in response:
                    all_data.append({
                        "ad_group_criterion_criterion_id": row.ad_group_criterion.criterion_id,
                        "ad_group_criterion_keyword_match_type": row.ad_group_criterion.keyword.match_type,
                        "ad_group_criterion_keyword_text": row.ad_group_criterion.keyword.text,
                        "ad_group_id": row.ad_group.id,
                        "ad_group_name": row.ad_group.name,
                        "campaign_id": row.campaign.id,
                        "campaign_name": row.campaign.name,
                        "customer_id": row.customer.id,
                        "campaign_advertising_channel_type": row.campaign.advertising_channel_type,
                        "customer_descriptive_name": row.customer.descriptive_name,
                        "ad_group_base_ad_group": row.ad_group.base_ad_group,
                        "campaign_base_campaign": row.campaign.base_campaign,
                        "metrics_all_conversions": row.metrics.all_conversions,
                        "metrics_all_conversions_value": row.metrics.all_conversions_value,
                        "metrics_cross_device_conversions": row.metrics.cross_device_conversions,
                        "metrics_value_per_all_conversions": row.metrics.value_per_all_conversions,
                        "segments_ad_network_type": row.segments.ad_network_type,
                        "segments_date": row.segments.date,
                        "segments_conversion_action": row.segments.conversion_action,
                        "segments_conversion_action_category": row.segments.conversion_action_category,
                        "segments_conversion_action_name": row.segments.conversion_action_name,
                        "segments_day_of_week": row.segments.day_of_week,
                        "segments_month": row.segments.month,
                        "segments_quarter": row.segments.quarter,
                        "segments_week": row.segments.week,
                        "segments_year": row.segments.year
                    })

            except GoogleAdsException as ex:
                print(f"Request failed for customer ID {customer_id} with error: {ex}")
                for error in ex.failure.errors:
                    print(f"Error message: {error.message}")
        return all_data
    
    def get_adgroup_label_data(customer_ids,query):
        all_data = []
        for customer_id in customer_ids:
            try:
                scopes=['https://www.googleapis.com/auth/adwords']
                
                service_account_info, developer_token,login_customer_id = GG_Connector.get_service_dev_token(customer_id) 
                credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=scopes)

                # Initialize the GoogleAdsClient with the credentials
                client = GoogleAdsClient(credentials=credentials, developer_token=developer_token,login_customer_id=login_customer_id)
                
                # Execute the query for the current customer ID
                ga_service = client.get_service("GoogleAdsService")
                response = ga_service.search(customer_id=customer_id, query=query)
                
                # Extract data from the response and append to all_data
                for row in response:
                    all_data.append({
                        "ad_group_name": row.ad_group.name,
                        "ad_group_id": row.ad_group.id,
                        "label_id": row.label.id,
                        "ad_group_label_ad_group": row.ad_group_label.ad_group,
                        "ad_group_label_label": row.ad_group_label.label,
                        "label_name": row.label.name,
                        "ad_group_label_resource_name": row.label.resource_name
                    })

            except GoogleAdsException as ex:
                print(f"Request failed for customer ID {customer_id} with error: {ex}")
                for error in ex.failure.errors:
                    print(f"Error message: {error.message}")
        return all_data
    
    def get_ad_network_type_description(enum_value):
        ad_network_type_mapping = {
            0: "UNSPECIFIED",
            1: "UNKNOWN",
            2: "SEARCH",
            3: "SEARCH_PARTNERS",
            4: "CONTENT",
            7: "MIXED",
            8: "YOUTUBE",
            9: "GOOGLE_TV",
            10: "GOOGLE_OWNED_CHANNELS"
        }
        return ad_network_type_mapping.get(enum_value, "UNKNOWN")

    def get_device_description(enum_value):
        device_mapping = {
            0: "UNSPECIFIED",
            1: "UNKNOWN",
            2: "MOBILE",
            3: "TABLET",
            4: "DESKTOP",
            6: "CONNECTED_TV",
            5: "OTHER",
        }
        return device_mapping.get(enum_value, "UNKNOWN")

    def get_slot_description(enum_value):
        slot_mapping = {
            0: "UNSPECIFIED",
            1: "UNKNOWN",
            2: "SEARCH_SIDE",
            3: "SEARCH_TOP",
            4: "SEARCH_OTHER",
            5: "CONTENT",
            6: "SEARCH_PARTNER_TOP",
            7: "SEARCH_PARTNER_OTHER",
            8: "MIXED",
        }
        return slot_mapping.get(enum_value, "UNKNOWN")
    
    def get_advertising_channel_type(value):
        advertising_channel_types = {
            0: "UNSPECIFIED",
            1: "UNKNOWN",
            2: "SEARCH",
            3: "DISPLAY",
            4: "SHOPPING",
            5: "HOTEL",
            6: "VIDEO",
            7: "MULTI_CHANNEL",
            8: "LOCAL",
            9: "SMART",
            10: "PERFORMANCE_MAX",
            11: "LOCAL_SERVICES",
            13: "TRAVEL",
            14: "DEMAND_GEN",
        }
        return advertising_channel_types.get(value, "UNKNOWN")
    
    def get_keyword_match_type(value):
        keyword_match_types = {
            0: "UNSPECIFIED",
            1: "UNKNOWN",
            2: "EXACT",
            3: "PHRASE",
            4: "BROAD",
        }
        return keyword_match_types.get(value, "INVALID_VALUE")
    
    def get_ad_group_ad_status(value):
        ad_group_ad_statuses = {
            0: "UNSPECIFIED",
            1: "UNKNOWN",
            2: "ENABLED",
            3: "PAUSED",
            4: "REMOVED",
        }
        return ad_group_ad_statuses.get(value, "INVALID_VALUE")
    
    def get_day_of_week(value):
        days_of_week = {
            0: "UNSPECIFIED",
            1: "UNKNOWN",
            2: "MONDAY",
            3: "TUESDAY",
            4: "WEDNESDAY",
            5: "THURSDAY",
            6: "FRIDAY",
            7: "SATURDAY",
            8: "SUNDAY",
        }
        return days_of_week.get(value, "INVALID_VALUE")
    
    def get_conversion_action_category(value):
        """Map a numeric value to its corresponding ConversionActionCategory name."""
        conversion_action_category_map = {
            0: "UNSPECIFIED",
            1: "UNKNOWN",
            2: "DEFAULT",
            3: "PAGE_VIEW",
            4: "PURCHASE",
            5: "SIGNUP",
            7: "DOWNLOAD",
            8: "ADD_TO_CART",
            9: "BEGIN_CHECKOUT",
            10: "SUBSCRIBE_PAID",
            11: "PHONE_CALL_LEAD",
            12: "IMPORTED_LEAD",
            13: "SUBMIT_LEAD_FORM",
            14: "BOOK_APPOINTMENT",
            15: "REQUEST_QUOTE",
            16: "GET_DIRECTIONS",
            17: "OUTBOUND_CLICK",
            18: "CONTACT",
            19: "ENGAGEMENT",
            20: "STORE_VISIT",
            21: "STORE_SALE",
            22: "QUALIFIED_LEAD",
            23: "CONVERTED_LEAD",
        }
        return conversion_action_category_map.get(value, "INVALID_CATEGORY")
    
    def get_conversion_attribution_event_type(value):
        conversion_attribution_event_type_map = {
            0: "UNSPECIFIED",
            1: "UNKNOWN",
            2: "IMPRESSION",
            3: "INTERACTION"
        }
        return conversion_attribution_event_type_map.get(value, "INVALID_EVENT_TYPE")
    
    def preprocess_interaction_event(event):
        return [e.name for e in event]
    
    def interaction_event_mapping():
        value_mapping = {
            0: "UNSPECIFIED",
            1: "UNKNOWN",
            2: "CLICK",
            3: "ENGAGEMENT",
            4: "VIDEO_VIEW",
            5: "NONE"
        }
        return value_mapping
    
    def get_click_type(enum_value):
        click_type_mapping = {
            0: "UNSPECIFIED",
            1: "UNKNOWN",
            2: "APP_DEEPLINK",
            3: "BREADCRUMBS",
            4: "BROADBAND_PLAN",
            5: "CALL_TRACKING",
            6: "CALLS",
            7: "CLICK_ON_ENGAGEMENT_AD",
            8: "GET_DIRECTIONS",
            9: "LOCATION_EXPANSION",
            10: "LOCATION_FORMAT_CALL",
            11: "LOCATION_FORMAT_DIRECTIONS",
            12: "LOCATION_FORMAT_IMAGE",
            13: "LOCATION_FORMAT_LANDING_PAGE",
            14: "LOCATION_FORMAT_MAP",
            15: "LOCATION_FORMAT_STORE_INFO",
            16: "LOCATION_FORMAT_TEXT",
            17: "MOBILE_CALL_TRACKING",
            18: "OFFER_PRINTS",
            19: "OTHER",
            20: "PRODUCT_EXTENSION_CLICKS",
            21: "PRODUCT_LISTING_AD_CLICKS",
            22: "SITELINKS",
            23: "STORE_LOCATOR",
            25: "URL_CLICKS",
            26: "VIDEO_APP_STORE_CLICKS",
            27: "VIDEO_CALL_TO_ACTION_CLICKS",
            28: "VIDEO_CARD_ACTION_HEADLINE_CLICKS",
            29: "VIDEO_END_CAP_CLICKS",
            30: "VIDEO_WEBSITE_CLICKS",
            31: "VISUAL_SITELINKS",
            32: "WIRELESS_PLAN",
            33: "PRODUCT_LISTING_AD_LOCAL",
            34: "PRODUCT_LISTING_AD_MULTICHANNEL_LOCAL",
            35: "PRODUCT_LISTING_AD_MULTICHANNEL_ONLINE",
            36: "PRODUCT_LISTING_ADS_COUPON",
            37: "PRODUCT_LISTING_AD_TRANSACTABLE",
            38: "PRODUCT_AD_APP_DEEPLINK",
            39: "SHOWCASE_AD_CATEGORY_LINK",
            40: "SHOWCASE_AD_LOCAL_STOREFRONT_LINK",
            42: "SHOWCASE_AD_ONLINE_PRODUCT_LINK",
            43: "SHOWCASE_AD_LOCAL_PRODUCT_LINK",
            44: "PROMOTION_EXTENSION",
            45: "SWIPEABLE_GALLERY_AD_HEADLINE",
            46: "SWIPEABLE_GALLERY_AD_SWIPES",
            47: "SWIPEABLE_GALLERY_AD_SEE_MORE",
            48: "SWIPEABLE_GALLERY_AD_SITELINK_ONE",
            49: "SWIPEABLE_GALLERY_AD_SITELINK_TWO",
            50: "SWIPEABLE_GALLERY_AD_SITELINK_THREE",
            51: "SWIPEABLE_GALLERY_AD_SITELINK_FOUR",
            52: "SWIPEABLE_GALLERY_AD_SITELINK_FIVE",
            53: "HOTEL_PRICE",
            54: "PRICE_EXTENSION",
            55: "HOTEL_BOOK_ON_GOOGLE_ROOM_SELECTION",
            56: "SHOPPING_COMPARISON_LISTING",
            57: "CROSS_NETWORK",
            58: "AD_IMAGE",
            59: "TRAVEL_ASSETS"
        }
        return click_type_mapping.get(enum_value, "UNKNOWN")
    
    
    def get_conversion_action_category(value):
        conversion_action_mapping = {
            0: 'UNSPECIFIED',
            1: 'UNKNOWN',
            2: 'DEFAULT',
            3: 'PAGE_VIEW',
            4: 'PURCHASE',
            5: 'SIGNUP',
            7: 'DOWNLOAD',
            8: 'ADD_TO_CART',
            9: 'BEGIN_CHECKOUT',
            10: 'SUBSCRIBE_PAID',
            11: 'PHONE_CALL_LEAD',
            12: 'IMPORTED_LEAD',
            13: 'SUBMIT_LEAD_FORM',
            14: 'BOOK_APPOINTMENT',
            15: 'REQUEST_QUOTE',
            16: 'GET_DIRECTIONS',
            17: 'OUTBOUND_CLICK',
            18: 'CONTACT',
            19: 'ENGAGEMENT',
            20: 'STORE_VISIT',
            21: 'STORE_SALE',
            22: 'QUALIFIED_LEAD',
            23: 'CONVERTED_LEAD'
        }
        return conversion_action_mapping.get(value, "UNKNOWN")
        
    def get_config_dict():
        config_dict = {
            "developer_token": os.getenv("DEVELOPER_TOKEN"),
            "client_id": os.getenv("GG_CLIENT_ID"),
            "client_secret": os.getenv("GG_CLIENT_SECRET"),
            "refresh_token": os.getenv("REFRESH_TOKEN"),
            "login_customer_id": os.getenv("LOGIN_CUSTOMER_ID"),
            "use_proto_plus": True
        }
        return config_dict
    
    def list_sites(service):
        listSite = []
        sites = service.sites().list().execute()
        for site in sites.get('siteEntry', []):
            listSite.append(site['siteUrl'])
        
        return listSite
    
    def query_search_analytics(service, site_url, start_date, end_date, dimensions):
        request = {
            'startDate': start_date,
            'endDate': end_date,
            'dimensions': dimensions,
            'rowLimit': 25000
        }
        data = service.searchanalytics().query(siteUrl=site_url, body=request).execute()
        return data
    
    def transform_data_main(records, url=None):
        transformed_records = []
        for record in records:
            transformed_records.append({
                "date": record['keys'][0],
                "url": url,
                "country": record['keys'][1],
                "device": record['keys'][2],
                "clicks": record['clicks'],
                "impressions": record['impressions'],
                "ctr": record['ctr'],
                "position": record['position']
            })
        return transformed_records
    
    def transform_data_keyword(records, url=None):
        transformed_records = []
        for record in records:
            transformed_records.append({
                "date": record['keys'][0],
                "url": url,
                "country": record['keys'][1],
                "device": record['keys'][2],
                "keyword": record['keys'][3],
                "clicks": record['clicks'],
                "impressions": record['impressions'],
                "ctr": record['ctr'],
                "position": record['position']
            })
        return transformed_records
    
    def transform_data_page(records, url=None):
        transformed_records = []
        for record in records:
            transformed_records.append({
                "date": record['keys'][0],
                "url": url,
                "page": record['keys'][1],
                "clicks": record['clicks'],
                "impressions": record['impressions'],
                "ctr": record['ctr'],
                "position": record['position']
            })
        return transformed_records
    
    def transform_data_keyword_page(records, url=None):
        transformed_records = []
        for record in records:
            transformed_records.append({
                "date": record['keys'][0],
                "url": url,
                "country": record['keys'][1],
                "device": record['keys'][2],
                "keyword": record['keys'][3],
                "page": record['keys'][4],
                "clicks": record['clicks'],
                "impressions": record['impressions'],
                "ctr": record['ctr'],
                "position": record['position']
            })
        return transformed_records
        