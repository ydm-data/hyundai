import os
import math
import hmac
import json
import base64
import hashlib
import urllib.request

from dotenv import load_dotenv
from datetime import datetime, timedelta

class LINE_Connector:
    def encode_with_base64(value: bytes) -> str:
        return base64.urlsafe_b64encode(value).decode('utf-8').rstrip("=")

    def calc_sha256_digest(content: str) -> str:
        sha256 = hashlib.new('sha256')
        sha256.update(content.encode())
        return sha256.hexdigest()
    
    def get_online_report(target_account,current_date,end_date,size,reportLevel):
        load_dotenv(override=True)
        secret_key = os.getenv('LINE_SECRET_KEY')
        access_key = os.getenv('LINE_ACCESS_KEY')
        method = "GET"
        flattened_data = []
        for adaccountId in target_account:
            while current_date <= end_date:
                page=1
                has_more_pages = True
                
                while has_more_pages:
                    canonical_url = f"/api/v3/adaccounts/{adaccountId}/reports/online/{reportLevel}"
                    url_parameters = f"?size={size}&since={current_date.strftime('%Y-%m-%d')}&until={current_date.strftime('%Y-%m-%d')}&page={page}"
                    request_body =  {}

                    has_request_body = request_body is not None

                    endpoint = 'https://ads.line.me' + canonical_url + url_parameters
                    request_body_json = json.dumps(request_body) if has_request_body else ""
                    content_type = 'application/json' if has_request_body else ""

                    jws_header = LINE_Connector.encode_with_base64(
                        json.dumps({
                            "alg": "HS256",
                            "kid": access_key,
                            "typ": "text/plain",
                        }).encode()
                    )

                    hex_digest = LINE_Connector.calc_sha256_digest(request_body_json)
                    payload_date = datetime.utcnow().strftime('%Y%m%d')
                    payload = "%s\n%s\n%s\n%s" % (hex_digest, content_type, payload_date, canonical_url)
                    jws_payload = LINE_Connector.encode_with_base64(payload.encode())

                    signing_input = "%s.%s" % (jws_header, jws_payload)
                    signature = hmac.new(
                        secret_key.encode(),
                        signing_input.encode(),
                        hashlib.sha256
                    ).digest()
                    encoded_signature = LINE_Connector.encode_with_base64(signature)
                    token = "%s.%s.%s" % (jws_header, jws_payload, encoded_signature)

                    http_headers = {
                        "Date": datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT'),
                        "Authorization": "Bearer %s" % token
                    }

                    if has_request_body:
                        http_headers["Content-Type"] = content_type
                        req = urllib.request.Request(endpoint, data=request_body_json.encode(), headers=http_headers, method=method)
                    else:
                        req = urllib.request.Request(endpoint, headers=http_headers, method=method)

                    try:  
                        with urllib.request.urlopen(req) as res:
                            resp = res.read()
                        
                        # Parse response
                        response_data = resp.decode()
                        data = json.loads(response_data)

                    except urllib.error.HTTPError as e:
                        print("HTTP Error:", e.code, e.reason)
                        print(e.read().decode())
                    except urllib.error.URLError as e:
                        print("URL Error:", e.reason)

                    for record in data['datas']:
                        flattened_data.append({
                            "adaccount_id": record["adaccount"]["id"],
                            "adaccount_name": record["adaccount"]["name"],
                            "currency": record["adaccount"]["currency"],
                            "timezone": record["adaccount"]["timezone"],
                            "configuredStatus": record["adaccount"]["configuredStatus"],
                            "deliveryStatus": record["adaccount"]["deliveryStatus"],
                            "campaign_id": record["campaign"]["id"],
                            "campaign_name": record["campaign"]["name"],
                            "campaign_objective": record["campaign"]["campaignObjective"],
                            "start_date": record["campaign"]["startDate"],
                            "end_date": record["campaign"]["endDate"],
                            "adgroup_id": record["adgroup"]["id"],
                            "adgroup_name": record["adgroup"]["name"],
                            "ad_id": record["ad"]["id"],
                            "ad_name": record["ad"]["name"],
                            "statistics": record['statistics'],
                            "date" : current_date.strftime('%Y-%m-%d')
                        })
                    if page >= math.ceil(data['paging']['totalElements'] / 100):
                        has_more_pages = False
                    page += 1
                current_date += timedelta(days=1)
        return flattened_data
    

    def get_ad_info(adaccountId_list):
        all_data = []
        for adaccountId in adaccountId_list:
            has_more_pages = True
            page = 1
            while has_more_pages:
                print(f" Page: {page}",end=", ")
                url = f'https://ads.line.me/api/v3/adaccounts/{adaccountId}/ads'
                secret_key = os.getenv('LINE_SECRET_KEY')
                access_key = os.getenv('LINE_ACCESS_KEY')

                canonical_url = f"/api/v3/adaccounts/{adaccountId}/ads"
                url_parameters = ""
                request_body = {}
                method = "GET"
                has_request_body = request_body is not None

                endpoint = 'https://ads.line.me' + canonical_url + url_parameters
                request_body_json = json.dumps(request_body) if has_request_body else ""
                content_type = 'application/json' if has_request_body else ""

                jws_header = LINE_Connector.encode_with_base64(
                    json.dumps({
                        "alg": "HS256",
                        "kid": access_key,
                        "typ": "text/plain",
                    }).encode()
                )

                hex_digest = LINE_Connector.calc_sha256_digest(request_body_json)
                payload_date = datetime.utcnow().strftime('%Y%m%d')
                payload = "%s\n%s\n%s\n%s" % (hex_digest, content_type, payload_date, canonical_url)
                jws_payload = LINE_Connector.encode_with_base64(payload.encode())

                signing_input = "%s.%s" % (jws_header, jws_payload)
                signature = hmac.new(
                    secret_key.encode(),
                    signing_input.encode(),
                    hashlib.sha256
                ).digest()
                encoded_signature = LINE_Connector.encode_with_base64(signature)
                token = "%s.%s.%s" % (jws_header, jws_payload, encoded_signature)

                http_headers = {
                    "Date": datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT'),
                    "Authorization": "Bearer %s" % token
                }

                if has_request_body:
                    http_headers["Content-Type"] = content_type
                    req = urllib.request.Request(endpoint, data=request_body_json.encode(), headers=http_headers, method=method)
                else:
                    req = urllib.request.Request(endpoint, headers=http_headers, method=method)

                with urllib.request.urlopen(req) as res:
                    resp = res.read()
                    response_data = resp.decode()
                    data = json.loads(response_data)
                    all_data += data['datas']
                if page >= math.ceil(data['paging']['totalElements'] / 100):
                    has_more_pages = False
                page += 1
        return all_data