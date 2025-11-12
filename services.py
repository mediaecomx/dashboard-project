# FILE: services.py

import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta, timezone
import pytz
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunRealtimeReportRequest, RunReportRequest, Dimension, Metric, MinuteRange,
    DateRange
)

class GoogleAnalyticsService:
    def __init__(self, config):
        self.client = BetaAnalyticsDataClient(credentials=config.ga_credentials)

    @st.cache_data(ttl=60)
    def fetch_realtime_report(_self, property_id: str):
        try:
            kpi_request = RunRealtimeReportRequest(
                property=f"properties/{property_id}",
                metrics=[Metric(name="activeUsers")],
                minute_ranges=[
                    MinuteRange(start_minutes_ago=29, end_minutes_ago=0),
                    MinuteRange(start_minutes_ago=4, end_minutes_ago=0)
                ]
            )
            pages_request = RunRealtimeReportRequest(
                property=f"properties/{property_id}",
                dimensions=[Dimension(name="unifiedScreenName"), Dimension(name="minutesAgo")],
                metrics=[Metric(name="activeUsers"), Metric(name="screenPageViews")],
                minute_ranges=[MinuteRange(start_minutes_ago=29, end_minutes_ago=0)],
                return_property_quota=True
            )
            kpi_response = _self.client.run_realtime_report(kpi_request)
            pages_response = _self.client.run_realtime_report(pages_request)
            
            active_users_30min = (int(kpi_response.rows[0].metric_values[0].value) if kpi_response.rows else 0)
            active_users_5min = (int(kpi_response.rows[1].metric_values[0].value) if len(kpi_response.rows) > 1 else 0)
            pq = getattr(pages_response, "property_quota", None)
            quota_details = {
                "tokens_per_hour": {"consumed": pq.tokens_per_hour.consumed if pq and pq.tokens_per_hour else 0, "remaining": pq.tokens_per_hour.remaining if pq and pq.tokens_per_hour else "N/A"},
                "tokens_per_day": {"consumed": pq.tokens_per_day.consumed if pq and pq.tokens_per_day else 0, "remaining": pq.tokens_per_day.remaining if pq and pq.tokens_per_day else "N/A"}
            }
            rows = [{"Page Title and Screen Class": row.dimension_values[0].value, "minutesAgo": int(row.dimension_values[1].value), "Active Users": int(row.metric_values[0].value), "Views": int(row.metric_values[1].value)} for row in pages_response.rows]
            return pd.DataFrame(rows), quota_details, datetime.now(pytz.utc), active_users_5min, active_users_30min
        except Exception as e:
            st.error(f"Lỗi khi lấy dữ liệu Realtime từ Google Analytics: {e}")
            return pd.DataFrame(), {}, datetime.now(pytz.utc), 0, 0

    @st.cache_data
    def fetch_historical_report(_self, property_id: str, start_date: str, end_date: str, segment: str):
        try:
            dimensions = [Dimension(name="pageTitle")]
            if segment == 'By Day': dimensions.append(Dimension(name="date"))
            elif segment == 'By Week': dimensions.append(Dimension(name="week"))
            request = RunReportRequest(
                property=f"properties/{property_id}",
                dimensions=dimensions,
                metrics=[Metric(name="sessions"), Metric(name="totalUsers")],
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                limit=50000
            )
            response = _self.client.run_report(request)
            rows = []
            for row in response.rows:
                item_data = {"Page Title": row.dimension_values[0].value, "Sessions": int(row.metric_values[0].value), "Users": int(row.metric_values[1].value)}
                if segment == 'By Day':
                    item_data['Date'] = datetime.strptime(row.dimension_values[1].value, '%Y%m%d').strftime('%Y-%m-%d')
                elif segment == 'By Week':
                    item_data['Week'] = row.dimension_values[1].value
                rows.append(item_data)
            return pd.DataFrame(rows)
        except Exception as e:
            st.error(f"Lỗi khi lấy dữ liệu Lịch sử từ Google Analytics: {e}")
            return pd.DataFrame()

class ShopifyService:
    def __init__(self, config):
        # Lưu lại toàn bộ danh sách cấu hình các cửa hàng
        self.stores_config = config.shopify_stores_config

    @st.cache_data(ttl=60)
    def fetch_realtime_purchases(_self):
        # Tạo một danh sách để chứa dữ liệu từ tất cả các cửa hàng
        all_stores_purchase_data = []
        
        # Lặp qua từng cửa hàng trong file cấu hình
        for store_creds in _self.stores_config:
            store_id = store_creds.get("store_id", "unknown_store")
            try:
                # Tạo URL và Header riêng cho từng cửa hàng
                base_url = f"https://{store_creds['store_url']}/admin/api/{store_creds['api_version']}/orders.json"
                headers = {"X-Shopify-Access-Token": store_creds['access_token']}
                
                thirty_minutes_ago = (datetime.now(timezone.utc) - timedelta(minutes=30)).strftime('%Y-%m-%dT%H:%M:%SZ')
                params = {"created_at_min": thirty_minutes_ago, "status": "any", "fields": "line_items,total_shipping_price_set,subtotal_price,created_at"}
                
                print(f"Fetching Shopify data for store: {store_id}")
                response = requests.get(base_url, headers=headers, params=params, timeout=10)
                response.raise_for_status()
                orders = response.json().get('orders', [])
                
                for order in orders:
                    subtotal = float(order.get('subtotal_price', 0.0))
                    shipping_fee = float(order.get('total_shipping_price_set', {}).get('shop_money', {}).get('amount', 0.0))
                    order_created_at = order.get('created_at')
                    for item in order.get('line_items', []):
                        item_price = float(item['price'])
                        item_quantity = item['quantity']
                        item_total_value = item_price * item_quantity
                        shipping_allocation = (shipping_fee * (item_total_value / subtotal)) if subtotal > 0 else 0
                        # Thêm dữ liệu vào danh sách chung
                        all_stores_purchase_data.append({
                            'Product Title': item['title'], 
                            'Purchases': item_quantity, 
                            'Revenue': item_total_value + shipping_allocation,
                            'created_at': order_created_at 
                        })
            except Exception as e:
                print(f"Lỗi khi lấy dữ liệu Realtime từ Shopify store '{store_id}': {e}")
                # Bỏ qua cửa hàng bị lỗi và tiếp tục với các cửa hàng khác
                continue
                
        # Trả về một DataFrame duy nhất chứa dữ liệu của tất cả các cửa hàng
        return pd.DataFrame(all_stores_purchase_data)

    @st.cache_data
    def fetch_historical_purchases(_self, start_date: str, end_date: str, segment: str):
        all_stores_purchase_data = []
        tz = pytz.timezone('Asia/Ho_Chi_Minh')
        start_dt_obj = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt_obj = datetime.strptime(end_date, "%Y-%m-%d")
        start_time_aware = tz.localize(start_dt_obj)
        end_time_aware = tz.localize(end_dt_obj + timedelta(days=1))
        
        # Lặp qua từng cửa hàng trong file cấu hình
        for store_creds in _self.stores_config:
            store_id = store_creds.get("store_id", "unknown_store")
            try:
                base_url = f"https://{store_creds['store_url']}/admin/api/{store_creds['api_version']}/orders.json"
                headers = {"X-Shopify-Access-Token": store_creds['access_token']}
                
                url = base_url
                params = {
                    "status": "any", 
                    "created_at_min": start_time_aware.isoformat(), 
                    "created_at_max": end_time_aware.isoformat(), 
                    "limit": 250, 
                    "fields": "id,line_items,subtotal_price,total_shipping_price_set,created_at"
                }
                
                print(f"Fetching historical Shopify data for store: {store_id}")
                while url:
                    response = requests.get(url, headers=headers, params=params, timeout=15)
                    response.raise_for_status()
                    data = response.json()
                    orders = data.get('orders', [])
                    for order in orders:
                        subtotal = float(order.get('subtotal_price', 0.0))
                        shipping_fee = float(order.get('total_shipping_price_set', {}).get('shop_money', {}).get('amount', 0.0))
                        created_at_utc = datetime.fromisoformat(order['created_at'].replace('Z', '+00:00'))
                        created_at_local = created_at_utc.astimezone(tz)
                        for item in order.get('line_items', []):
                            item_price = float(item.get('price', 0.0))
                            item_quantity = int(item.get('quantity', 0))
                            item_total_value = item_price * item_quantity
                            shipping_allocation = (shipping_fee * (item_total_value / subtotal)) if subtotal > 0 else 0
                            item_data = {'Page Title': item['title'], 'Purchases': item_quantity, 'Revenue': item_total_value + shipping_allocation}
                            if segment == 'By Day':
                                item_data['Date'] = created_at_local.strftime('%Y-%m-%d')
                            elif segment == 'By Week':
                                item_data['Week'] = created_at_local.strftime('%Y-%U')
                            all_stores_purchase_data.append(item_data)
                    
                    url = None
                    if 'Link' in response.headers:
                        links = requests.utils.parse_header_links(response.headers['Link'])
                        for link in links:
                            if link.get('rel') == 'next':
                                url = link.get('url')
                                params = None
                                break
            except Exception as e:
                st.error(f"Lỗi khi lấy dữ liệu Lịch sử từ Shopify store '{store_id}': {e}")
                continue

        if not all_stores_purchase_data: return pd.DataFrame()
        
        purchases_df = pd.DataFrame(all_stores_purchase_data)
        group_by_cols = ['Page Title']
        if segment == 'By Day': group_by_cols.append('Date')
        elif segment == 'By Week': group_by_cols.append('Week')
        
        # Tính tổng hợp dữ liệu từ tất cả các cửa hàng
        return purchases_df.groupby(group_by_cols).agg({'Purchases': 'sum', 'Revenue': 'sum'}).reset_index()
