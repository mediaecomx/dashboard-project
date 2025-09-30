# FILE: processor.py

import pandas as pd
import numpy as np
import streamlit as st
from datetime import datetime, timezone
import re
# --- THAY ĐỔI ---
# Không import config trực tiếp nữa
# --- KẾT THÚC THAY ĐỔI ---
from services import GoogleAnalyticsService, ShopifyService

class DataProcessor:
    # --- THAY ĐỔI: Nhận config trong __init__ ---
    def __init__(self, ga_service: GoogleAnalyticsService, shopify_service: ShopifyService, config):
        self.ga_service = ga_service
        self.shopify_service = shopify_service
        self.symbols = config.SYMBOLS
        self.page_title_map = config.page_title_map
        self.product_to_symbol_map = config.product_to_symbol_map
        
        if 'last_ga_data' not in st.session_state:
            st.session_state.last_ga_data = None
        if 'last_ga_fetch_time' not in st.session_state:
            st.session_state.last_ga_fetch_time = None
        if 'last_quota_details' not in st.session_state:
            st.session_state.last_quota_details = None
        if 'last_ga_kpis' not in st.session_state:
            st.session_state.last_ga_kpis = (0, 0)
        
    def _extract_core_and_symbol(self, title: str, symbols: list):
        found_symbol = ""
        title_str = str(title)
        for s in symbols:
            if s in title_str:
                found_symbol = s
                break
        cleaned_text = title_str.lower().split('–')[0].split(' - ')[0]
        for s in symbols:
            cleaned_text = cleaned_text.replace(s, '')
        cleaned_text = re.sub(r'[^\w\s]', '', cleaned_text, flags=re.UNICODE).strip()
        return cleaned_text, found_symbol

    def get_marketer_from_page_title(self, title: str) -> str:
        for symbol in self.symbols:
            if symbol in title:
                return self.page_title_map[symbol]
        return ""

    def _get_product_symbol(self, product_title: str) -> str:
        for product_name, symbol in self.product_to_symbol_map.items():
            if product_name.lower() in product_title.lower():
                return symbol
        return "🛒"
        
    def get_processed_realtime_data(self, property_id: str, selected_tz):
        QUOTA_GUARD_THRESHOLD = 500
        QUOTA_DEGRADED_THRESHOLD = 2000
        DYNAMIC_TTLS = {'normal': 60, 'degraded': 300}
        can_fetch = True
        reason = ""
        if st.session_state.last_quota_details and st.session_state.last_ga_fetch_time:
            remaining_hourly_tokens = st.session_state.last_quota_details.get("tokens_per_hour", {}).get("remaining", float('inf'))
            if remaining_hourly_tokens < QUOTA_GUARD_THRESHOLD:
                can_fetch = False
                reason = f"API call blocked. Hourly quota is critically low ({remaining_hourly_tokens} remaining)."
            else:
                ttl_to_use = DYNAMIC_TTLS['degraded'] if remaining_hourly_tokens < QUOTA_DEGRADED_THRESHOLD else DYNAMIC_TTLS['normal']
                time_since_last_fetch = (datetime.now(timezone.utc) - st.session_state.last_ga_fetch_time).total_seconds()
                if time_since_last_fetch < ttl_to_use:
                    can_fetch = False
                    reason = f"Using cached data. Next fetch in {int(ttl_to_use - time_since_last_fetch)}s (Mode: {'Degraded' if ttl_to_use == 300 else 'Normal'})."
        
        if can_fetch:
            ga_raw_df, quota_details, fetch_time, active_users_5min, active_users_30min = self.ga_service.fetch_realtime_report(property_id)
            st.session_state.last_ga_data = ga_raw_df
            st.session_state.last_quota_details = quota_details
            st.session_state.last_ga_fetch_time = fetch_time
            st.session_state.last_ga_kpis = (active_users_5min, active_users_30min)
            if quota_details.get("tokens_per_hour", {}).get("remaining", 0) < QUOTA_DEGRADED_THRESHOLD:
                 st.sidebar.warning(f"Quota is low! Refresh rate reduced to 5 minutes.")
        else:
            ga_raw_df = st.session_state.last_ga_data
            quota_details = st.session_state.last_quota_details
            fetch_time = st.session_state.last_ga_fetch_time
            active_users_5min, active_users_30min = st.session_state.last_ga_kpis
            st.sidebar.info(reason)

        shopify_raw_df = self.shopify_service.fetch_realtime_purchases()

        if ga_raw_df is None or ga_raw_df.empty:
            saved_5min, saved_30min = st.session_state.last_ga_kpis
            return {
                "active_users_5min": saved_5min, "active_users_30min": saved_30min, "total_views": 0,
                "purchase_count_30min": 0, "final_pages_df": pd.DataFrame(),
                "per_min_df": pd.DataFrame(), "fetch_time": fetch_time or datetime.now(timezone.utc),
                "quota_details": quota_details or {}, "debug_data": {},
                "purchase_events": pd.DataFrame()
            }
        
        total_views = ga_raw_df['Views'].sum()
        purchase_count_30min = shopify_raw_df['Purchases'].sum() if not shopify_raw_df.empty else 0
        per_min_summary = ga_raw_df.groupby('minutesAgo')['Active Users'].sum()
        per_min_data = {str(i): per_min_summary.get(i, 0) for i in range(30)}
        per_min_df = pd.DataFrame([{"Time": f"-{int(k)} min", "Active Users": v} for k, v in sorted(per_min_data.items(), key=lambda item: int(item[0]))])
        
        ga_pages_df = ga_raw_df.groupby("Page Title and Screen Class").agg(ActiveUsers=('Active Users', 'sum')).reset_index()
        ga_processed_df = ga_pages_df.copy()
        ga_processed_df[['core_title', 'symbol']] = ga_processed_df['Page Title and Screen Class'].apply(lambda x: pd.Series(self._extract_core_and_symbol(x, self.symbols)))
        
        purchase_events_df = pd.DataFrame()
        if not shopify_raw_df.empty:
            shopify_processed_df = shopify_raw_df.copy()
            shopify_processed_df['created_at'] = pd.to_datetime(shopify_processed_df['created_at'])
            events_data = shopify_processed_df.copy()
            events_data['Marketer'] = events_data['Product Title'].apply(self.get_marketer_from_page_title)
            events_data['ProductSymbol'] = events_data['Product Title'].apply(self._get_product_symbol)
            events_data = events_data[events_data['Marketer'] != ""]
            purchase_events_df = events_data[['created_at', 'Marketer', 'ProductSymbol']].copy()
            shopify_processed_df[['core_title', 'symbol']] = shopify_processed_df['Product Title'].apply(lambda x: pd.Series(self._extract_core_and_symbol(x, self.symbols)))
            shopify_grouped = shopify_processed_df.groupby(['core_title', 'symbol']).agg(
                Purchases=('Purchases', 'sum'),
                Revenue=('Revenue', 'sum'),
                LastPurchaseTime=('created_at', 'max')
            ).reset_index()
            merged_df = pd.merge(ga_processed_df, shopify_grouped, on=['core_title', 'symbol'], how='left')
        else:
            merged_df = ga_processed_df.copy()
            merged_df['Purchases'] = 0
            merged_df['Revenue'] = 0.0
            merged_df['LastPurchaseTime'] = pd.NaT

        merged_df["Purchases"] = merged_df["Purchases"].fillna(0).astype(int)
        merged_df["Revenue"] = merged_df["Revenue"].fillna(0).astype(float)
        merged_df["CR"] = np.divide(merged_df["Purchases"], merged_df["ActiveUsers"], out=np.zeros_like(merged_df["ActiveUsers"], dtype=float), where=(merged_df["ActiveUsers"] != 0)) * 100
        merged_df['Marketer'] = merged_df['Page Title and Screen Class'].apply(self.get_marketer_from_page_title)
        
        def format_timestamp_to_hms(ts):
            if pd.notna(ts):
                return ts.astimezone(selected_tz).strftime('%H:%M:%S')
            return ""
        
        merged_df['Last Purchase'] = merged_df['LastPurchaseTime'].apply(format_timestamp_to_hms)
        
        final_pages_df = merged_df.sort_values(by="ActiveUsers", ascending=False).rename(
            columns={"ActiveUsers": "Active Users"}
        )[
            ["Page Title and Screen Class", "Marketer", "Active Users", "Purchases", "Last Purchase", "Revenue", "CR"]
        ]

        debug_data = {
            "ga_raw": ga_raw_df, "shopify_raw": shopify_raw_df,
            "ga_processed": ga_processed_df, "merged": merged_df
        }
        if 'shopify_grouped' in locals():
            debug_data["shopify_grouped"] = shopify_grouped
        
        return {
            "active_users_5min": active_users_5min, "active_users_30min": active_users_30min,
            "total_views": total_views, "purchase_count_30min": purchase_count_30min,
            "final_pages_df": final_pages_df, "per_min_df": per_min_df,
            "fetch_time": fetch_time, "quota_details": quota_details, "debug_data": debug_data,
            "purchase_events": purchase_events_df
        }

    def get_processed_historical_data(self, property_id: str, start_date_str, end_date_str, segment):
        ga_raw_df = self.ga_service.fetch_historical_report(property_id, start_date_str, end_date_str, segment)
        shopify_raw_df = self.shopify_service.fetch_historical_purchases(start_date_str, end_date_str, segment)

        if ga_raw_df.empty:
            return pd.DataFrame(), {"ga_raw": ga_raw_df, "shopify_raw": shopify_raw_df}
        ga_processed_df = ga_raw_df.copy()
        ga_processed_df[['core_title', 'symbol']] = ga_processed_df['Page Title'].apply(lambda x: pd.Series(self._extract_core_and_symbol(x, self.symbols)))
        merge_on_cols = ['core_title', 'symbol']
        if segment == 'By Day': merge_on_cols.append('Date')
        elif segment == 'By Week': merge_on_cols.append('Week')
        if not shopify_raw_df.empty:
            shopify_processed_df = shopify_raw_df.copy()
            shopify_processed_df[['core_title', 'symbol']] = shopify_processed_df['Page Title'].apply(lambda x: pd.Series(self._extract_core_and_symbol(x, self.symbols)))
            shopify_grouped = shopify_processed_df.groupby(merge_on_cols)[['Purchases', 'Revenue']].sum().reset_index()
            merged_df = pd.merge(ga_processed_df, shopify_grouped, on=merge_on_cols, how='left')
        else:
            merged_df = ga_processed_df.copy()
            merged_df['Purchases'] = 0
            merged_df['Revenue'] = 0.0
        merged_df["Purchases"] = merged_df["Purchases"].fillna(0).astype(int)
        merged_df["Revenue"] = merged_df["Revenue"].fillna(0).astype(float)
        agg_cols = ['core_title', 'symbol']
        if segment == 'By Day': agg_cols.append('Date')
        elif segment == 'By Week': agg_cols.append('Week')
        final_grouped_df = merged_df.groupby(agg_cols).agg(
            **{'Page Title': ('Page Title', 'first'), 'Sessions': ('Sessions', 'sum'), 'Users': ('Users', 'sum'),
               'Purchases': ('Purchases', 'first'), 'Revenue': ('Revenue', 'first')}
        ).reset_index()
        final_grouped_df['Marketer'] = final_grouped_df['Page Title'].apply(self.get_marketer_from_page_title)
        final_grouped_df['Session CR'] = np.divide(final_grouped_df['Purchases'], final_grouped_df['Sessions'], out=np.zeros_like(final_grouped_df['Sessions'], dtype=float), where=(final_grouped_df['Sessions'] != 0)) * 100
        final_grouped_df['User CR'] = np.divide(final_grouped_df['Purchases'], final_grouped_df['Users'], out=np.zeros_like(final_grouped_df['Users'], dtype=float), where=(final_grouped_df['Users'] != 0)) * 100
        column_order = ["Page Title", "Marketer", "Sessions", "Users", "Purchases", "Revenue", "Session CR", "User CR"]
        if segment == 'By Day': column_order.insert(0, 'Date')
        elif segment == 'By Week': column_order.insert(0, 'Week')
        all_data_df = final_grouped_df.sort_values(by=["Sessions"], ascending=False)[column_order]
        if segment != 'Summary':
            all_data_df = all_data_df.sort_values(by=[column_order[0], "Sessions"], ascending=[True, False])
        debug_data = {"ga_raw": ga_raw_df, "shopify_raw": shopify_raw_df, "merged": merged_df, "final": all_data_df}
        return all_data_df, debug_data
