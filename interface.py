# FILE: interface.py

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import time
import pytz
import requests
from datetime import datetime, timedelta
from config import config

# ... (Toàn bộ các hàm tiện ích và các hàm của class DashboardUI giữ nguyên) ...

def highlight_metrics(val):
    if isinstance(val, (int, float)) and val > 0:
        return 'background-color: #023020; color: #23d123; font-weight: bold;'
    if isinstance(val, str) and val != "":
        return 'background-color: #023020; color: #23d123; font-weight: bold;'
    return ''

def get_heatmap_color_and_text(value, target, cold_color, hot_color):
    if target == 0:
        bg_rgb = cold_color
    else:
        ratio = min(1.0, value / target)
        r = int(cold_color[0] + ratio * (hot_color[0] - cold_color[0]))
        g = int(cold_color[1] + ratio * (hot_color[1] - cold_color[1]))
        b = int(cold_color[2] + ratio * (hot_color[2] - cold_color[2]))
        bg_rgb = (r, g, b)
    brightness = (bg_rgb[0] * 299 + bg_rgb[1] * 587 + bg_rgb[2] * 114) / 1000
    text_color = "#FFFFFF" if brightness < 140 else "#000000"
    return f"rgb({bg_rgb[0]},{bg_rgb[1]},{bg_rgb[2]})", text_color

def render_progress_bar(value, total):
    if not isinstance(value, (int, float)) or total == 0:
        percentage = 0
    else:
        percentage = min(100, (value / total) * 100)
    
    if percentage >= 90:
        color = "#FF4B4B"
    elif percentage >= 75:
        color = "#FFC732"
    else:
        color = "#00B084"
    
    st.markdown(f"""
        <style>
            .stProgress > div > div > div > div {{
                background-color: {color};
            }}
        </style>""", unsafe_allow_html=True)
    st.progress(percentage / 100)

class DashboardUI:
    def __init__(self, auth, data_processor):
        self.auth = auth
        self.processor = data_processor
        if 'realtime_history' not in st.session_state:
            st.session_state.realtime_history = []

    def render_sidebar(self):
        with st.sidebar:
            user_info = st.session_state['user_info']
            avatar_url = user_info.get("avatar_url") or config.default_avatar_url
            st.markdown(f"""
                <div style="display: flex; flex-direction: column; align-items: center; text-align: center; margin-bottom: 20px;">
                    <img src="{avatar_url}" style="width: 100px; height: 100px; border-radius: 50%; object-fit: cover; border: 2px solid #3c4043;">
                    <p style="margin-top: 10px; margin-bottom: 0; font-size: 1em; color: #d0d0d0;">Welcome,</p>
                    <p style="margin: 0; font-size: 1.25em; font-weight: bold; color: #1ED760;">{user_info['username']}</p>
                </div>
            """, unsafe_allow_html=True)
            
            st.title("Navigation")
            page = st.radio("Choose a report:", ("Realtime Dashboard", "Landing Page Report", "Profile"))
            
            self.auth.logout("Log Out", "sidebar") 

            impersonating = False
            effective_user_info = user_info
            if user_info['role'] == 'admin':
                st.divider()
                employee_details = {v['username']: v for k, v in config.users_details.items() if v.get('role') == 'employee'}
                options = ["None (View as Admin)"] + list(employee_details.keys())
                selected_user_name = st.selectbox("Impersonate User", options=options)
                if selected_user_name != "None (View as Admin)":
                    impersonating = True
                    effective_user_info = config.get_user_details_by_username(selected_user_name)
                    st.info(f"Viewing as **{selected_user_name}**")
            
            debug_mode = st.checkbox("Enable Debug Mode") if user_info['role'] == 'admin' and not impersonating else False
        
        return page, effective_user_info, debug_mode

    def render_profile_page(self):
        st.title("👤 Your Profile")
        st.header("Update Your Avatar")
        col1, col2 = st.columns([1, 2])
        with col1:
            avatar_url = st.session_state['user_info'].get('avatar_url') or config.default_avatar_url
            st.image(avatar_url, width=150)
        with col2:
            uploaded_file = st.file_uploader("Upload a new image (JPG, PNG):", type=["jpg", "jpeg", "png"])
            if uploaded_file:
                with st.spinner("Uploading to Cloudinary..."):
                    try:
                        response = requests.post(f"https://api.cloudinary.com/v1_1/{config.cloudinary_cloud_name}/image/upload",
                                                 files={"file": uploaded_file.getvalue()},
                                                 data={"upload_preset": config.cloudinary_upload_preset},
                                                 timeout=30)
                        response.raise_for_status()
                        new_link = response.json().get("secure_url")
                        if new_link:
                            username = st.session_state['user_info']['username']
                            config.supabase.table("profiles").upsert({"username": username, "avatar_url": new_link}).execute()
                            st.session_state['avatar_url'] = new_link
                            st.session_state['user_info']['avatar_url'] = new_link
                            st.success("Avatar updated successfully!")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error("Upload succeeded but no URL returned.")
                    except Exception as e:
                        st.error(f"Failed to upload image. Error: {e}")

    def render_realtime_dashboard(self, effective_user_info, debug_mode):
        st.title("🚀 Realtime Dashboard")
        with st.sidebar:
            selected_tz_name = st.selectbox("Select Timezone", options=list(config.TIMEZONE_MAPPINGS.keys()), key="timezone_selector")
            refresh_interval = st.session_state.get('refresh_interval', 75)
            if st.session_state['user_info']['role'] == 'admin' and effective_user_info['role'] == 'admin':
                new_interval = st.number_input("Set Refresh Interval (seconds)", min_value=30, value=refresh_interval, step=15)
                if new_interval != refresh_interval:
                    st.session_state.refresh_interval = new_interval
                    st.rerun()
                refresh_interval = new_interval
                time_window_options = [30, 60, 90, 120]
                current_window = st.session_state.get('time_window', 60)
                try: default_index = time_window_options.index(current_window)
                except ValueError: default_index = 1
                selected_window = st.selectbox("Set Chart Time Window (minutes)", options=time_window_options, index=default_index)
                if selected_window != current_window:
                    st.session_state.time_window = selected_window
                    st.session_state.realtime_history = []
                    st.rerun()

        selected_tz = pytz.timezone(config.TIMEZONE_MAPPINGS[selected_tz_name])
        timer_placeholder, placeholder = st.empty(), st.empty()

        with placeholder.container():
            data = self.processor.get_processed_realtime_data(selected_tz)
            localized_fetch_time = data['fetch_time'].astimezone(selected_tz)
            st.markdown(f"*Last update: {localized_fetch_time.strftime('%Y-%m-%d %H:%M:%S')}*")
            top_col1, top_col2, top_col3 = st.columns(3)
            with top_col1:
                bg_color, text_color = get_heatmap_color_and_text(data['active_users_5min'], config.TARGET_USERS_5MIN, config.COLOR_COLD, config.COLOR_HOT)
                st.markdown(f"""<div style="background-color: {bg_color}; border-radius: 7px; padding: 20px; text-align: center; height: 100%;"><p style="font-size: 16px; color: {text_color}; margin-bottom: 5px;">ACTIVE USERS (5 MIN)</p><p style="font-size: 32px; font-weight: bold; color: {text_color}; margin: 0;">{data['active_users_5min']}</p></div>""", unsafe_allow_html=True)
            with top_col2:
                bg_color, text_color = get_heatmap_color_and_text(data['active_users_30min'], config.TARGET_USERS_30MIN, config.COLOR_COLD, config.COLOR_HOT)
                st.markdown(f"""<div style="background-color: {bg_color}; border-radius: 7px; padding: 20px; text-align: center; height: 100%;"><p style="font-size: 16px; color: {text_color}; margin-bottom: 5px;">ACTIVE USERS (30 MIN)</p><p style="font-size: 32px; font-weight: bold; color: {text_color}; margin: 0;">{data['active_users_30min']}</p></div>""", unsafe_allow_html=True)
            with top_col3:
                bg_color, text_color = get_heatmap_color_and_text(data['total_views'], config.TARGET_VIEWS_30MIN, config.COLOR_COLD, config.COLOR_HOT)
                st.markdown(f"""<div style="background-color: {bg_color}; border-radius: 7px; padding: 20px; text-align: center; height: 100%;"><p style="font-size: 16px; color: {text_color}; margin-bottom: 5px;">VIEWS (30 MIN)</p><p style="font-size: 32px; font-weight: bold; color: {text_color}; margin: 0;">{data['total_views']}</p></div>""", unsafe_allow_html=True)
            st.divider()
            bottom_col1, bottom_col2 = st.columns(2)
            with bottom_col1:
                st.markdown(f"""<div style="background-color: #025402; border: 2px solid #057805; border-radius: 7px; padding: 20px; text-align: center; height: 100%;"><p style="font-size: 16px; color: #b0b0b0; margin-bottom: 5px;">PURCHASES (30 MIN)</p><p style="font-size: 32px; font-weight: bold; color: #23d123; margin: 0;">{data['purchase_count_30min']}</p></div>""", unsafe_allow_html=True)
            with bottom_col2:
                cr = (data['purchase_count_30min'] / data['active_users_30min'] * 100) if data['active_users_30min'] > 0 else 0
                st.markdown(f"""<div style="background-color: #013254; border: 2px solid #0564a8; border-radius: 7px; padding: 20px; text-align: center; height: 100%;"><p style="font-size: 16px; color: #b0b0b0; margin-bottom: 5px;">CONVERSION RATE (30 MIN)</p><p style="font-size: 32px; font-weight: bold; color: #23a7d1; margin: 0;">{cr:.2f}%</p></div>""", unsafe_allow_html=True)
            
            self._render_realtime_trend_chart(data, localized_fetch_time, refresh_interval, data['purchase_events'])
            self._render_per_minute_chart(data['per_min_df'])
            st.divider()
            st.subheader("Page and screen in last 30 minutes")
            self._render_realtime_dataframe(data['final_pages_df'], effective_user_info, selected_tz)

            if st.session_state['user_info']['role'] == 'admin' and not (effective_user_info['role'] == 'employee'):
                self._render_quota_monitoring(data['quota_details'])
            if debug_mode:
                self._render_realtime_debug_section(data['debug_data'], data['quota_details'])

        # --- BẮT ĐẦU SỬA LỖI ---
        # Thay thế vòng lặp for bằng vòng lặp while để kiểm soát việc đếm ngược
        seconds_left = refresh_interval
        while seconds_left > 0:
            timer_placeholder.markdown(f'<p style="color:green;"><b>Next refresh in: {int(seconds_left)} seconds...</b></p>', unsafe_allow_html=True)
            
            # Ngủ 5 giây, hoặc ít hơn nếu thời gian còn lại nhỏ hơn 5 giây
            sleep_duration = min(seconds_left, 5)
            time.sleep(sleep_duration)
            
            # Trừ đi chính xác số giây đã ngủ
            seconds_left -= sleep_duration
        
        timer_placeholder.markdown(f'<p style="color:blue;"><b>Refreshing now...</b></p>', unsafe_allow_html=True)
        st.rerun()
        # --- KẾT THÚC SỬA LỖI ---

    def _render_realtime_trend_chart(self, data, localized_fetch_time, refresh_interval, purchase_events):
        if not data['final_pages_df'].empty:
            marketer_summary = data['final_pages_df'].groupby('Marketer')['Active Users'].sum()
            current_snapshot = marketer_summary.to_dict()
        else:
            current_snapshot = {}
        st.session_state.realtime_history.append({'timestamp': localized_fetch_time, **current_snapshot})
        time_window_minutes = st.session_state.get('time_window', 60)
        MAX_HISTORY_POINTS = int((time_window_minutes * 60) / refresh_interval)
        if len(st.session_state.realtime_history) > MAX_HISTORY_POINTS:
            st.session_state.realtime_history = st.session_state.realtime_history[-MAX_HISTORY_POINTS:]
        history_df = pd.DataFrame(st.session_state.realtime_history).set_index('timestamp')
        history_df_melted = history_df.reset_index().melt(id_vars='timestamp', var_name='Marketer', value_name='Active Users').dropna(subset=['Active Users'])
        
        st.divider()
        st.subheader(f"Active Users Trend by Marketer (Last {time_window_minutes} minutes)")
        if not history_df_melted.empty:
            fig_trend = px.line(history_df_melted, x='timestamp', y='Active Users', color='Marketer', template='plotly_dark', color_discrete_sequence=px.colors.qualitative.Plotly)
            fig_trend.update_traces(line=dict(width=3))
            fig_trend.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', yaxis=dict(gridcolor='rgba(255,255,255,0.1)'), legend_title_text='', legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1), hovermode="x unified")
            
            if not purchase_events.empty and not history_df_melted.empty:
                purchase_events['created_at_local'] = purchase_events['created_at'].dt.tz_convert(localized_fetch_time.tzinfo)
                merged_events = pd.merge_asof(
                    purchase_events.sort_values('created_at_local'),
                    history_df_melted.sort_values('timestamp'),
                    left_on='created_at_local',
                    right_on='timestamp',
                    by='Marketer',
                    direction='nearest'
                )
                for _, event in merged_events.iterrows():
                    try:
                        marker_color = fig_trend.data[[trace.name for trace in fig_trend.data].index(event['Marketer'])].line.color
                        fig_trend.add_trace(go.Scatter(
                            x=[event['created_at_local']],
                            y=[event['Active Users']],
                            mode='text',
                            text=[f"<b>{event['ProductSymbol']}{event['Marketer']}</b>"],
                            textposition='top center',
                            textfont=dict(size=12, color=marker_color),
                            hoverinfo='none',
                            showlegend=False
                        ))
                    except (ValueError, IndexError):
                        pass
            
            st.plotly_chart(fig_trend, use_container_width=True)
        else:
            st.write("Collecting data for trend chart... Please wait for the next refresh.")

    def _render_per_minute_chart(self, per_min_df):
        if not per_min_df.empty and per_min_df["Active Users"].sum() > 0:
            st.subheader("Total Active Users per Minute (All Marketers)")
            fig_bar = px.bar(per_min_df, x="Time", y="Active Users", template="plotly_dark", color_discrete_sequence=['#4A90E2'])
            fig_bar.update_layout(xaxis_title=None, yaxis_title="Active Users", plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', yaxis=dict(gridcolor='rgba(255,255,255,0.1)'), xaxis=dict(tickangle=-90))
            st.plotly_chart(fig_bar, use_container_width=True)
            
    def _render_realtime_dataframe(self, pages_df_full, effective_user_info, selected_tz):
        can_view_all = (effective_user_info['role'] == 'admin' or effective_user_info.get('can_view_all_realtime_data', False))
        pages_to_display = pages_df_full
        if not can_view_all:
            marketer_id = effective_user_info['marketer_id']
            pages_to_display = pages_df_full[pages_df_full['Marketer'] == marketer_id]
        if not pages_to_display.empty:
            styler = pages_to_display.style.format({
                'CR': "{:.2f}%", 
                'Revenue': "${:,.2f}"
            })
            styler.apply(lambda x: x.map(highlight_metrics) if x.name in ['Purchases', 'Revenue', 'CR', 'Last Purchase'] else [''] * len(x), axis=0)
            st.dataframe(
                styler,
                use_container_width=True,
                column_config={"Page Title and Screen Class": st.column_config.TextColumn("Page Title", width="large")}
            )
        else:
            st.write("No data available for your user.")
    
    def _render_quota_monitoring(self, quota_details):
        st.divider()
        st.subheader("📊 API Quota Monitoring")
        tokens_day_consumed = quota_details.get("tokens_per_day", {}).get("consumed", 0)
        tokens_day_remaining = quota_details.get("tokens_per_day", {}).get("remaining", "N/A")
        tokens_hour_consumed = quota_details.get("tokens_per_hour", {}).get("consumed", 0)
        tokens_hour_remaining = quota_details.get("tokens_per_hour", {}).get("remaining", "N/A")
        q_col1, q_col2 = st.columns(2)
        with q_col1:
            st.metric("Hourly Tokens", f"{tokens_hour_consumed} / {config.HOURLY_TOKEN_QUOTA}")
            st.caption(f"Used in the current hour. Remaining: {tokens_hour_remaining}")
            render_progress_bar(tokens_hour_consumed, config.HOURLY_TOKEN_QUOTA)
        with q_col2:
            st.metric("Daily Tokens", f"{tokens_day_consumed} / {config.DAILY_TOKEN_QUOTA}")
            st.caption(f"Total used today. Resets daily at 14:00 (VN Time). Remaining: {tokens_day_remaining}")
            render_progress_bar(tokens_day_consumed, config.DAILY_TOKEN_QUOTA)

    def _render_realtime_debug_section(self, debug_data, quota_details):
        st.divider()
        st.subheader("🕵️‍♂️ Debug Mode: Realtime Data Flow")
        with st.expander("1. Raw Data from APIs"):
            st.write("GA (Traffic):"); st.dataframe(debug_data['ga_raw'])
            st.write("Shopify (Purchases):"); st.dataframe(debug_data['shopify_raw'])
        with st.expander("2. Processed Data (before merge)"):
            st.write("GA Processed:"); st.dataframe(debug_data['ga_processed'])
            if 'shopify_grouped' in debug_data:
                st.write("Shopify Processed & Grouped:")
                st.dataframe(debug_data['shopify_grouped'])
        with st.expander("3. Merged Data"):
            st.dataframe(debug_data['merged'])
        with st.expander("4. API Quota Details (from this request)"):
            st.json(quota_details)

    def render_historical_report(self, effective_user_info, debug_mode):
        st.title("📊 Page Performance Report")
        col1, col2 = st.columns(2)
        with col1:
            date_options = ["Today", "Yesterday", "This Week", "Last Week", "Last 7 days", "Last 30 days", "Custom Range..."]
            selected_option = st.selectbox("Select Date Range", options=date_options, index=5)
        with col2:
            segment_option = st.selectbox("Segment by:", ("Summary", "By Day", "By Week"))
        min_purchases = 1 if segment_option != 'Summary' else 0
        if segment_option != 'Summary':
            min_purchases = st.number_input("Minimum Purchases to Display", min_value=0, value=1, step=1)
        start_date, end_date = self._get_date_range_from_selection(selected_option)
        if start_date and end_date:
            st.markdown(f"**Displaying data for:** `{start_date.strftime('%b %d, %Y')}{' - ' + end_date.strftime('%b %d, %Y') if start_date != end_date else ''}`")
            with st.spinner("Fetching data from GA & Shopify..."):
                all_data_df, debug_data = self.processor.get_processed_historical_data(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"), segment_option)
                if not all_data_df.empty:
                    if segment_option != 'Summary':
                        all_data_df = all_data_df[all_data_df['Purchases'] >= min_purchases]
                    data_to_display = pd.DataFrame()
                    if effective_user_info['role'] == 'admin':
                        data_to_display = all_data_df
                    else:
                        marketer_id = effective_user_info['marketer_id']
                        data_to_display = all_data_df[all_data_df['Marketer'] == marketer_id]
                    if not data_to_display.empty:
                        if segment_option == "Summary":
                            total_sessions = data_to_display['Sessions'].sum()
                            total_users = data_to_display['Users'].sum()
                            total_purchases = data_to_display['Purchases'].sum()
                            total_revenue = data_to_display['Revenue'].sum()
                            total_session_cr = (total_purchases / total_sessions * 100) if total_sessions > 0 else 0
                            total_user_cr = (total_purchases / total_users * 100) if total_users > 0 else 0
                            total_row = pd.DataFrame([{"Page Title": "Total", "Marketer": "", "Sessions": total_sessions, "Users": total_users, "Purchases": total_purchases, "Revenue": total_revenue, "Session CR": total_session_cr, "User CR": total_user_cr}])
                            data_to_display = pd.concat([total_row, data_to_display], ignore_index=True)
                        st.dataframe(
                            data_to_display.style.format({'Revenue': "${:,.2f}", 'Session CR': "{:.2f}%", 'User CR': "{:.2f}%"}).apply(lambda x: x.map(highlight_metrics) if x.name in ['Purchases', 'Revenue', 'Session CR', 'User CR'] else [''] * len(x), axis=0),
                            use_container_width=True,
                            column_config={"Page Title": st.column_config.TextColumn(width="large")}
                        )
                    else:
                        st.write("No data found for your user/filters in the selected date range.")
                    if debug_mode:
                        st.divider()
                        st.subheader(f"🕵️‍♂️ Debug Mode: Page Performance Data Flow ({segment_option})")
                        with st.expander("1. Raw Google Analytics Data"): st.dataframe(debug_data['ga_raw'])
                        with st.expander("2. Raw Shopify Data"): st.dataframe(debug_data['shopify_raw'])
                        with st.expander("3. Merged Data (Before final grouping)"): st.dataframe(debug_data['merged'])
                        with st.expander("4. Final Data (Grouped, with Marketer, Sorted)"): st.dataframe(debug_data['final'])
                else:
                    st.write("No page data found with sessions in the selected date range.")
    
    def _get_date_range_from_selection(self, selection: str):
        if selection == "Custom Range...":
            today = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).date()
            selected_range = st.date_input("Select your custom date range", value=(today - timedelta(days=6), today), min_value=today - timedelta(days=365), max_value=today, format="YYYY/MM/DD")
            if len(selected_range) == 2:
                return selected_range[0], selected_range[1]
            return None, None
        today = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).date()
        if selection == "Today": start_date, end_date = today, today
        elif selection == "Yesterday": start_date, end_date = today - timedelta(days=1), today - timedelta(days=1)
        elif selection == "This Week": start_date, end_date = today - timedelta(days=today.weekday()), today
        elif selection == "Last Week": end_date = today - timedelta(days=today.weekday() + 1); start_date = end_date - timedelta(days=6)
        elif selection == "Last 7 days": start_date, end_date = today - timedelta(days=6), today
        elif selection == "Last 30 days": start_date, end_date = today - timedelta(days=29), today
        else: start_date, end_date = today, today
        return start_date, end_date