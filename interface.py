# FILE: interface.py

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import time
import pytz
import requests
from datetime import datetime, timedelta, timezone
from config import get_config
from streamlit.components.v1 import html
import json

# --- THAY ĐỔI: Bỏ thư viện random vì không còn dọn dẹp ngẫu nhiên ---

@st.cache_data(ttl=60)
def load_history_from_supabase(time_window_hours):
    """
    Tải dữ liệu lịch sử từ Supabase trong khoảng thời gian quy định (tính bằng giờ).
    """
    try:
        config = get_config()
        # --- THAY ĐỔI: Sử dụng hours thay vì minutes ---
        start_time = datetime.now(timezone.utc) - timedelta(hours=time_window_hours)
        
        response = config.supabase.table("realtime_history").select("timestamp, snapshot_data") \
            .gte("timestamp", start_time.isoformat()) \
            .order("timestamp", desc=False) \
            .execute()

        if not response.data:
            return pd.DataFrame()

        records = []
        for row in response.data:
            ts = pd.to_datetime(row['timestamp'])
            snapshot = row['snapshot_data']
            if snapshot:
                for marketer, users in snapshot.items():
                    records.append({
                        'timestamp': ts,
                        'Marketer': marketer,
                        'Active Users': users
                    })
        
        return pd.DataFrame(records)

    except Exception as e:
        print(f"Error loading history from Supabase: {e}")
        return pd.DataFrame()

def save_snapshot_to_supabase(snapshot_data, timestamp):
    """
    Lưu một bản ghi dữ liệu mới vào Supabase.
    """
    try:
        config = get_config()
        config.supabase.table("realtime_history").insert({
            "timestamp": timestamp.isoformat(),
            "snapshot_data": snapshot_data
        }).execute()
    except Exception as e:
        print(f"Error saving snapshot to Supabase: {e}")

# --- THAY ĐỔI: Xóa hàm cleanup_old_history_supabase() vì đã dùng Cron Job ---

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
    
@st.cache_data(ttl=60)
def get_app_settings():
    """Đọc cài đặt toàn cục của ứng dụng từ Supabase."""
    try:
        config = get_config()
        # Lấy tất cả các cột, bao gồm cả các cột mới
        response = config.supabase.table("app_settings").select("*").eq("id", 1).single().execute()
        if response.data:
            return response.data
    except Exception as e:
        st.error(f"Could not load app settings: {e}")
    # Trả về giá trị mặc định nếu có lỗi, bao gồm cả các giá trị mới
    return {
        "enable_notifications": True,
        "enable_confetti": True,
        "confetti_effect": "realistic_look",
        "confetti_duration_ms": 5000,
        "toast_duration_ms": 8000,
        "toast_sound_url": "",
        "confetti_sound_url": "",
        "refresh_interval": 75,
        "time_window_hours": 3
    }

def admin_settings_ui(current_settings):
    """Hiển thị giao diện tùy chỉnh cho Admin trên sidebar."""
    st.divider()
    st.subheader("⚙️ App Settings")
    
    with st.form("app_settings_form"):
        st.write("Control global settings for all users.")
        
        # --- BẮT ĐẦU TÍNH NĂNG MỚI: Cài đặt chung ---
        st.write("**Realtime Dashboard Settings**")
        
        # Lấy giá trị hiện tại từ settings, nếu không có thì dùng mặc định
        current_interval = current_settings.get("refresh_interval", 75)
        new_interval = st.number_input(
            "Refresh Interval (seconds)", 
            min_value=30, 
            value=current_interval, 
            step=15,
            help="How often the dashboard should refresh for all users."
        )

        time_window_options = [1, 3, 6, 12, 24]
        current_window_hours = current_settings.get("time_window_hours", 3)
        try:
            default_index = time_window_options.index(current_window_hours)
        except ValueError:
            default_index = 1 # Mặc định là 3 giờ

        selected_window_hours = st.selectbox(
            "Chart Time Window (hours)", 
            options=time_window_options, 
            index=default_index,
            help="The time range displayed on the trend chart."
        )
        
        st.divider()
        st.write("**Notification & Effect Settings**")
        # --- KẾT THÚC TÍNH NĂNG MỚI ---

        enable_notifications = st.toggle(
            "Enable Sale Notifications", 
            value=current_settings.get("enable_notifications", True)
        )
        enable_confetti = st.toggle(
            "Enable Confetti Effect", 
            value=current_settings.get("enable_confetti", True)
        )
        
        effects = {
            "Mưa Rơi (Realistic)": "realistic_look",
            "Bùng Nổ (Big Celebration)": "celebration",
            "Mưa Sao Băng (Stars)": "stars",
            "Pháo Hoa (Fireworks)": "fireworks",
            "Tuyết Rơi (Snow)": "snow"
        }
        effect_options = list(effects.keys())
        try:
            current_effect_name = list(effects.keys())[list(effects.values()).index(current_settings.get("confetti_effect"))]
            default_index = effect_options.index(current_effect_name)
        except ValueError:
            default_index = 0
            
        selected_effect_name = st.selectbox(
            "Confetti Style", 
            options=effect_options,
            index=default_index
        )
        
        confetti_duration = st.slider(
            "Effect & Toast Duration (seconds)", 
            min_value=3, max_value=30, 
            value=int(current_settings.get("confetti_duration_ms", 5000) / 1000),
            step=1
        )

        submitted = st.form_submit_button("Save All Settings", use_container_width=True)
        
        if submitted:
            # Gom tất cả các cài đặt mới vào một dictionary để update
            new_settings = {
                "refresh_interval": new_interval,
                "time_window_hours": selected_window_hours,
                "enable_notifications": enable_notifications,
                "enable_confetti": enable_confetti,
                "confetti_effect": effects[selected_effect_name],
                "confetti_duration_ms": confetti_duration * 1000,
                "toast_duration_ms": confetti_duration * 1000,
                "updated_at": datetime.now().isoformat()
            }
            try:
                config = get_config()
                # Update tất cả các cài đặt lên Supabase một lần duy nhất
                config.supabase.table("app_settings").update(new_settings).eq("id", 1).execute()
                st.success("Global settings updated successfully!")
                st.cache_data.clear()
                time.sleep(1)
                st.rerun()
            except Exception as e:
                st.error(f"Failed to save settings: {e}")

def render_realtime_sales_listener(settings):
    config = get_config()
    supabase_url = config.supabase_url
    supabase_anon_key = config.supabase_anon_key

    if not supabase_url or not supabase_anon_key:
        return

    supabase_url_json = json.dumps(supabase_url or "")
    supabase_anon_key_json = json.dumps(supabase_anon_key or "")
    
    settings_json = json.dumps(settings)

    listener_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
    <meta charset="utf-8" />
    <style>
      #toast-container {{
        position: fixed;
        bottom: 24px;
        right: 24px;
        z-index: 1000000;
        display: flex;
        flex-direction: column-reverse;
        gap: 12px;
        align-items: flex-end;
      }}
      .sale-toast {{
        background-image: linear-gradient(145deg, #00b084, #028a68);
        color: #fff;
        padding: 20px 28px;
        border-radius: 12px;
        box-shadow: 0 12px 35px rgba(0,0,0,0.3);
        font: 18px/1.4 system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
        pointer-events: auto;
        border: 1px solid rgba(255, 255, 255, 0.2);
        animation: slideInUp 300ms ease-out forwards;
      }}
      @keyframes slideInUp {{ from {{ transform: translateY(100%); opacity: 0; }} to {{ transform: translateY(0); opacity: 1; }} }}
      @keyframes fadeOut {{ from {{ opacity: 1; }} to {{ opacity: 0; visibility: hidden; }} }}
      .sale-toast strong {{ font-weight: 700; font-size: 20px; }}
      .sale-toast span {{ display: block; font-size: 15px; opacity: 0.85; margin-top: 5px; }}
    </style>
    </head>
    <body>
    <script>
      try {{
        const frame = window.frameElement;
        if (frame) {{
          frame.style.position = "fixed"; frame.style.inset = "0";
          frame.style.width = "100vw"; frame.style.height = "100vh";
          frame.style.border = "none"; frame.style.pointerEvents = "none";
          frame.style.zIndex = "100000";
        }}
      }} catch (e) {{}}
    </script>
    <script src="https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2"></script>
    <script src="https://cdn.jsdelivr.net/npm/canvas-confetti@1.9.2/dist/confetti.browser.min.js"></script>
    <script>
      const APP_SETTINGS = {settings_json};
      const SUPABASE_URL = {supabase_url_json};
      const SUPABASE_ANON = {supabase_anon_key_json};

      let toastContainer = document.getElementById('toast-container');
      if (!toastContainer) {{
        toastContainer = document.createElement('div');
        toastContainer.id = 'toast-container';
        document.body.appendChild(toastContainer);
      }}

      function playSound(url) {{
        if (url) {{
          try {{
            const audio = new Audio(url);
            audio.play().catch(e => console.warn("Audio play was prevented by browser policy.", e));
          }} catch (e) {{
            console.error("Error creating or playing audio:", e);
          }}
        }}
      }}

      function shootConfetti(durationMs, effectName) {{
        if (!APP_SETTINGS.enable_confetti) return;
        
        playSound(APP_SETTINGS.confetti_sound_url);

        const animationEnd = Date.now() + durationMs;
        const defaults = {{ startVelocity: 30, spread: 360, ticks: 60, zIndex: 1000001 }};
        
        function randomInRange(min, max) {{ return Math.random() * (max - min) + min; }}
        
        const interval = setInterval(function() {{
            const timeLeft = animationEnd - Date.now();
            if (timeLeft <= 0) {{ return clearInterval(interval); }}
            const particleCount = 50 * (timeLeft / durationMs);

            switch(effectName) {{
                case 'celebration': confetti({{ ...defaults, particleCount, origin: {{ x: randomInRange(0.1, 0.9), y: Math.random() - 0.2 }} }}); break;
                case 'stars': confetti({{ ...defaults, particleCount: 1, shapes: ['star'], gravity: randomInRange(0.4, 0.6), scalar: randomInRange(0.4, 1), origin: {{ x: Math.random(), y: -0.1 }} }}); break;
                case 'fireworks': const x = Math.random(); const y = Math.random(); confetti({{ ...defaults, particleCount, origin: {{ x, y }}, angle: randomInRange(0, 360), spread: 100, decay: 0.9 }}); break;
                case 'snow': confetti({{ ...defaults, particleCount: 1, shapes: ['circle'], colors: ['#FFFFFF'], origin: {{ x: Math.random(), y: -0.1 }}, gravity: 0.1, ticks: 200 }}); break;
                case 'realistic_look': default: confetti({{ ...defaults, particleCount, origin: {{ x: randomInRange(0.1, 0.3), y: Math.random() - 0.2 }} }}); confetti({{ ...defaults, particleCount, origin: {{ x: randomInRange(0.7, 0.9), y: Math.random() - 0.2 }} }}); break;
            }}
        }}, 250);
      }}

      function showToastAndConfetti(row) {{
        if (!APP_SETTINGS.enable_notifications) return;
        
        playSound(APP_SETTINGS.toast_sound_url);
        
        const div = document.createElement("div");
        const revenue = Number(row.revenue || 0).toFixed(2);
        const title = row.product_title || "New Shopify order";
        const marketer = row.marketer || "Marketing";
        const symbol = row.product_symbol || "🛒";
        div.className = "sale-toast";
        div.innerHTML = `<strong>${{symbol}} ${{marketer}}</strong> &bull; $${{revenue}}<br/><span>${{title}}</span>`;
        
        document.getElementById('toast-container').appendChild(div);
        
        shootConfetti(APP_SETTINGS.confetti_duration_ms, APP_SETTINGS.confetti_effect);
        
        setTimeout(() => {{
          div.style.animation = "fadeOut 650ms ease-in forwards";
          setTimeout(() => div.remove(), 700);
        }}, APP_SETTINGS.toast_duration_ms);
      }}

      const KEY = "notified_order_ids_v2";
      const load = () => {{ try {{ const m = JSON.parse(localStorage.getItem(KEY) || "{{}}"); const now = Date.now(); for (const k of Object.keys(m)) if (now - m[k] > 86400000) delete m[k]; localStorage.setItem(KEY, JSON.stringify(m)); return m; }} catch {{ return {{}}; }} }};
      const seen = id => !!load()[id];
      const mark = id => {{ const m = load(); m[id] = Date.now(); localStorage.setItem(KEY, JSON.stringify(m)); }};

      if (SUPABASE_URL && SUPABASE_ANON) {{
        const client = window.supabase.createClient(SUPABASE_URL, SUPABASE_ANON, {{ realtime: {{ params: {{ eventsPerSecond: 5 }} }} }});
        
        client.channel("realtime_sales_notifications")
          .on("postgres_changes", {{ event: "INSERT", schema: "public", table: "sales_events" }}, (payload) => {{
            const row = payload?.new || {{}};
            const id = String(row.order_id || "");
            if (!id || seen(id)) return;
            showToastAndConfetti(row);
            mark(id);
          }})
          .subscribe();
      }}
    </script>
    </body>
    </html>
    """
    html(listener_html, height=0)

class DashboardUI:
    def __init__(self, auth, data_processor, config):
        self.auth = auth
        self.processor = data_processor
        self.config = config
        if 'property_name' not in st.session_state:
            st.session_state.property_name = self.config.DEFAULT_PROPERTY_NAME

    def render_sidebar(self):
        with st.sidebar:
            user_info = st.session_state['user_info']
            avatar_url = user_info.get("avatar_url") or self.config.default_avatar_url
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

            # Lấy cài đặt chung NGAY LẬP TỨC để sử dụng cho toàn bộ app
            app_settings = get_app_settings()

            if user_info['role'] == 'admin':
                st.divider()
                st.subheader("Admin Controls")
                
                property_names = list(self.config.AVAILABLE_PROPERTIES.keys())
                
                try:
                    current_index = property_names.index(st.session_state.property_name)
                except ValueError:
                    current_index = 0

                selected_property_name = st.selectbox(
                    "Select Google Analytics Property", 
                    options=property_names,
                    index=current_index
                )
                
                if selected_property_name != st.session_state.property_name:
                    st.session_state.property_name = selected_property_name
                    st.cache_data.clear() 
                    st.rerun() 
            
            st.info(f"Viewing data for: **{st.session_state.property_name}**")

            # Chỉ hiển thị form cài đặt cho admin
            if user_info['role'] == 'admin':
                admin_settings_ui(app_settings)
            
            impersonating = False
            effective_user_info = user_info
            if user_info['role'] == 'admin':
                st.divider()
                employee_details = {v['username']: v for k, v in self.config.users_details.items() if v.get('role') == 'employee'}
                options = ["None (View as Admin)"] + list(employee_details.keys())
                selected_user_name = st.selectbox("Impersonate User", options=options)
                if selected_user_name != "None (View as Admin)":
                    impersonating = True
                    effective_user_info = self.config.get_user_details_by_username(selected_user_name)
                    st.info(f"Viewing as **{selected_user_name}**")
            
            debug_mode = st.checkbox("Enable Debug Mode") if user_info['role'] == 'admin' and not impersonating else False
        
        return page, effective_user_info, debug_mode, app_settings

    def render_realtime_dashboard(self, effective_user_info, debug_mode, app_settings):
        st.title("🚀 Realtime Dashboard")
        
        render_realtime_sales_listener(app_settings)
        
        current_property_id = self.config.AVAILABLE_PROPERTIES[st.session_state.property_name]

        # --- THAY ĐỔI: Xóa các widget cài đặt khỏi sidebar vì đã chuyển vào form của admin ---
        with st.sidebar:
            st.divider()
            st.subheader("Dashboard Settings")
            selected_tz_name = st.selectbox("Select Timezone", options=list(self.config.TIMEZONE_MAPPINGS.keys()), key="timezone_selector")

        # --- THAY ĐỔI: Lấy refresh_interval từ app_settings ---
        refresh_interval = app_settings.get('refresh_interval', 75)
        
        selected_tz = pytz.timezone(self.config.TIMEZONE_MAPPINGS[selected_tz_name])
        timer_placeholder, placeholder = st.empty(), st.empty()

        with placeholder.container():
            data = self.processor.get_processed_realtime_data(current_property_id, selected_tz)
            localized_fetch_time = data['fetch_time'].astimezone(selected_tz)
            st.markdown(f"*Last update: {localized_fetch_time.strftime('%Y-%m-%d %H:%M:%S')}*")
            top_col1, top_col2, top_col3 = st.columns(3)
            with top_col1:
                bg_color, text_color = get_heatmap_color_and_text(data['active_users_5min'], self.config.TARGET_USERS_5MIN, self.config.COLOR_COLD, self.config.COLOR_HOT)
                st.markdown(f"""<div style="background-color: {bg_color}; border-radius: 7px; padding: 20px; text-align: center; height: 100%;"><p style="font-size: 16px; color: {text_color}; margin-bottom: 5px;">ACTIVE USERS (5 MIN)</p><p style="font-size: 32px; font-weight: bold; color: {text_color}; margin: 0;">{data['active_users_5min']}</p></div>""", unsafe_allow_html=True)
            with top_col2:
                bg_color, text_color = get_heatmap_color_and_text(data['active_users_30min'], self.config.TARGET_USERS_30MIN, self.config.COLOR_COLD, self.config.COLOR_HOT)
                st.markdown(f"""<div style="background-color: {bg_color}; border-radius: 7px; padding: 20px; text-align: center; height: 100%;"><p style="font-size: 16px; color: {text_color}; margin-bottom: 5px;">ACTIVE USERS (30 MIN)</p><p style="font-size: 32px; font-weight: bold; color: {text_color}; margin: 0;">{data['active_users_30min']}</p></div>""", unsafe_allow_html=True)
            with top_col3:
                bg_color, text_color = get_heatmap_color_and_text(data['total_views'], self.config.TARGET_VIEWS_30MIN, self.config.COLOR_COLD, self.config.COLOR_HOT)
                st.markdown(f"""<div style="background-color: {bg_color}; border-radius: 7px; padding: 20px; text-align: center; height: 100%;"><p style="font-size: 16px; color: {text_color}; margin-bottom: 5px;">VIEWS (30 MIN)</p><p style="font-size: 32px; font-weight: bold; color: {text_color}; margin: 0;">{data['total_views']}</p></div>""", unsafe_allow_html=True)
            st.divider()
            bottom_col1, bottom_col2 = st.columns(2)
            with bottom_col1:
                st.markdown(f"""<div style="background-color: #025402; border: 2px solid #057805; border-radius: 7px; padding: 20px; text-align: center; height: 100%;"><p style="font-size: 16px; color: #b0b0b0; margin-bottom: 5px;">PURCHASES (30 MIN)</p><p style="font-size: 32px; font-weight: bold; color: #23d123; margin: 0;">{data['purchase_count_30min']}</p></div>""", unsafe_allow_html=True)
            with bottom_col2:
                cr = (data['purchase_count_30min'] / data['active_users_30min'] * 100) if data['active_users_30min'] > 0 else 0
                st.markdown(f"""<div style="background-color: #013254; border: 2px solid #0564a8; border-radius: 7px; padding: 20px; text-align: center; height: 100%;"><p style="font-size: 16px; color: #b0b0b0; margin-bottom: 5px;">CONVERSION RATE (30 MIN)</p><p style="font-size: 32px; font-weight: bold; color: #23a7d1; margin: 0;">{cr:.2f}%</p></div>""", unsafe_allow_html=True)
            
            self._render_realtime_trend_chart(data, localized_fetch_time, data['purchase_events'], app_settings)
            
            self._render_per_minute_chart(data['per_min_df'])
            st.divider()
            st.subheader("Page and screen in last 30 minutes")
            self._render_realtime_dataframe(data['final_pages_df'], effective_user_info, selected_tz)

            if st.session_state['user_info']['role'] == 'admin' and not (effective_user_info['role'] == 'employee'):
                self._render_quota_monitoring(data['quota_details'])
            if debug_mode:
                self._render_realtime_debug_section(data['debug_data'], data['quota_details'])

        seconds_left = refresh_interval
        while seconds_left > 0:
            timer_placeholder.markdown(f'<p style="color:green;"><b>Next refresh in: {int(seconds_left)} seconds...</b></p>', unsafe_allow_html=True)
            sleep_duration = min(seconds_left, 5)
            time.sleep(sleep_duration)
            seconds_left -= sleep_duration
        
        timer_placeholder.markdown(f'<p style="color:blue;"><b>Refreshing now...</b></p>', unsafe_allow_html=True)
        st.rerun()

    def _render_realtime_trend_chart(self, data, localized_fetch_time, purchase_events, app_settings):
        if not data['final_pages_df'].empty:
            marketer_summary = data['final_pages_df'].groupby('Marketer')['Active Users'].sum()
            current_snapshot = marketer_summary.to_dict()
        else:
            current_snapshot = {}

        if current_snapshot:
            save_snapshot_to_supabase(current_snapshot, localized_fetch_time)

        # --- THAY ĐỔI: Lấy time_window_hours từ app_settings ---
        time_window_hours = app_settings.get('time_window_hours', 3)
        history_df_melted = load_history_from_supabase(time_window_hours)
        
        st.divider()
        st.subheader(f"Active Users Trend by Marketer (Last {time_window_hours} hours)")

        if not history_df_melted.empty:
            history_df_melted['timestamp'] = history_df_melted['timestamp'].dt.tz_convert(localized_fetch_time.tzinfo)

            fig_trend = px.line(history_df_melted, x='timestamp', y='Active Users', color='Marketer', template='plotly_dark', color_discrete_sequence=px.colors.qualitative.Plotly)
            fig_trend.update_traces(line=dict(width=3))
            fig_trend.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', yaxis=dict(gridcolor='rgba(255,255,255,0.1)'), legend_title_text='', legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1), hovermode="x unified")
            
            if not purchase_events.empty:
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
            st.metric("Hourly Tokens", f"{tokens_hour_consumed} / {self.config.HOURLY_TOKEN_QUOTA}")
            st.caption(f"Used in the current hour. Remaining: {tokens_hour_remaining}")
            render_progress_bar(tokens_hour_consumed, self.config.HOURLY_TOKEN_QUOTA)
        with q_col2:
            st.metric("Daily Tokens", f"{tokens_day_consumed} / {self.config.DAILY_TOKEN_QUOTA}")
            st.caption(f"Total used today. Resets daily at 14:00 (VN Time). Remaining: {tokens_day_remaining}")
            render_progress_bar(tokens_day_consumed, self.config.DAILY_TOKEN_QUOTA)

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
        
        current_property_id = self.config.AVAILABLE_PROPERTIES[st.session_state.property_name]

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
                all_data_df, debug_data = self.processor.get_processed_historical_data(current_property_id, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"), segment_option)
                if not all_data_df.empty:
                    if segment_option != 'Summary':
                        all_data_df = all_data_df[all_data_df['Purchases'] >= min_purchases]
                    data_to_display = pd.DataFrame()
                    if effective_user_info['role'] == 'admin':
                        data_to_display = all_data_df
                    else:
                        marketer_id = effective_user_info['marketer_id']
                        employee_df = all_data_df[all_data_df['Marketer'] == marketer_id]
                        data_to_display = employee_df
                    
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
                            data_to_display.style.format({
                                'Revenue': "${:,.2f}",
                                'Session CR': "{:.2f}%",
                                'User CR': "{:.2f}%"
                            }).apply(lambda x: x.map(highlight_metrics) if x.name in ['Purchases', 'Revenue', 'Session CR', 'User CR'] else [''] * len(x), axis=0),
                            use_container_width=True
                        )
                    else: st.write("No data found for your user/filters in the selected date range.")
                    
                    if debug_mode:
                        st.divider()
                        st.subheader(f"🕵️‍♂️ Debug Mode: Page Performance Data Flow ({segment_option})")
                        with st.expander("1. Raw Google Analytics Data"):
                            st.dataframe(debug_data['ga_raw']);
                        with st.expander("2. Raw Shopify Data"):
                            st.dataframe(debug_data['shopify_raw']);
                        with st.expander("3. Merged Data (Before final grouping)"):
                            st.dataframe(debug_data['merged']);
                        with st.expander("4. Final Data (Grouped, with Marketer, Sorted)"):
                            st.dataframe(debug_data['final']);
                else: st.write("No page data found with sessions in the selected date range.")
    
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
