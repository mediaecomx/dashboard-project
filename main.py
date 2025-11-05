# FILE: main.py

import streamlit as st
import streamlit_authenticator as stauth
from config import get_config
from services import GoogleAnalyticsService, ShopifyService
from processor import DataProcessor
from interface import DashboardUI

def fetch_and_set_avatar(username: str, config):
    """
    Hàm này lấy avatar từ Supabase. Nếu người dùng chưa có avatar trên Supabase,
    nó sẽ sử dụng avatar mặc định từ file config.
    Kết quả luôn được cập nhật vào st.session_state['avatar_url'].
    """
    try:
        # Luôn cố gắng truy vấn Supabase để lấy avatar mới nhất của người dùng
        profile_data = config.supabase.table("profiles").select("avatar_url").eq("username", username).single().execute()
        
        # Nếu có dữ liệu và có avatar_url trong đó...
        if profile_data.data and profile_data.data.get('avatar_url'):
            # ... thì cập nhật session state bằng link avatar đó.
            st.session_state['avatar_url'] = profile_data.data.get('avatar_url')
        else:
            # Nếu không, dùng link avatar mặc định từ config.
            st.session_state['avatar_url'] = config.default_avatar_url
    except Exception:
        # Nếu có bất kỳ lỗi nào xảy ra (ví dụ: mất kết nối),
        # an toàn nhất là dùng link mặc định.
        st.session_state['avatar_url'] = config.default_avatar_url

def main():
    """
    Hàm chính để khởi tạo và chạy ứng dụng Dashboard.
    """
    st.set_page_config(layout="wide")
    st.markdown("""<style>.stApp{background-color:black;color:white;}.stMetric{color:white;}.stDataFrame{color:white;}.stPlotlyChart{background-color:transparent;}.block-container{padding-top: 2rem; padding-bottom: 2rem; padding-left: 5rem; padding-right: 5rem;}</style>""", unsafe_allow_html=True)
    
    # Lấy đối tượng config một cách an toàn bằng hàm mới.
    # Lệnh này sẽ chạy sau khi Streamlit đã khởi tạo xong session.
    config = get_config()

    authenticator = stauth.Authenticate(
        config.auth_config['credentials'],
        config.auth_config['cookie']['name'],
        config.auth_config['cookie']['key'],
        config.auth_config['cookie']['expiry_days'],
        config.auth_config['preauthorized']
    )

    authenticator.login()

    if st.session_state["authentication_status"]:
        username = st.session_state["username"]
        user_full_details = config.get_user_details_by_username(username)
        
        if not user_full_details:
            st.error("Không tìm thấy thông tin chi tiết cho người dùng. Vui lòng liên hệ quản trị viên.")
            st.stop()

        # Hàm fetch_and_set_avatar giờ sẽ được gọi mỗi khi rerun
        fetch_and_set_avatar(username, config)
        
        # THAY ĐỔI: Truyền config vào hàm
        fetch_and_set_avatar(username, config)
        st.session_state['user_info'] = {
            "username": username,
            "role": user_full_details.get('role'),
            "marketer_id": user_full_details.get('marketer_id'),
            "can_view_all_realtime_data": user_full_details.get('can_view_all_realtime_data', False),
            "avatar_url": st.session_state.get('avatar_url')
        }
        
        ga_service = GoogleAnalyticsService(config)
        shopify_service = ShopifyService(config)
        data_processor = DataProcessor(ga_service, shopify_service, config)
        ui = DashboardUI(authenticator, data_processor, config)

        page, effective_user_info, debug_mode, app_settings = ui.render_sidebar()
        
        if page == "Realtime Dashboard":
            ui.render_realtime_dashboard(effective_user_info, debug_mode, app_settings)
        elif page == "Landing Page Report":
            ui.render_historical_report(effective_user_info, debug_mode)
        elif page == "Profile":
            st.title("Profile Page")
            st.write("This page is under construction.")

    elif st.session_state["authentication_status"] is False:
        st.error('Tên người dùng hoặc mật khẩu không chính xác')
    
    elif st.session_state["authentication_status"] is None:
        st.warning('Vui lòng nhập tên người dùng và mật khẩu của bạn')

if __name__ == "__main__":
    main()
