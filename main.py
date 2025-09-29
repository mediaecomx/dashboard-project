# FILE: main.py

import streamlit as st
import streamlit_authenticator as stauth
from config import config
from services import GoogleAnalyticsService, ShopifyService
from processor import DataProcessor
from interface import DashboardUI

# --- TIỆN ÍCH LẤY AVATAR ---
def fetch_and_set_avatar(username: str):
    if 'avatar_url' not in st.session_state or not st.session_state['avatar_url']:
        try:
            profile_data = config.supabase.table("profiles").select("avatar_url").eq("username", username).single().execute()
            if profile_data.data and profile_data.data.get('avatar_url'):
                st.session_state['avatar_url'] = profile_data.data.get('avatar_url')
            else:
                st.session_state['avatar_url'] = config.default_avatar_url
        except Exception:
            st.session_state['avatar_url'] = config.default_avatar_url

def main():
    """
    Hàm chính để khởi tạo và chạy ứng dụng Dashboard.
    """
    # Cấu hình trang phải là lệnh Streamlit đầu tiên.
    st.set_page_config(layout="wide")
    st.markdown("""<style>.stApp{background-color:black;color:white;}.stMetric{color:white;}.stDataFrame{color:white;}.stPlotlyChart{background-color:transparent;}.block-container{padding-top: 2rem; padding-bottom: 2rem; padding-left: 5rem; padding-right: 5rem;}</style>""", unsafe_allow_html=True)

    # 1. KHỞI TẠO AUTHENTICATOR
    authenticator = stauth.Authenticate(
        config.auth_config['credentials'],
        config.auth_config['cookie']['name'],
        config.auth_config['cookie']['key'],
        config.auth_config['cookie']['expiry_days'],
        config.auth_config['preauthorized']
    )

    # 2. RENDER FORM ĐĂNG NHẬP VÀ XỬ LÝ TRẠNG THÁI
    authenticator.login()

    if st.session_state["authentication_status"]:
        # --- ĐĂNG NHẬP THÀNH CÔNG ---
        username = st.session_state["username"]
        user_full_details = config.get_user_details_by_username(username)
        
        if not user_full_details:
            st.error("Không tìm thấy thông tin chi tiết cho người dùng. Vui lòng liên hệ quản trị viên.")
            st.stop()
        
        fetch_and_set_avatar(username)
        st.session_state['user_info'] = {
            "username": username,
            "role": user_full_details.get('role'),
            "marketer_id": user_full_details.get('marketer_id'),
            "can_view_all_realtime_data": user_full_details.get('can_view_all_realtime_data', False),
            "avatar_url": st.session_state.get('avatar_url')
        }

        ga_service = GoogleAnalyticsService()
        shopify_service = ShopifyService()
        data_processor = DataProcessor(ga_service, shopify_service)
        ui = DashboardUI(authenticator, data_processor)

        # --- BẮT ĐẦU SỬA LỖI ---
        # Nhận thêm app_settings từ sidebar
        page, effective_user_info, debug_mode, app_settings = ui.render_sidebar()
        
        if page == "Realtime Dashboard":
            # Truyền app_settings vào hàm render
            ui.render_realtime_dashboard(effective_user_info, debug_mode, app_settings)
        # --- KẾT THÚC SỬA LỖI ---
        elif page == "Landing Page Report":
            ui.render_historical_report(effective_user_info, debug_mode)
        elif page == "Profile":
            # Giả sử bạn có hàm này, nếu không, hãy tạm comment lại
            # ui.render_profile_page() 
            st.title("Profile Page")
            st.write("This page is under construction.")


    elif st.session_state["authentication_status"] is False:
        st.error('Tên người dùng hoặc mật khẩu không chính xác')
    
    elif st.session_state["authentication_status"] is None:
        st.warning('Vui lòng nhập tên người dùng và mật khẩu của bạn')

if __name__ == "__main__":
    main()