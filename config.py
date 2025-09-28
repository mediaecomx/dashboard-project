# FILE: config.py

import json
import os
import streamlit as st
from google.oauth2 import service_account
from supabase import create_client, Client
import yaml
from yaml.loader import SafeLoader

class AppConfig:
    def __init__(self) -> None:
        # --- CẤU HÌNH CHUNG ---
        self.PROPERTY_ID = "506473229"
        self.HOURLY_TOKEN_QUOTA = 5000
        self.DAILY_TOKEN_QUOTA = 25000
        self.TARGET_USERS_5MIN = 50
        self.TARGET_USERS_30MIN = 200
        self.TARGET_VIEWS_30MIN = 1000
        self.COLOR_COLD = (40, 40, 60)
        self.COLOR_HOT = (255, 190, 0)

        self.TIMEZONE_MAPPINGS = {
            "Viet Nam (UTC+7)": "Asia/Ho_Chi_Minh",
            "New York (UTC-4)": "America/New_York",
            "Chicago (UTC-5)": "America/Chicago",
            "Denver (UTC-6)": "America/Denver",
            "Los Angeles (UTC-7)": "America/Los_Angeles",
            "Anchorage (UTC-8)": "America/Anchorage",
            "Honolulu (UTC-10)": "Pacific/Honolulu",
        }

        # --- MAPPING TÊN MARKETER / PAGE TITLE ---
        self.page_title_map = {}
        self.landing_page_map = {}
        self.SYMBOLS = []
        # --- BẮT ĐẦU THAY ĐỔI ---
        # Thêm biến để lưu ánh xạ sản phẩm -> biểu tượng
        self.product_to_symbol_map = {}
        # --- KẾT THÚC THAY ĐỔI ---
        try:
            with open("marketer_mapping.json", "r", encoding="utf-8") as f:
                full_mapping = json.load(f)
            self.page_title_map = full_mapping.get("page_title_mapping", {})
            self.landing_page_map = full_mapping.get("landing_page_mapping", {})
            # --- BẮT ĐẦU THAY ĐỔI ---
            # Đọc thêm ánh xạ sản phẩm
            self.product_to_symbol_map = full_mapping.get("product_to_symbol_mapping", {})
            # --- KẾT THÚC THAY ĐỔI ---
            self.SYMBOLS = sorted(list(self.page_title_map.keys()), key=len, reverse=True)
        except Exception:
            self.page_title_map = {}
            self.landing_page_map = {}
            self.SYMBOLS = []
            self.product_to_symbol_map = {}

        # --- AVATAR MẶC ĐỊNH ---
        self.default_avatar_url = "https://i.ibb.co/wN8TsVMW/avatar.jpg"

        # --- GA CREDENTIALS ---
        self.ga_credentials = None
        try:
            if "google_credentials" in st.secrets:
                google_creds_dict = dict(st.secrets["google_credentials"])
                google_creds_dict["private_key"] = google_creds_dict["private_key"].replace("\\n", "\n")
                self.ga_credentials = service_account.Credentials.from_service_account_info(
                    google_creds_dict, scopes=["https://www.googleapis.com/auth/analytics.readonly"]
                )
        except Exception:
            self.ga_credentials = None
        
        # --- SHOPIFY CREDENTIALS ---
        self.shopify_creds = st.secrets.get("shopify_credentials", {})

        # --- SUPABASE ---
        self.supabase: Client | None = None
        try:
            if "supabase" in st.secrets:
                url = st.secrets["supabase"]["url"]
                key = st.secrets["supabase"]["service_role_key"]
                self.supabase = create_client(url, key)
        except Exception:
            self.supabase = None

        # --- CLOUDINARY ---
        self.cloudinary_cloud_name = st.secrets.get("cloudinary", {}).get("cloud_name")
        self.cloudinary_upload_preset = st.secrets.get("cloudinary", {}).get("upload_preset")

        # --- AUTHENTICATOR CONFIG ---
        self.auth_config = self.prepare_auth_config()
        self.users_details = st.secrets.get("users", {})

    def prepare_auth_config(self):
        users_from_secrets = st.secrets.get("users", {})
        credentials = {"usernames": {}}
        for key, user_data in users_from_secrets.items():
            username = user_data.get("username")
            if username:
                credentials["usernames"][username] = {
                    "email": user_data.get("email", f"{username}@example.com"),
                    "name": user_data.get("name", username.capitalize()),
                    "password": user_data.get("password")
                }
        auth_config = {
            "credentials": credentials,
            "cookie": {
                "name": st.secrets.get("cookie", {}).get("name", "dashboard_cookie"),
                "key": st.secrets.get("cookie", {}).get("encrypt_key", "default_secret_key"),
                "expiry_days": 15
            },
            "preauthorized": {
                "emails": []
            }
        }
        return auth_config

    def get_user_details_by_username(self, username: str):
        for _, user_info in self.users_details.items():
            if user_info.get("username") == username:
                return user_info
        return None

# Singleton config
config = AppConfig()