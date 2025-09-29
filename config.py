# FILE: config.py

import json
import os
import streamlit as st
from google.oauth2 import service_account
from supabase import create_client, Client
import yaml
from yaml.loader import SafeLoader
import toml
import copy

class AppConfig:
    def _deep_merge(self, base: dict, override: dict):
        result = copy.deepcopy(base or {})
        for key, value in (override or {}).items():
            if isinstance(value, dict) and isinstance(result.get(key), dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        return result

    def _load_secrets(self):
        print(f"!!! SCRIPT IS RUNNING FROM THIS DIRECTORY: {os.getcwd()}")
        st_data = {}
        try:
            if hasattr(st, "secrets") and len(st.secrets.items()) > 0:
                print("--- Loading secrets from st.secrets ---")
                st_data = dict(st.secrets)
        except Exception as e:
            print(f"Could not use st.secrets. Reason: {e}")

        file_data = {}
        secrets_path = os.path.join(".streamlit", "secrets.toml")
        if os.path.exists(secrets_path):
            try:
                print("--- Loading secrets from .streamlit/secrets.toml file ---")
                file_data = toml.load(secrets_path)
            except Exception as e:
                print(f"Error loading secrets.toml manually: {e}")

        env_data = { "supabase": { "url": os.environ.get("SUPABASE_URL"), "anon_key": os.environ.get("SUPABASE_ANON_KEY"), "service_role_key": os.environ.get("SUPABASE_SERVICE_ROLE_KEY"), } }
        if env_data["supabase"]["url"] is None: del env_data["supabase"]["url"]
        if env_data["supabase"]["anon_key"] is None: del env_data["supabase"]["anon_key"]
        if env_data["supabase"]["service_role_key"] is None: del env_data["supabase"]["service_role_key"]

        merged = self._deep_merge(file_data, env_data)
        merged = self._deep_merge(merged, st_data)
        if not merged:
            print("--- No secrets found by any method ---")
        return merged
    
    # --- BẮT ĐẦU SỬA LỖI THEO TƯ VẤN MỚI CỦA CHUYÊN GIA ---
    def refresh_supabase_from_secrets(self):
        """
        Sửa lỗi: Tách việc lấy URL/key và việc tạo server client.
        Nếu create_client lỗi, UI vẫn có đủ thông tin để chạy.
        """
        # 1. Luôn cập nhật URL/anon_key từ secrets, bất kể server client có tạo được hay không
        supa_config = self.secrets.get("supabase", {})
        self.supabase_url = supa_config.get("url")
        self.supabase_anon_key = supa_config.get("anon_key")

        # 2. Chỉ bao quanh việc tạo server client bằng try/except
        self.supabase = None
        service_role_key = supa_config.get("service_role_key")
        try:
            if self.supabase_url and service_role_key:
                self.supabase = create_client(self.supabase_url, service_role_key)
        except Exception as e:
            # Lỗi này giờ đây không còn nghiêm trọng, chỉ in ra cảnh báo
            print(f"[config_refresh] WARNING: Supabase server client creation failed, but preserving URL/anon for UI. Error: {e}")
        
        # Log trạng thái cuối cùng
        print(f"[config_refresh] Supabase URL present: {bool(self.supabase_url)}, anon key present: {bool(self.supabase_anon_key)}, server client: {bool(self.supabase)}")
    # --- KẾT THÚC SỬA LỖI ---
        
    def __init__(self) -> None:
        # Giữ nguyên phần khởi tạo khác
        self.secrets = self._load_secrets()
        self.AVAILABLE_PROPERTIES = { "Trang Web Chính (PropeLify)": "501726461", "Trang Web Test": "506473229", "Ứng dụng Mobile": "ID_CUA_UNG_DUNG_MOBILE" }
        self.DEFAULT_PROPERTY_NAME = "Trang Web Chính (PropeLify)"
        self.HOURLY_TOKEN_QUOTA = 5000
        self.DAILY_TOKEN_QUOTA = 25000
        self.TARGET_USERS_5MIN = 50
        self.TARGET_USERS_30MIN = 200
        self.TARGET_VIEWS_30MIN = 1000
        self.COLOR_COLD = (40, 40, 60)
        self.COLOR_HOT = (255, 190, 0)
        self.TIMEZONE_MAPPINGS = { "Viet Nam (UTC+7)": "Asia/Ho_Chi_Minh", "New York (UTC-4)": "America/New_York", "Chicago (UTC-5)": "America/Chicago", "Denver (UTC-6)": "America/Denver", "Los Angeles (UTC-7)": "America/Los_Angeles", "Anchorage (UTC-8)": "America/Anchorage", "Honolulu (UTC-10)": "Pacific/Honolulu" }
        self.page_title_map = {}
        self.landing_page_map = {}
        self.SYMBOLS = []
        self.product_to_symbol_map = {}
        try:
            with open("marketer_mapping.json", "r", encoding="utf-8") as f:
                full_mapping = json.load(f)
            self.page_title_map = full_mapping.get("page_title_mapping", {})
            self.landing_page_map = full_mapping.get("landing_page_mapping", {})
            self.product_to_symbol_map = full_mapping.get("product_to_symbol_mapping", {})
            self.SYMBOLS = sorted(list(self.page_title_map.keys()), key=len, reverse=True)
        except Exception:
            pass
        self.default_avatar_url = "https://i.ibb.co/wN8TsVMW/avatar.jpg"
        self.ga_credentials = None
        try:
            if "google_credentials" in self.secrets:
                google_creds_dict = dict(self.secrets["google_credentials"])
                google_creds_dict["private_key"] = google_creds_dict["private_key"].replace("\\n", "\n")
                self.ga_credentials = service_account.Credentials.from_service_account_info(
                    google_creds_dict, scopes=["https://www.googleapis.com/auth/analytics.readonly"]
                )
        except Exception as e:
            print(f"Error loading Google credentials: {e}")
            self.ga_credentials = None
        self.shopify_creds = self.secrets.get("shopify_credentials", {})
        self.cloudinary_cloud_name = self.secrets.get("cloudinary", {}).get("cloud_name")
        self.cloudinary_upload_preset = self.secrets.get("cloudinary", {}).get("upload_preset")
        self.users_details = self.secrets.get("users", {})
        self.auth_config = self.prepare_auth_config()
        self.supabase = None
        self.supabase_url = None
        self.supabase_anon_key = None
        self.refresh_supabase_from_secrets()

    def prepare_auth_config(self):
        users_from_secrets = self.secrets.get("users", {})
        credentials = {"usernames": {}}
        for key, user_data in users_from_secrets.items():
            username = user_data.get("username")
            if username:
                credentials["usernames"][username] = {
                    "email": user_data.get("email", f"{username}@example.com"),
                    "name": user_data.get("name", username.capitalize()),
                    "password": user_data.get("password")
                }
        auth_config = { "credentials": credentials, "cookie": { "name": self.secrets.get("cookie", {}).get("name", "dashboard_cookie"), "key": self.secrets.get("cookie", {}).get("encrypt_key", "default_secret_key"), "expiry_days": 15 }, "preauthorized": { "emails": [] } }
        return auth_config

    def get_user_details_by_username(self, username: str):
        for _, user_info in self.users_details.items():
            if user_info.get("username") == username:
                return user_info
        return None

# Singleton config
config = AppConfig()
