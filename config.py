import os
import json
from dotenv import load_dotenv

load_dotenv()


class Config:
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
    SHEET_ID = os.getenv("SHEET_ID")
    REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    SESSION_TTL_PADDING = int(os.getenv("SESSION_TTL_PADDING", "300"))

    # New IDs for owner and admin
    OWNER_TELEGRAM_ID = os.getenv("OWNER_TELEGRAM_ID")

    # ADMIN_TELEGRAM_ID can be a single ID or comma-separated list
    _admin_ids_str = os.getenv("ADMIN_TELEGRAM_ID", "")
    ADMIN_TELEGRAM_IDS = []
    if _admin_ids_str:
        # Split by comma and strip whitespace
        ADMIN_TELEGRAM_IDS = [aid.strip() for aid in _admin_ids_str.split(",") if aid.strip()]

    # Keep ADMIN_TELEGRAM_ID for backward compatibility (first admin in list)
    ADMIN_TELEGRAM_ID = ADMIN_TELEGRAM_IDS[0] if ADMIN_TELEGRAM_IDS else None

    # Scheduler settings
    CAMPAIGN_CHECK_INTERVAL_MINUTES = int(os.getenv("CAMPAIGN_CHECK_INTERVAL_MINUTES", "1"))

    # Google Sheets credentials
    _credentials_value = os.getenv("GOOGLE_CREDENTIALS")
    if _credentials_value:
        try:
            GOOGLE_CREDENTIALS = json.loads(_credentials_value)
        except json.JSONDecodeError:
            # Если это путь к файлу
            credentials_path = os.path.abspath(_credentials_value)
            if os.path.exists(credentials_path):
                with open(credentials_path, "r", encoding="utf-8") as f:
                    GOOGLE_CREDENTIALS = json.load(f)
            else:
                raise ValueError(
                    "GOOGLE_CREDENTIALS должен быть валидным JSON или путем к файлу"
                )
    else:
        raise ValueError("GOOGLE_CREDENTIALS не установлен")

    @classmethod
    def validate(cls):
        if not cls.TELEGRAM_TOKEN:
            raise ValueError("TELEGRAM_TOKEN не установлен")
        if not cls.SHEET_ID:
            raise ValueError("SHEET_ID не установлен")

