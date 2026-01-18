import os
from dotenv import load_dotenv

load_dotenv()

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "").strip()

WB_TOKEN = os.getenv("WB_TOKEN", "").strip()

TZ = os.getenv("TZ", "Europe/Moscow").strip()
REPORT_TIME = os.getenv("REPORT_TIME", "10:05").strip()
DAYS = int(os.getenv("DAYS", "14"))

if not TG_BOT_TOKEN:
    raise RuntimeError("TG_BOT_TOKEN is empty in .env")
if not TG_CHAT_ID:
    raise RuntimeError("TG_CHAT_ID is empty in .env")
