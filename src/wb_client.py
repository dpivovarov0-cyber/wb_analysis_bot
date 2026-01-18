from dataclasses import dataclass
from typing import Dict, Optional, List
import io
import csv
import time
import zipfile
import uuid
import os
import requests
import json


from src.config import WB_TOKEN

BASE = "https://seller-analytics-api.wildberries.ru"
ADS_BASE = "https://advert-api.wildberries.ru"

def fetch_ads_spend_by_day(date_from: str, date_to: str) -> Dict[str, float]:
    token = WB_TOKEN   # ← ВОТ ЭТО КЛЮЧЕВО
    if not token:
        return {}

    url = f"{ADS_BASE}/adv/v1/upd"
    headers = {"Authorization": token}
    params = {"from": date_from, "to": date_to}

    try:
        r = requests.get(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        items = r.json() or []
    except Exception:
        return {}

    out = {}
    for it in items:
        t = it.get("updTime")
        s = it.get("updSum", 0) or 0
        if not t:
            continue
        day = t[:10]
        out[day] = out.get(day, 0.0) + float(s)
    return out


def fetch_all_nm_ids() -> list[int]:
    """
    Получаем все nmID продавца через Content API WB
    """
    url = "https://content-api.wildberries.ru/content/v2/get/cards/list"
    headers = {
        "Authorization": f"Bearer {WB_TOKEN}",
        "Content-Type": "application/json"
    }

    nm_ids = []
    cursor = {"limit": 100}

    while True:
        payload = {
            "settings": {
                "cursor": cursor,
                "filter": {
                    "withPhoto": -1
                }
            }
        }

        r = requests.post(url, headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        data = r.json()

        cards = data.get("cards", [])
        if not cards:
            break

        for c in cards:
            if "nmID" in c:
                nm_ids.append(c["nmID"])

        cursor = data["cursor"]
        if cursor.get("total", 0) < cursor.get("limit", 100):
            break

    return sorted(set(nm_ids))



@dataclass
class WBDay:
    visibility: int = 0
    open: int = 0         # переходы (открытия/переходы в карточку)
    orders: int = 0       # заказы
    ad_spend: Optional[float] = None  # затраты на рекламу (если будет в отчете)


def _headers() -> dict:
    if not WB_TOKEN:
        raise RuntimeError("WB_TOKEN is empty. Put it into .env")
    return {"Authorization": WB_TOKEN}


def _safe_int(x) -> int:
    try:
        if x is None:
            return 0
        s = str(x).strip().replace(" ", "").replace("\u00A0", "")
        if s == "":
            return 0
        return int(float(s.replace(",", ".")))
    except Exception:
        return 0


def _safe_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        s = str(x).strip().replace(" ", "").replace("\u00A0", "")
        if s == "":
            return None
        return float(s.replace(",", "."))
    except Exception:
        return None

from datetime import datetime, timedelta

NMIDS_PATH = os.path.join("data", "wb_nm_ids.json")

def save_nm_ids(nm_ids: list[int]) -> None:
    os.makedirs("data", exist_ok=True)
    with open(NMIDS_PATH, "w", encoding="utf-8") as f:
        json.dump(sorted(set(nm_ids)), f, ensure_ascii=False, indent=2)

def refresh_nm_ids_cache(max_age_hours: int = 24) -> list[int]:
    os.makedirs("data", exist_ok=True)

    if os.path.exists(NMIDS_PATH):
        mtime = datetime.fromtimestamp(os.path.getmtime(NMIDS_PATH))
        if datetime.now() - mtime < timedelta(hours=max_age_hours):
            return load_nm_ids()

    nm_ids = fetch_all_nm_ids()
    save_nm_ids(nm_ids)
    return nm_ids

def load_nm_ids() -> list[int]:
    with open("data/wb_nm_ids.json", "r", encoding="utf-8") as f:
        return json.load(f)

def _create_detail_history_report(start: str, end: str, tz: str = "Europe/Moscow") -> str:
    """
    POST /api/v2/nm-report/downloads
    reportType=DETAIL_HISTORY_REPORT (Sales funnel report by WB articles)
    """
    url = f"{BASE}/api/v2/nm-report/downloads"
    download_id = str(uuid.uuid4())

    payload = {
        "id": download_id,
        "reportType": "DETAIL_HISTORY_REPORT",
        "userReportName": f"mp_analysis_bot {start}..{end}",
        "params": {
            # В доке: nmIDs можно оставить пустым, чтобы получить отчет по всем товарам
            # (для некоторых типов он обязателен, но для DETAIL_HISTORY_REPORT допускают пустой для "все товары")
            "nmIDs": refresh_nm_ids_cache(),
            "subjectIds": [],
            "brandNames": [],
            "tagIds": [],
            "startDate": start,
            "endDate": end,
            "timezone": tz,
            "aggregationLevel": "day",
            "skipDeletedNm": True
        }
    }

    r = requests.post(url, json=payload, headers=_headers(), timeout=45)
    r.raise_for_status()
    return download_id


def _get_report_status(download_id: str) -> Optional[dict]:
    """
    GET /api/v2/nm-report/downloads?filter[downloadIds]=...
    """
    url = f"{BASE}/api/v2/nm-report/downloads"
    params = {"filter[downloadIds]": download_id}
    r = requests.get(url, params=params, headers=_headers(), timeout=45)
    r.raise_for_status()
    js = r.json()
    data = js.get("data", [])
    if not data:
        return None
    return data[0]


def _download_report_zip(download_id: str) -> bytes:
    """
    GET /api/v2/nm-report/downloads/file/{downloadId}
    ZIP -> CSV inside
    """
    url = f"{BASE}/api/v2/nm-report/downloads/file/{download_id}"
    r = requests.get(url, headers=_headers(), timeout=90)
    r.raise_for_status()
    return r.content


def _wait_and_download_csv(download_id: str, max_wait_sec: int = 180) -> str:
    """
    Ждём SUCCESS, скачиваем ZIP, достаём CSV как текст (utf-8/utf-16)
    Важно: методы nm-report лимитированы (3 запроса в минуту) — поэтому polling редкий.
    """
    waited = 0
    while waited <= max_wait_sec:
        info = _get_report_status(download_id)
        if info and info.get("status") == "SUCCESS":
            zbytes = _download_report_zip(download_id)
            with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
                # берем первый CSV
                name = next((n for n in zf.namelist() if n.lower().endswith(".csv")), None)
                if not name:
                    raise RuntimeError("WB report zip has no CSV inside")
                raw = zf.read(name)

            # пробуем декодировки
            for enc in ("utf-8-sig", "utf-16", "cp1251"):
                try:
                    return raw.decode(enc)
                except Exception:
                    pass
            # если совсем странно — вернем как latin-1 (не упадет) и дальше будем парсить
            return raw.decode("latin-1", errors="replace")

        if info and info.get("status") == "FAILED":
            raise RuntimeError(f"WB report generation FAILED for {download_id}")

        # ждем и не долбим лимиты
        time.sleep(20)
        waited += 20

    raise RuntimeError(f"WB report not ready in {max_wait_sec}s (downloadId={download_id})")


def _parse_detail_history_csv(csv_text: str) -> Dict[str, WBDay]:
    """
    Парсим CSV, агрегируем по dt (дата) в суммарные:
    - показы
    - переходы/открытия
    - заказы
    - затраты (если есть колонка)
    Названия колонок могут немного отличаться — ищем по набору возможных имен.
    """
    # delimiter у WB чаще ';'
    sample = csv_text[:2000]
    delim = ";" if sample.count(";") >= sample.count(",") else ","

    reader = csv.DictReader(io.StringIO(csv_text), delimiter=delim)
    headers = [h.strip() for h in (reader.fieldnames or [])]

    def pick_col(candidates: List[str]) -> Optional[str]:
        low = {h.lower(): h for h in headers}
        for c in candidates:
            if c.lower() in low:
                return low[c.lower()]
        return None

    # кандидаты под разные возможные заголовки
    col_date = "dt"
    col_vis = None
    col_open = pick_col(["openCardCount", "open", "opens", "openCount", "clicks", "переходы", "открытия", "открытия карточки"])
    col_orders = pick_col(["ordersCount", "orders", "orderCount", "заказы", "заказали", "количество заказов"])
    col_spend = None

    if not col_date:
        # чтобы не гадать молча
        raise RuntimeError(f"WB CSV: cannot find date column. Headers: {headers[:40]}")

    out: Dict[str, WBDay] = {}

    for row in reader:
        dt = (row.get(col_date) or "").strip()
        if not dt:
            continue

        day = out.get(dt, WBDay())
        day.open += _safe_int(row.get(col_open) if col_open else row.get("openCardCount"))
        day.orders += _safe_int(row.get(col_orders) if col_orders else row.get("ordersCount"))
        out[dt] = day

    return out


def fetch_wb_14d(start: str, end: str) -> Dict[str, WBDay]:
    """
    Главная функция для main.py:
    WB 14 дней по дням через Seller Analytics CSV (Jam) DETAIL_HISTORY_REPORT.
    """
    # кэшируем CSV, чтобы не жечь лимиты и не создавать много отчётов
    cache_path = os.path.join("data", f"wb_detail_history_{start}_{end}.csv")
    os.makedirs("data", exist_ok=True)

    if os.path.exists(cache_path):
        with open(cache_path, "r", encoding="utf-8", errors="ignore") as f:
            csv_text = f.read()
    else:
        download_id = _create_detail_history_report(start, end, tz="Europe/Moscow")
        csv_text = _wait_and_download_csv(download_id, max_wait_sec=240)
        with open(cache_path, "w", encoding="utf-8") as f:
            f.write(csv_text)

    days = _parse_detail_history_csv(csv_text)

    spend_map = fetch_ads_spend_by_day(start, end)
    for dt, d in days.items():
        if dt in spend_map:
            d.ad_spend = spend_map[dt]

    return days


    return _parse_detail_history_csv(csv_text)

