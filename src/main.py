from __future__ import annotations
from src.wb_client import fetch_wb_14d

from datetime import datetime, timedelta
import pytz

from src.config import TZ
from src.storage import init_db, upsert_metrics
from src.report import make_charts_14d
from src.tg_sender import send_message, send_photo

def fmt_int(n: int) -> str:
    return f"{n:,}".replace(",", " ")

def fmt_money(rub: float) -> str:
    return f"{int(round(rub)):,}".replace(",", " ") + " ₽"

def trend_icon(cur: float, prev: float) -> str:
    if cur > prev:
        return "🔼"
    if cur < prev:
        return "🔽"
    return "⏺"

def fmt_delta(cur: float, prev: float, is_percent: bool = True) -> str:
    d = cur - prev
    if prev == 0:
        # чтобы не было деления на 0
        sign = "+" if d > 0 else ""
        return f"({sign}{fmt_int(int(d))})"
    pct = (d / prev) * 100.0 if is_percent else 0.0
    sign = "+" if d > 0 else ""
    return f"({sign}{fmt_int(int(round(d)))} | {sign}{pct:.1f}%)"

def moscow_now():
    tz = pytz.timezone(TZ)
    return datetime.now(tz)

def main():
    init_db()

    now = moscow_now()
    yesterday = (now - timedelta(days=1)).date()

    # --- WB: реальные данные за 14 дней ---
    start_14 = (yesterday - timedelta(days=13)).isoformat()
    end_14 = yesterday.isoformat()

    wb_days = fetch_wb_14d(start_14, end_14)

    for dt, d in wb_days.items():
        upsert_metrics(
            dt,
            "wb",
            0,  # impressions (показы) не используем
            d.open,  # clicks
            d.orders,  # orders
            d.ad_spend  # spend
        )

    # --- отчет за вчера (WB) + дельты к позавчера ---
    dt_y = yesterday.isoformat()
    dt_prev = (yesterday - timedelta(days=1)).isoformat()

    wb_y = wb_days.get(dt_y)
    wb_p = wb_days.get(dt_prev)

    # вчера
    open_y = wb_y.open if wb_y else 0
    orders_y = wb_y.orders if wb_y else 0
    spend_y = wb_y.ad_spend if (wb_y and wb_y.ad_spend is not None) else 0.0

    # позавчера
    open_p = wb_p.open if wb_p else 0
    orders_p = wb_p.orders if wb_p else 0
    spend_p = wb_p.ad_spend if (wb_p and wb_p.ad_spend is not None) else 0.0

    cr_y = (orders_y / open_y * 100) if open_y else 0.0
    cr_p = (orders_p / open_p * 100) if open_p else 0.0

    # CPO = cost per order
    cpo_y = (spend_y / orders_y) if orders_y else 0.0
    cpo_p = (spend_p / orders_p) if orders_p else 0.0

    text = (
        f"Отчет за {dt_y} (вчера)\n\n"
        f"WB\n"
        f"Переходы: {fmt_int(open_y)} {trend_icon(open_y, open_p)} {fmt_delta(open_y, open_p)}\n"
        f"Заказы: {fmt_int(orders_y)} {trend_icon(orders_y, orders_p)} {fmt_delta(orders_y, orders_p)}\n"
        f"% заказа (CR): {cr_y:.2f}%\n"
        f"Реклама: {fmt_money(spend_y)} {trend_icon(spend_y, spend_p)} {fmt_delta(spend_y, spend_p)}\n"
        f"CPO: {cpo_y:.1f} ₽ {trend_icon(cpo_y, cpo_p)} ({cpo_y - cpo_p:+.1f} ₽)"
    )

    send_message(text)

    # 2 графика по площадкам за 14 дней
    charts = make_charts_14d()
    for p in charts:
        send_photo(p)

if __name__ == "__main__":
    main()
