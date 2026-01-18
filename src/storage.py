import sqlite3
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

DB_PATH = Path("data/mp.db")

def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH.as_posix())
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def init_db() -> None:
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_metrics (
                date TEXT NOT NULL,
                marketplace TEXT NOT NULL, -- 'wb' or 'ozon'
                impressions INTEGER NOT NULL DEFAULT 0,
                clicks INTEGER NOT NULL DEFAULT 0,
                orders INTEGER NOT NULL DEFAULT 0,
                ad_spend REAL, -- can be NULL if not available
                PRIMARY KEY (date, marketplace)
            );
            """
        )

def upsert_metrics(
    date: str,
    marketplace: str,
    impressions: int,
    clicks: int,
    orders: int,
    ad_spend: Optional[float] = None
) -> None:
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO daily_metrics (date, marketplace, impressions, clicks, orders, ad_spend)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(date, marketplace) DO UPDATE SET
                impressions=excluded.impressions,
                clicks=excluded.clicks,
                orders=excluded.orders,
                ad_spend=excluded.ad_spend;
            """,
            (date, marketplace, impressions, clicks, orders, ad_spend)
        )

def get_last_n_days(n_days: int) -> List[Tuple[str, str, int, int, int, Optional[float]]]:
    with _connect() as conn:
        cur = conn.execute(
            """
            SELECT date, marketplace, impressions, clicks, orders, ad_spend
            FROM daily_metrics
            ORDER BY date DESC
            LIMIT ?;
            """,
            (n_days * 2,)  # wb+ozon
        )
        return cur.fetchall()

from typing import List, Tuple, Optional

def get_last_n_days_for_marketplace(mp: str, n_days: int) -> List[Tuple[str, int, int, int, Optional[float]]]:
    with _connect() as conn:
        cur = conn.execute(
            """
            SELECT date, impressions, clicks, orders, ad_spend
            FROM daily_metrics
            WHERE marketplace = ?
            ORDER BY date DESC
            LIMIT ?;
            """,
            (mp, n_days)
        )
        rows = cur.fetchall()
        # вернем по возрастанию даты, чтобы график шел слева направо
        return list(reversed(rows))
