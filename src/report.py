from __future__ import annotations

print("REPORT.PY LOADED")
from matplotlib.ticker import MultipleLocator

from src import storage
from dataclasses import dataclass
from typing import Optional, Dict, List
from pathlib import Path
import matplotlib.pyplot as plt
import math


OUT_DIR = Path("out/charts")
DAYS = 14

def make_charts_14d() -> List[str]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    def get_last_days(marketplace: str, n: int):
        rows = storage.get_last_n_days_for_marketplace(marketplace, n)
        if not rows:
            return []
        # посмотрим формат первой строки
        first = rows[0]
        # 6 полей
        if len(first) == 6:
            return rows
        # 5 полей — добавим marketplace сами
        if len(first) == 5:
            return [(date, marketplace, imp, clk, ords, spend) for (date, imp, clk, ords, spend) in rows]
        raise ValueError(f"Unexpected row format: {first}")

    def plot_marketplace(marketplace: str, title: str, filename: str) -> Path:
        days = get_last_days(marketplace, DAYS)
        if not days:
            return OUT_DIR / filename

        dates = [date[5:] for (date, mp, imp, clk, ords, spend) in days]
        clicks = [clk for (date, mp, imp, clk, ords, spend) in days]
        orders = [ords for (date, mp, imp, clk, ords, spend) in days]
        spend = [float(spend or 0.0) for (date, mp, imp, clk, ords, spend) in days]

        fig, (ax_top, ax_bottom) = plt.subplots(
            nrows=2,
            figsize=(10.8, 6.0),
            gridspec_kw={"height_ratios": [3, 2]},
            sharex=True
        )
        fig.patch.set_facecolor("white")
        fig.suptitle(title)

        # --- TOP: Переходы + Заказы (две оси) ---
        COLOR_CLICKS = "#6A5ACD"  # фиолетовый
        COLOR_ORDERS = "#1F77B4"  # синий

        l_clicks, = ax_top.plot(
            dates, clicks,
            color=COLOR_CLICKS,
            marker="o",
            linewidth=2.6,
            label="Переходы"
        )
        ax_top.fill_between(dates, clicks, color=COLOR_CLICKS, alpha=0.12)

        # сетка поверх заливки (важно)
        ax_top.set_axisbelow(True)

        # сетка (горизонтальная + лёгкая вертикальная)
        ax_top.grid(True, axis="y", alpha=0.25)  # п.1
        ax_top.grid(True, axis="x", alpha=0.08)  # п.3 (можно убрать, если не нужно)

        from matplotlib.ticker import MultipleLocator

        ax_top.set_ylim(0, 10000)
        ax_top.yaxis.set_major_locator(MultipleLocator(1000))

        # ВАЖНО: сначала создаём правую ось
        ax_orders = ax_top.twinx()
        l_orders, = ax_orders.plot(
            dates, orders,
            color=COLOR_ORDERS,
            marker="o",
            linewidth=2.2,
            label="Заказы"
        )
        ax_orders.set_ylabel("Заказы")

        # шкала заказов 300-700 с шагом 100
        ax_orders.set_ylim(300, 1000)
        ax_orders.yaxis.set_major_locator(MultipleLocator(100))

        ax_top.legend([l_clicks, l_orders], ["Переходы", "Заказы"], loc="upper left", fontsize=10)

        # --- подписи для КАЖДОЙ точки ---

        # Переходы — НАД точкой
        for x, y in zip(dates, clicks):
            ax_top.annotate(
                f"{y}",
                xy=(x, y),
                xytext=(0, 8),
                textcoords="offset points",
                ha="center",
                va="bottom",
                fontsize=8,
                fontweight="bold",
                color=COLOR_CLICKS
            )

        # Заказы — ПОД точкой
        for x, y in zip(dates, orders):
            ax_orders.annotate(
                f"{y}",
                xy=(x, y),
                xytext=(0, -12),
                textcoords="offset points",
                ha="center",
                va="top",
                fontsize=8,
                fontweight="bold",
                color=COLOR_ORDERS
            )

        # --- BOTTOM: Затраты (₽) ---
        if max(spend) == 0.0:
            ax_bottom.text(
                0.02, 0.65,
                "Затраты: нет данных",
                transform=ax_bottom.transAxes,
                fontsize=11
            )
            ax_bottom.set_ylabel("Затраты (₽)")
            ax_bottom.grid(True, axis="y", alpha=0.15)


        else:

            # --- Затраты (₽) — синие столбцы (левая ось) ---
            bars_spend = ax_bottom.bar(dates, spend, alpha=0.30, label="Затраты (₽)", width=0.80)
            ax_bottom.set_ylabel("Затраты (₽)")
            ax_bottom.set_ylim(0, 15000)
            ax_bottom.yaxis.set_major_locator(MultipleLocator(3000))
            ax_bottom.set_axisbelow(True)
            ax_bottom.grid(True, axis="y", alpha=0.15)
            ax_bottom.grid(True, axis="x", alpha=0.08)
            # подписи затрат (внутри/над столбцом)
            for b, val in zip(bars_spend, spend):
                x = b.get_x() + b.get_width() / 2
                h = b.get_height()
                label = f"{int(val):,}".replace(",", " ")
                if h >= 1200:
                    y = h - 350
                    va = "top"
                else:
                    y = h + 150
                    va = "bottom"
                ax_bottom.text(
                    x, y, label,
                    ha="center",
                    va=va,
                    fontsize=8,
                    fontweight="bold"
                )

            # --- CPO (₽/заказ) — жёлтые столбцы "внутри" (правая ось) ---
            # CPO = spend / orders
            cpo = [(s / o) if o else 0.0 for s, o in zip(spend, orders)]
            ax_cpo = ax_bottom.twinx()
            bars_cpo = ax_cpo.bar(
                dates, cpo,
                width=0.35,  # уже — выглядит "внутри" синего
                alpha=0.95,
                color="#F2C94C",
                label="CPO (₽/заказ)"
            )
            ax_cpo.set_ylabel("CPO (₽/заказ)")
            # правая шкала CPO: шаг 5 ₽
            ax_cpo.set_ylim(5, 50)
            ax_cpo.yaxis.set_major_locator(MultipleLocator(10))
            # подписи CPO внутри каждого жёлтого столбца (каждый день)
            for b, val in zip(bars_cpo, cpo):
                x = b.get_x() + b.get_width() / 2
                h = b.get_height()
                label = f"{val:.1f}"
                if h >= 2.0:
                    y = h - 0.6  # чуть ниже верхушки
                    va = "top"
                else:
                    y = h + 0.4
                    va = "bottom"
                ax_cpo.text(
                    x, y, label,
                    ha="center",
                    va=va,
                    fontsize=7,
                    fontweight="bold",
                    color="white"
                )

            # общая легенда (и Затраты, и CPO)
            ax_bottom.legend(
                [bars_spend, bars_cpo],
                ["Затраты (₽)", "CPO (₽/заказ)"],
                loc="upper left",
                fontsize=10
            )

        # Чуть повернём даты, чтобы смотрелось аккуратно
        ax_bottom.tick_params(axis="x", rotation=0)

        fig.tight_layout(rect=[0, 0, 1, 0.96])
        out_path = OUT_DIR / filename
        fig.savefig(out_path, dpi=180)
        plt.close(fig)
        return out_path

    wb_path = plot_marketplace("wb", "WB — 14 дней", "wb_14d.png")

    # ВАЖНО: всегда возвращаем список
    paths = []
    if wb_path.exists():
        paths.append(str(wb_path))
    return paths
