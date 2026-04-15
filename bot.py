"""
Cainiao & UCB — ежедневная сводка партий в 12:00 по Ташкенту.
Читает Google Sheet как публичный CSV (без API ключей).

Переменные окружения (задать в Render → Environment):
  CAINIAO_BOT_TOKEN   — токен бота Cainiao
  CAINIAO_CHAT_ID     — chat_id группы Cainiao
  UCB_BOT_TOKEN       — токен бота UCB
  UCB_CHAT_ID         — chat_id группы UCB
  SHEET_CSV_URL       — публичный CSV URL таблицы
  PUBLIC_URL          — URL этого сервиса на Render (для self-ping)
  PORT                — порт (Render ставит сам, не трогать)
"""

import os
import csv
import time
import logging
import asyncio
import threading
import urllib.request
from datetime import datetime, date
from http.server import HTTPServer, BaseHTTPRequestHandler
from io import StringIO
from zoneinfo import ZoneInfo

from telegram import Bot
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

TZ = ZoneInfo("Asia/Tashkent")

# ─── Переменные окружения ─────────────────────────────────────────────────────
CAINIAO_TOKEN  = os.environ["CAINIAO_BOT_TOKEN"]
CAINIAO_CHATID = os.environ["CAINIAO_CHAT_ID"]
UCB_TOKEN      = os.environ["UCB_BOT_TOKEN"]
UCB_CHATID     = os.environ["UCB_CHAT_ID"]
SHEET_CSV_URL  = os.environ["SHEET_CSV_URL"]
PUBLIC_URL     = os.environ.get("PUBLIC_URL", "")

# ─── Индексы колонок (0-based) ────────────────────────────────────────────────
# B=1 Проект, F=5 AWB/Flight No., N=13 ETA
COL_PROJECT = 1
COL_AWB     = 5
COL_ETA     = 13

# ─── Фильтр по проекту ───────────────────────────────────────────────────────
# Значения в колонке B (нижний регистр)
UCB_VALUES    = {"uzum mko", "uzum dg"}
SHARED_VALUE  = "cainiao + uzum"   # относится к ОБОИМ проектам


def belongs_to(project_cell: str, project: str) -> bool:
    """Проверяет принадлежность строки к проекту по значению колонки B."""
    val = project_cell.strip().lower()
    if val == SHARED_VALUE:
        return True          # партия идёт в оба бота
    if project == "cainiao":
        return val == "cainiao"
    if project == "ucb":
        return val in UCB_VALUES
    return False


# ─── Чтение Google Sheet как CSV ─────────────────────────────────────────────
def fetch_rows() -> list[list[str]]:
    """Скачивает публичный CSV и возвращает список строк."""
    log.info("Загружаем таблицу: %s", SHEET_CSV_URL)
    req = urllib.request.Request(
        SHEET_CSV_URL,
        headers={"User-Agent": "Mozilla/5.0"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8")
    reader = csv.reader(StringIO(raw))
    return list(reader)


# ─── Парсинг даты ─────────────────────────────────────────────────────────────
def parse_date(val: str) -> date | None:
    val = val.strip()
    if not val:
        return None
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(val, fmt).date()
        except ValueError:
            continue
    return None


# ─── Формирование сводки ──────────────────────────────────────────────────────
def build_report(rows: list[list[str]], today: date, project: str, project_name: str) -> str:
    """
    Логика:
      ETA = сегодня        → ✅ Прибыли сегодня
      ETA пустая           → ⏳ Ещё не прибыли (берём и завтра)
      ETA < сегодня        → пропускаем (уже было в прошлых сводках)
      ETA > сегодня        → пропускаем (плановая, ждём факта)
    """
    arrived: list[str] = []
    pending: list[str] = []

    for i, row in enumerate(rows):
        if i == 0:
            continue  # заголовок
        if len(row) <= max(COL_PROJECT, COL_AWB, COL_ETA):
            continue

        # Фильтр по проекту
        if not belongs_to(row[COL_PROJECT], project):
            continue

        awb = row[COL_AWB].strip()
        if not awb:
            continue

        eta_raw = row[COL_ETA].strip() if len(row) > COL_ETA else ""
        eta = parse_date(eta_raw)

        if eta is None:
            pending.append(awb)
        elif eta == today:
            arrived.append(awb)
        # eta < today или > today — не показываем

    date_str = today.strftime("%d.%m.%Y")
    lines = [f"📦 *{project_name}* — сводка по партиям на {date_str}\n"]

    if arrived:
        lines.append("✅ *Прибыли сегодня:*")
        for awb in arrived:
            lines.append(f"  • {awb}")
    else:
        lines.append("✅ *Прибыли сегодня:* нет")

    lines.append("")

    if pending:
        lines.append("⏳ *Ещё не прибыли (ожидаются):*")
        for awb in pending:
            lines.append(f"  • {awb}")
    else:
        lines.append("⏳ *Ожидаемых партий нет*")

    return "\n".join(lines)


# ─── Отправка сводки ──────────────────────────────────────────────────────────
async def send_report(token: str, chat_id: str, project: str, project_name: str) -> None:
    try:
        rows = fetch_rows()
        today = datetime.now(tz=TZ).date()
        text = build_report(rows, today, project, project_name)
        bot = Bot(token=token)
        await bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")
        log.info("[%s] Сводка отправлена.", project_name)
    except Exception as e:
        log.error("[%s] Ошибка: %s", project_name, e, exc_info=True)


# ─── Health server (нужен Render для keep-alive) ──────────────────────────────
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, *args):
        pass


def run_health_server():
    port = int(os.environ.get("PORT", 8000))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    log.info("Health server запущен на порту %s", port)
    server.serve_forever()


# ─── Self-ping (не даёт Render усыпить сервис) ────────────────────────────────
def self_ping():
    if not PUBLIC_URL:
        log.warning("PUBLIC_URL не задан — self-ping отключён.")
        return
    while True:
        time.sleep(4 * 60)  # каждые 4 минуты
        try:
            urllib.request.urlopen(PUBLIC_URL, timeout=10)
            log.info("Self-ping OK")
        except Exception as e:
            log.warning("Self-ping failed: %s", e)


# ─── Главный цикл ─────────────────────────────────────────────────────────────
async def main():
    # Запускаем health server и self-ping в фоне
    threading.Thread(target=run_health_server, daemon=True).start()
    threading.Thread(target=self_ping, daemon=True).start()

    scheduler = AsyncIOScheduler(timezone=TZ)

    # Cainiao — каждый день в 12:00 Ташкент
    scheduler.add_job(
        send_report,
        trigger="cron", hour=12, minute=0,
        args=[CAINIAO_TOKEN, CAINIAO_CHATID, "cainiao", "Cainiao"],
        id="cainiao_report",
        misfire_grace_time=300,
    )

    # UCB — каждый день в 12:00 Ташкент
    scheduler.add_job(
        send_report,
        trigger="cron", hour=12, minute=0,
        args=[UCB_TOKEN, UCB_CHATID, "ucb", "UCB"],
        id="ucb_report",
        misfire_grace_time=300,
    )

    scheduler.start()
    log.info("Планировщик запущен. Ежедневная сводка в 12:00 Ташкент.")

    try:
        while True:
            await asyncio.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        log.info("Остановка.")
        scheduler.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
