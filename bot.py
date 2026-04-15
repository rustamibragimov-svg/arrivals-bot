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

Команды в Telegram:
  /test — отправить сводку прямо сейчас (без ожидания 12:00)
"""

import os
import csv
import time
import logging
import asyncio
import threading
import urllib.request
from datetime import datetime, date, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from io import StringIO
from zoneinfo import ZoneInfo

from telegram import Bot, Update
from telegram.ext import Application, CommandHandler, ContextTypes
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
# B=1 Проект, F=5 AWB/Flight No., O=14 ATA with time
COL_PROJECT = 1
COL_AWB     = 5
COL_ETA     = 14

# ─── Фильтр по проекту ───────────────────────────────────────────────────────
UCB_VALUES   = {"uzum mko", "uzum dg"}
SHARED_VALUE = "cainiao + uzum"


def belongs_to(project_cell: str, project: str) -> bool:
    val = project_cell.strip().lower()
    if val == SHARED_VALUE:
        return True
    if project == "cainiao":
        return val == "cainiao"
    if project == "ucb":
        return val in UCB_VALUES
    return False


# ─── Чтение Google Sheet как CSV ─────────────────────────────────────────────
def fetch_rows() -> list[list[str]]:
    log.info("Загружаем таблицу: %s", SHEET_CSV_URL)
    req = urllib.request.Request(
        SHEET_CSV_URL,
        headers={"User-Agent": "Mozilla/5.0"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8")
    reader = csv.reader(StringIO(raw))
    return list(reader)


# ─── Парсинг даты/времени ────────────────────────────────────────────────────
def parse_ata(val: str):
    """Парсит ATA (колонка O) как timezone-aware datetime (Ташкент).
    Поддерживает форматы со временем и без."""
    val = val.strip()
    if not val:
        return None
    for fmt in ("%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M", "%d.%m.%Y",
                "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(val, fmt)
            return dt.replace(tzinfo=TZ)
        except ValueError:
            continue
    return None


def parse_date(val: str) -> date | None:
    """Парсит ETA (колонка N) только как дату (для отображения)."""
    val = val.strip().split()[0]  # убираем время если есть
    if not val:
        return None
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(val, fmt).date()
        except ValueError:
            continue
    return None


# ─── Формирование сводки ──────────────────────────────────────────────────────
def build_report(rows: list[list[str]], report_dt: datetime, project: str, project_name: str) -> str:
    """
    Окно «прибывших»: от вчерашних 12:00 до сегодняшних 12:00 (Ташкент).

    ATA в окне             → ✅ Прибыли сегодня
    ATA пустая / > 12:00   → ⏳ Ожидаются (с ETA из колонки N)
    ATA < вчера 12:00      → не показываем (уже было в прошлых сводках)
    """
    # Границы окна
    prev_report_dt = report_dt - timedelta(days=1)  # вчера 12:00

    arrived: list[str] = []
    pending: list[tuple[str, str]] = []
    seen: set[str] = set()

    for i, row in enumerate(rows):
        if i == 0:
            continue
        if len(row) <= max(COL_PROJECT, COL_AWB, COL_ETA):
            continue
        if not belongs_to(row[COL_PROJECT], project):
            continue

        awb = row[COL_AWB].strip()
        if not awb or awb in seen:
            continue
        seen.add(awb)

        ata_raw = row[COL_ETA].strip() if len(row) > COL_ETA else ""
        ata_dt = parse_ata(ata_raw)

        # ETA (колонка N=13) — плановая дата/время, показываем рядом с AWB
        eta_display = ""
        if len(row) > 13 and row[13].strip():
            eta_display = row[13].strip()

        if ata_dt is None:
            # ATA не заполнена — партия ещё в пути
            pending.append((awb, eta_display))
        elif prev_report_dt < ata_dt <= report_dt:
            # Прибыла в окне: вчера 12:00 — сегодня 12:00
            arrived.append(awb)
        elif ata_dt > report_dt:
            # Прилетит после текущего отчёта — в ожидаемых
            pending.append((awb, eta_display))
        # ata_dt <= prev_report_dt → уже показывали, пропускаем

    date_str = report_dt.strftime("%d.%m.%Y")
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
        for awb, eta_d in pending:
            suffix = f"  _(ETA: {eta_d})_" if eta_d else ""
            lines.append(f"  • {awb}{suffix}")
    else:
        lines.append("⏳ *Ожидаемых партий нет*")

    return "\n".join(lines)


# ─── Отправка сводки ──────────────────────────────────────────────────────────
async def send_report(token: str, chat_id: str, project: str, project_name: str) -> None:
    try:
        rows = fetch_rows()
        # report_dt — сегодня в 12:00 по Ташкенту (граница окна прибывших)
        now = datetime.now(tz=TZ)
        report_dt = now.replace(hour=12, minute=0, second=0, microsecond=0)
        text = build_report(rows, report_dt, project, project_name)
        bot = Bot(token=token)
        await bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")
        log.info("[%s] Сводка отправлена.", project_name)
    except Exception as e:
        log.error("[%s] Ошибка: %s", project_name, e, exc_info=True)


# ─── /test команда ───────────────────────────────────────────────────────────
# Написать /test прямо в группе — бот сразу пришлёт сводку, не дожидаясь 12:00
async def cmd_test_cainiao(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.info("[Cainiao] /test от пользователя %s", update.effective_user.id)
    await update.message.reply_text("⏳ Генерирую сводку Cainiao...")
    await send_report(CAINIAO_TOKEN, CAINIAO_CHATID, "cainiao", "Cainiao")
    await update.message.reply_text("✅ Готово — сводка отправлена в группу Cainiao.")


async def cmd_test_ucb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.info("[UCB] /test от пользователя %s", update.effective_user.id)
    await update.message.reply_text("⏳ Генерирую сводку UCB...")
    await send_report(UCB_TOKEN, UCB_CHATID, "ucb", "UCB")
    await update.message.reply_text("✅ Готово — сводка отправлена в группу UCB.")


# ─── Health server ────────────────────────────────────────────────────────────
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


# ─── Self-ping ────────────────────────────────────────────────────────────────
def self_ping():
    if not PUBLIC_URL:
        log.warning("PUBLIC_URL не задан — self-ping отключён.")
        return
    while True:
        time.sleep(4 * 60)
        try:
            urllib.request.urlopen(PUBLIC_URL, timeout=10)
            log.info("Self-ping OK")
        except Exception as e:
            log.warning("Self-ping failed: %s", e)


# ─── Главный цикл ─────────────────────────────────────────────────────────────
async def main():
    threading.Thread(target=run_health_server, daemon=True).start()
    threading.Thread(target=self_ping, daemon=True).start()

    # Планировщик — ежедневно в 12:00 Ташкент
    scheduler = AsyncIOScheduler(timezone=TZ)
    scheduler.add_job(
        send_report, trigger="cron", hour=12, minute=0,
        args=[CAINIAO_TOKEN, CAINIAO_CHATID, "cainiao", "Cainiao"],
        id="cainiao_report", misfire_grace_time=300,
    )
    scheduler.add_job(
        send_report, trigger="cron", hour=12, minute=0,
        args=[UCB_TOKEN, UCB_CHATID, "ucb", "UCB"],
        id="ucb_report", misfire_grace_time=300,
    )
    scheduler.start()
    log.info("Планировщик запущен. Ежедневная сводка в 12:00 Ташкент.")

    # Два Application — по одному на каждого бота, оба слушают /test
    app_cainiao = Application.builder().token(CAINIAO_TOKEN).build()
    app_cainiao.add_handler(CommandHandler("test", cmd_test_cainiao))

    app_ucb = Application.builder().token(UCB_TOKEN).build()
    app_ucb.add_handler(CommandHandler("test", cmd_test_ucb))

    async with app_cainiao, app_ucb:
        await app_cainiao.initialize()
        await app_ucb.initialize()
        await app_cainiao.start()
        await app_ucb.start()
        await app_cainiao.updater.start_polling(allowed_updates=Update.ALL_TYPES)
        await app_ucb.updater.start_polling(allowed_updates=Update.ALL_TYPES)
        log.info("Боты запущены. Команда /test доступна в группах.")

        try:
            while True:
                await asyncio.sleep(60)
        except (KeyboardInterrupt, SystemExit):
            pass
        finally:
            await app_cainiao.updater.stop()
            await app_ucb.updater.stop()
            await app_cainiao.stop()
            await app_ucb.stop()
            scheduler.shutdown()
            log.info("Остановка.")


if __name__ == "__main__":
    asyncio.run(main())
