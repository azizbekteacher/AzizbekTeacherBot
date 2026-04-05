"""Google Sheets integratsiyasi — ro'yxatdan o'tgan foydalanuvchilar va konsultatsiya ma'lumotlari."""

import base64
import json
import logging
import os
from datetime import datetime
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials

from config import GOOGLE_SHEETS_SPREADSHEET_ID

log = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

HEADERS = [
    "No", "Ism (Telegram)", "Yosh", "Telefon", "Maqsad/Natija",
    "Video ko'rganmi", "Qulay vaqt", "Telegram ID", "Sana",
]

_client: gspread.Client | None = None


def _get_client() -> gspread.Client | None:
    """Google Sheets clientni yaratish yoki cache dan qaytarish."""
    global _client
    if _client is not None:
        return _client

    try:
        # 1) Env variable dan base64 kodlangan JSON
        creds_b64 = os.getenv("GOOGLE_SHEETS_CREDENTIALS_B64", "")
        if creds_b64:
            creds_json = base64.b64decode(creds_b64).decode("utf-8")
            info = json.loads(creds_json)
            creds = Credentials.from_service_account_info(info, scopes=SCOPES)
            _client = gspread.authorize(creds)
            return _client

        # 2) Fallback: lokal fayl
        creds_path = Path("credentials.json")
        if creds_path.exists():
            creds = Credentials.from_service_account_file(str(creds_path), scopes=SCOPES)
            _client = gspread.authorize(creds)
            return _client

        log.warning("Google Sheets credentials topilmadi (env yoki fayl)")
        return None
    except Exception as e:
        log.error("Google Sheets autentifikatsiya xatosi: %s", e)
        return None


def _get_worksheet() -> gspread.Worksheet | None:
    """Spreadsheet ichidagi birinchi worksheetni olish. Headerlar yo'q bo'lsa qo'shadi."""
    if not GOOGLE_SHEETS_SPREADSHEET_ID:
        return None

    client = _get_client()
    if client is None:
        return None

    try:
        spreadsheet = client.open_by_key(GOOGLE_SHEETS_SPREADSHEET_ID)
        worksheet = spreadsheet.sheet1

        # Headerlarni tekshirish — bo'sh bo'lsa yozish
        first_row = worksheet.row_values(1)
        if not first_row:
            worksheet.append_row(HEADERS, value_input_option="USER_ENTERED")

        return worksheet
    except Exception as e:
        log.error("Google Sheets worksheet olishda xato: %s", e)
        return None


def append_registration(data: dict):
    """Ro'yxatdan o'tgan foydalanuvchi ma'lumotlarini Google Sheets ga qo'shish."""
    worksheet = _get_worksheet()
    if worksheet is None:
        return

    try:
        all_values = worksheet.get_all_values()
        row_number = len(all_values)

        now = datetime.now().strftime("%Y-%m-%d %H:%M")

        row = [
            row_number,
            data.get("full_name", ""),
            data.get("age", ""),
            data.get("phone", ""),
            data.get("goal", ""),
            data.get("video_watched", ""),
            data.get("preferred_time", ""),
            str(data.get("telegram_id", "")),
            now,
        ]
        worksheet.append_row(row, value_input_option="USER_ENTERED")
        log.info("Google Sheets: foydalanuvchi qo'shildi — %s", data.get("full_name"))
    except Exception as e:
        log.error("Google Sheets ga yozishda xato: %s", e)


def update_consultation(telegram_id: int, date_label: str, time_slot: str):
    """Foydalanuvchining konsultatsiya ma'lumotlarini Google Sheets da qo'shish."""
    worksheet = _get_worksheet()
    if worksheet is None:
        return

    try:
        cell = worksheet.find(str(telegram_id), in_column=8)
        if cell is None:
            log.warning("Google Sheets: telegram_id=%s topilmadi", telegram_id)
            return

        # Sana ustuniga konsultatsiya ma'lumotini qo'shish
        current = worksheet.cell(cell.row, 9).value or ""
        note = f"{current} | Konsult: {date_label} {time_slot}" if current else f"Konsult: {date_label} {time_slot}"
        worksheet.update_cell(cell.row, 9, note)
        log.info("Google Sheets: konsultatsiya yangilandi — %s", telegram_id)
    except Exception as e:
        log.error("Google Sheets konsultatsiya yangilashda xato: %s", e)


def migrate_existing_to_sheets():
    """Mavjud userlarni Google Sheets ga sinxronlash (deploy da).
    Sheet dagi telegram_id larni tekshirib, yo'qlarini qo'shadi."""
    worksheet = _get_worksheet()
    if worksheet is None:
        return

    try:
        from db import get_all_users_with_survey
        users = get_all_users_with_survey()
        if not users:
            return

        # Sheet dagi mavjud telegram_id larni olish
        all_values = worksheet.get_all_values()
        existing_ids = set()
        if len(all_values) > 1:
            tid_col = HEADERS.index("Telegram ID")
            for row in all_values[1:]:
                if len(row) > tid_col and row[tid_col]:
                    existing_ids.add(row[tid_col].strip())

        # Faqat sheet da yo'q userlarni qo'shish
        next_num = len(all_values)  # headers + mavjud qatorlar
        rows = []
        for u in users:
            tid = str(u.get("telegram_id", ""))
            if tid in existing_ids:
                continue
            next_num += 1
            rows.append([
                next_num - 1,
                u.get("full_name", ""),
                u.get("age") or "",
                u.get("phone", ""),
                u.get("goal") or u.get("exam_goal") or "",
                u.get("video_watched", ""),
                u.get("preferred_time") or "",
                tid,
                u.get("created_at", ""),
            ])

        if rows:
            worksheet.append_rows(rows, value_input_option="USER_ENTERED")
            log.info("Google Sheets: %d ta yangi user qo'shildi", len(rows))
        else:
            log.info("Google Sheets: barcha userlar allaqachon mavjud")
    except Exception as e:
        log.error("Google Sheets migrate xatosi: %s", e)
