"""Google Sheets integratsiyasi — ro'yxatdan o'tgan foydalanuvchilar va konsultatsiya ma'lumotlari."""

import logging
from datetime import datetime
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials

from config import GOOGLE_SHEETS_CREDENTIALS, GOOGLE_SHEETS_SPREADSHEET_ID

log = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

HEADERS = [
    "No", "Ism (Telegram)", "Telefon", "Maqsad/Natija",
    "Video ko'rganmi", "Qulay vaqt", "Telegram ID", "Sana",
]

_client: gspread.Client | None = None


def _get_client() -> gspread.Client | None:
    """Google Sheets clientni yaratish yoki cache dan qaytarish."""
    global _client
    if _client is not None:
        return _client

    creds_path = Path(GOOGLE_SHEETS_CREDENTIALS)
    if not creds_path.exists():
        log.warning("Google Sheets credentials fayli topilmadi: %s", creds_path)
        return None

    try:
        creds = Credentials.from_service_account_file(str(creds_path), scopes=SCOPES)
        _client = gspread.authorize(creds)
        return _client
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
        cell = worksheet.find(str(telegram_id), in_column=7)
        if cell is None:
            log.warning("Google Sheets: telegram_id=%s topilmadi", telegram_id)
            return

        # Sana ustuniga konsultatsiya ma'lumotini qo'shish
        current = worksheet.cell(cell.row, 8).value or ""
        note = f"{current} | Konsult: {date_label} {time_slot}" if current else f"Konsult: {date_label} {time_slot}"
        worksheet.update_cell(cell.row, 8, note)
        log.info("Google Sheets: konsultatsiya yangilandi — %s", telegram_id)
    except Exception as e:
        log.error("Google Sheets konsultatsiya yangilashda xato: %s", e)


def migrate_existing_to_sheets():
    """Mavjud userlarni bir martalik Google Sheets ga yozish (deploy da)."""
    worksheet = _get_worksheet()
    if worksheet is None:
        return

    try:
        all_values = worksheet.get_all_values()
        # Agar 2+ qator bo'lsa — allaqachon migrate qilingan
        if len(all_values) > 1:
            log.info("Google Sheets: ma'lumotlar allaqachon mavjud, migrate skip")
            return

        from db import get_all_users_with_survey
        users = get_all_users_with_survey()
        if not users:
            return

        rows = []
        for i, u in enumerate(users, 1):
            rows.append([
                i,
                u.get("full_name", ""),
                u.get("phone", ""),
                u.get("goal") or u.get("exam_goal") or "",
                u.get("video_watched", ""),
                u.get("preferred_time") or "",
                str(u.get("telegram_id", "")),
                u.get("created_at", ""),
            ])

        if rows:
            worksheet.append_rows(rows, value_input_option="USER_ENTERED")
            log.info("Google Sheets: %d ta mavjud user migrate qilindi", len(rows))
    except Exception as e:
        log.error("Google Sheets migrate xatosi: %s", e)
