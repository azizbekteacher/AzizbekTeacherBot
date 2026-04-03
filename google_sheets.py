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
    "№",
    "Ism",
    "Telefon",
    "Qo'shimcha tel",
    "Username",
    "Yosh",
    "Ish/O'qish joyi",
    "Sinab ko'rgan usullari",
    "Kurslar",
    "Imtihon rejasi",
    "Imtihon natijasi",
    "Muhimlik",
    "Natija ma'nosi",
    "Byudjet",
    "Video ko'rganmi",
    "Konsultatsiya kuni",
    "Konsultatsiya vaqti",
    "Telegram ID",
    "Ro'yxatdan o'tgan sana",
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
    """Ro'yxatdan o'tgan foydalanuvchi ma'lumotlarini Google Sheets ga qo'shish.

    data kalitlari: full_name, phone, extra_phone, username, age, workplace,
                    methods_tried, previous_courses, exam_plan, exam_goal,
                    importance, result_meaning, budget, video_watched, telegram_id
    """
    worksheet = _get_worksheet()
    if worksheet is None:
        return

    try:
        # Tartib raqamini aniqlash
        all_values = worksheet.get_all_values()
        row_number = len(all_values)  # header = 1-qator, shuning uchun len = keyingi №

        now = datetime.now().strftime("%Y-%m-%d %H:%M")

        row = [
            row_number,
            data.get("full_name", ""),
            data.get("phone", ""),
            data.get("extra_phone", ""),
            data.get("username", ""),
            data.get("age", ""),
            data.get("workplace", ""),
            data.get("methods_tried", ""),
            data.get("previous_courses", ""),
            data.get("exam_plan", ""),
            data.get("exam_goal", ""),
            data.get("importance", ""),
            data.get("result_meaning", ""),
            data.get("budget", ""),
            data.get("video_watched", ""),
            "",  # Konsultatsiya kuni — hali bo'sh
            "",  # Konsultatsiya vaqti — hali bo'sh
            str(data.get("telegram_id", "")),
            now,
        ]
        worksheet.append_row(row, value_input_option="USER_ENTERED")
        log.info("Google Sheets: foydalanuvchi qo'shildi — %s", data.get("full_name"))
    except Exception as e:
        log.error("Google Sheets ga yozishda xato: %s", e)


def update_consultation(telegram_id: int, date_label: str, time_slot: str):
    """Foydalanuvchining konsultatsiya kun va vaqtini Google Sheets da yangilash.

    telegram_id bo'yicha qatorni topib, konsultatsiya ustunlarini to'ldiradi.
    """
    worksheet = _get_worksheet()
    if worksheet is None:
        return

    try:
        # Telegram ID ustuni — 18-ustun (R)
        cell = worksheet.find(str(telegram_id), in_column=18)
        if cell is None:
            log.warning("Google Sheets: telegram_id=%s topilmadi", telegram_id)
            return

        # Konsultatsiya kuni — 16-ustun (P), vaqti — 17-ustun (Q)
        worksheet.update_cell(cell.row, 16, date_label)
        worksheet.update_cell(cell.row, 17, time_slot)
        log.info("Google Sheets: konsultatsiya yangilandi — %s, %s %s", telegram_id, date_label, time_slot)
    except Exception as e:
        log.error("Google Sheets konsultatsiya yangilashda xato: %s", e)
