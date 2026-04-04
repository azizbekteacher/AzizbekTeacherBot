import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "bot.db"


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_connection()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER UNIQUE NOT NULL,
            full_name TEXT NOT NULL,
            phone TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS bookings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES users(id),
            date TEXT NOT NULL,
            time_slot TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(date, time_slot)
        );

        CREATE TABLE IF NOT EXISTS admins (
            telegram_id INTEGER PRIMARY KEY,
            added_by INTEGER,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS scheduled_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER NOT NULL,
            message_type TEXT NOT NULL,
            send_at TEXT NOT NULL,
            sent INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS survey_answers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL UNIQUE REFERENCES users(id),
            username TEXT,
            age INTEGER,
            workplace TEXT,
            methods_tried TEXT,
            previous_courses TEXT,
            exam_plan TEXT,
            exam_goal TEXT,
            importance TEXT,
            result_meaning TEXT,
            budget TEXT,
            video_watched TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS testers (
            telegram_id INTEGER PRIMARY KEY,
            added_by INTEGER,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS bot_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            key TEXT UNIQUE NOT NULL,
            category TEXT NOT NULL,
            label TEXT NOT NULL,
            content_type TEXT NOT NULL DEFAULT 'text',
            text TEXT,
            media_file_id TEXT,
            schedule_delay_minutes INTEGER,
            is_active INTEGER NOT NULL DEFAULT 1,
            sort_order INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
    """)
    conn.commit()
    # Migration: extra_phone ustuni
    try:
        conn.execute("ALTER TABLE users ADD COLUMN extra_phone TEXT DEFAULT ''")
        conn.commit()
    except sqlite3.OperationalError:
        pass
    conn.close()


DEFAULT_MESSAGES = [
    # --- registration ---
    ("reg_welcome", "registration", "Welcome matn", "text",
     "<b>AzizbekTeacher</b> botiga xush kelibsiz!\n\nMarhamat, o'z <b>ism va familiyangizni</b> kiriting:",
     None, None),
    ("reg_name_prompt", "registration", "Ism so'rash", "text",
     "Marhamat, o'z <b>ism va familiyangizni</b> kiriting:", None, None),
    ("reg_name_error", "registration", "Ism xato", "text",
     "Iltimos, to'liq ism va familiyangizni kiriting:", None, None),
    ("reg_phone_prompt", "registration", "Telefon so'rash", "text",
     "Endi telefon raqamingizni kiriting yoki tugmani bosing:", None, None),
    ("reg_phone_error", "registration", "Telefon xato", "text",
     "Iltimos, to'g'ri telefon raqam kiriting yoki tugmani bosing:", None, None),
    ("reg_username_prompt", "registration", "Username so'rash", "text",
     "Telegram username ingizni kiriting (masalan: @username):", None, None),
    ("reg_username_error", "registration", "Username xato", "text",
     "Iltimos, username ni @ bilan boshlang (masalan: @username):", None, None),
    ("reg_age_prompt", "registration", "Yosh so'rash", "text",
     "Yoshingizni kiriting:", None, None),
    ("reg_age_error", "registration", "Yosh xato", "text",
     "Iltimos, faqat raqam kiriting (masalan: 20):", None, None),
    ("reg_workplace_prompt", "registration", "Ish joyi", "text",
     "O'qish yoki ish joyingiz qayerda?", None, None),
    ("reg_methods_prompt", "registration", "Usullar", "text",
     "Ilgari ingliz tilini o'rganish uchun qanday usullarni sinab ko'rgansiz?", None, None),
    ("reg_courses_prompt", "registration", "Kurslar", "text",
     "Online yoki offline kurslarda o'qiganmisiz? Qaysilarida?", None, None),
    ("reg_exam_prompt", "registration", "Imtihon", "text",
     "Qachon va qanday imtihon topshirmoqchisiz? (masalan: IELTS, CEFR, ...)", None, None),
    ("reg_exam_result_prompt", "registration", "Imtihon natija", "text",
     "Imtihonda qanday natijaga erishmoqchisiz?", None, None),
    ("reg_importance_prompt", "registration", "Muhimlik", "text",
     "Ingliz tilini o'rganish siz uchun qanchalik muhim?\n10 ballik shkala bo'yicha baholang:", None, None),
    ("reg_result_meaning_prompt", "registration", "Natija ma'nosi", "text",
     "Bu natija siz uchun nimani anglatadi? Qisqacha yozing:", None, None),
    ("reg_budget_prompt", "registration", "Byudjet", "text",
     "Ingliz tili kursiga oylik byudjetingiz qancha?", None, None),
    ("reg_video_prompt", "registration", "Video savoli", "text",
     "Videodarsni ko'rdingizmi?", None, None),
    ("reg_complete_yes", "registration", "Tugallash (Ha)", "text",
     "Siz ro'yxatdan muvaffaqiyatli o'tdingiz!\n\nVideo darsni ko'rganingiz uchun rahmat! Tez orada siz bilan bog'lanamiz.",
     None, None),
    ("reg_complete_no", "registration", "Tugallash (Yo'q)", "text",
     "Siz ro'yxatdan muvaffaqiyatli o'tdingiz!\n\nMarhamat, videodarsni ko'rib chiqing:\n\n<a href=\"{video_link}\">Darslikni ko'rish</a>\n\nTez orada siz bilan bog'lanamiz.",
     None, None),
    ("start_followup", "general", "Start followup (30 daq)", "text",
     "Siz hali konsultatsiyaga yozilmadingiz.\n\n"
     "Bepul konsultatsiya olish uchun \"Konsultatsiya olish\" tugmasini bosing!",
     None, 30),
    # --- followup xabarlar admin paneldan qo'shiladi ---
    # --- consultation ---
    ("consult_video_question", "consultation", "Video savoli", "text",
     "Videodarsini ko'rdingizmi?", None, None),
    ("consult_watch_first", "consultation", "Avval ko'ring", "text",
     "Iltimos, avval videodarsni ko'ring, so'ng konsultatsiyaga yoziling", None, None),
    ("consult_day_prompt", "consultation", "Kun tanlash", "text",
     "Marhamat, konsultatsiya kunini tanlang:", None, None),
    ("consult_time_prompt", "consultation", "Vaqt tanlash", "text",
     "<b>{day_label}</b> — vaqtni tanlang:", None, None),
    ("consult_success", "consultation", "Booking tasdiqlash", "text",
     "<b>Konsultatsiya muvaffaqiyatli band qilindi!</b>\n\nKun: {day_label}\nVaqt: {time_slot}\n\nTez orada siz bilan bog'lanamiz!",
     None, None),
    # --- general ---
    ("start_welcome", "general", "Start birinchi xabar", "text",
     "Siz ro'yxatdan muvaffaqiyatli o'tdingiz!\n\nMarhamat, videodarsni ko'rib chiqing:\n\n<a href=\"{video_link}\">Darslikni ko'rish</a>",
     None, None),
    ("welcome_back", "general", "Qayta xush kelibsiz", "text",
     "Xush kelibsiz, <b>{full_name}</b>!\n\nQuyidagi tugmalardan foydalaning:", None, None),
    ("default_message", "general", "Default javob", "text",
     "Quyidagi tugmalardan foydalaning:", None, None),
    ("not_registered", "general", "Ro'yxatdan o'tmagan", "text",
     "Botdan foydalanish uchun /start buyrug'ini bosing.", None, None),
    ("not_registered_alert", "general", "Ro'yxatdan o'tmagan (alert)", "text",
     "Avval /start orqali ro'yxatdan o'ting.", None, None),
    ("no_days_available", "general", "Bo'sh kun yo'q", "text",
     "Hozirda bo'sh kun yo'q. Iltimos, keyinroq urinib ko'ring.", None, None),
]


def seed_bot_messages():
    conn = get_connection()
    for row in DEFAULT_MESSAGES:
        conn.execute(
            """INSERT OR IGNORE INTO bot_messages
               (key, category, label, content_type, text, media_file_id, schedule_delay_minutes)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            row,
        )
    conn.commit()
    conn.close()


def get_msg(key: str) -> dict | None:
    conn = get_connection()
    row = conn.execute("SELECT * FROM bot_messages WHERE key = ?", (key,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_msg_text(key: str, fallback: str = "") -> str:
    msg = get_msg(key)
    if msg and msg.get("text"):
        return msg["text"]
    # Fallback: DEFAULT_MESSAGES dan qidirish
    for row in DEFAULT_MESSAGES:
        if row[0] == key and row[4]:
            return row[4]
    return fallback


def get_messages_by_category(category: str) -> list[dict]:
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM bot_messages WHERE category = ? ORDER BY sort_order, id",
        (category,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_msg_text(key: str, text: str):
    conn = get_connection()
    conn.execute(
        "UPDATE bot_messages SET text = ?, updated_at = datetime('now') WHERE key = ?",
        (text, key),
    )
    conn.commit()
    conn.close()


def update_msg_media(key: str, file_id: str, content_type: str):
    conn = get_connection()
    conn.execute(
        "UPDATE bot_messages SET media_file_id = ?, content_type = ?, updated_at = datetime('now') WHERE key = ?",
        (file_id, content_type, key),
    )
    conn.commit()
    conn.close()


def update_msg_delay(key: str, minutes: int):
    conn = get_connection()
    conn.execute(
        "UPDATE bot_messages SET schedule_delay_minutes = ?, updated_at = datetime('now') WHERE key = ?",
        (minutes, key),
    )
    conn.commit()
    conn.close()


def toggle_msg_active(key: str) -> bool:
    conn = get_connection()
    row = conn.execute("SELECT is_active FROM bot_messages WHERE key = ?", (key,)).fetchone()
    if not row:
        conn.close()
        return False
    new_val = 0 if row["is_active"] else 1
    conn.execute(
        "UPDATE bot_messages SET is_active = ?, updated_at = datetime('now') WHERE key = ?",
        (new_val, key),
    )
    conn.commit()
    conn.close()
    return True


def create_custom_message(key: str, label: str, category: str, text: str | None = None,
                          content_type: str = "text", media_file_id: str | None = None,
                          schedule_delay_minutes: int | None = None) -> bool:
    conn = get_connection()
    try:
        conn.execute(
            """INSERT INTO bot_messages (key, category, label, content_type, text, media_file_id, schedule_delay_minutes)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (key, category, label, content_type, text, media_file_id, schedule_delay_minutes),
        )
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        conn.close()
        return False


def delete_message(key: str) -> bool:
    conn = get_connection()
    cur = conn.execute("DELETE FROM bot_messages WHERE key = ?", (key,))
    conn.commit()
    deleted = cur.rowcount > 0
    conn.close()
    return deleted


def clear_category_messages(category: str) -> int:
    """Kategoriya bo'yicha barcha xabarlarni o'chirish."""
    conn = get_connection()
    cur = conn.execute("DELETE FROM bot_messages WHERE category = ?", (category,))
    conn.commit()
    count = cur.rowcount
    conn.close()
    return count


def get_start_messages() -> list[dict]:
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM bot_messages WHERE category = 'start' AND is_active = 1 ORDER BY sort_order, id",
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_followup_messages() -> list[dict]:
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM bot_messages WHERE category = 'followup' AND is_active = 1 ORDER BY schedule_delay_minutes",
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def migrate_scheduled_users():
    """scheduled_messages dagi telegram_id larni users jadvaliga qo'shish (agar yo'q bo'lsa)."""
    conn = get_connection()
    conn.execute("""
        INSERT OR IGNORE INTO users (telegram_id, full_name, phone)
        SELECT DISTINCT telegram_id, 'Noma''lum', ''
        FROM scheduled_messages
        WHERE telegram_id NOT IN (SELECT telegram_id FROM users)
    """)
    conn.commit()
    conn.close()


def seed_admins(admin_ids: list[int]):
    conn = get_connection()
    for tid in admin_ids:
        conn.execute(
            "INSERT OR IGNORE INTO admins (telegram_id) VALUES (?)", (tid,)
        )
    conn.commit()
    conn.close()


def add_admin(telegram_id: int, added_by: int) -> bool:
    conn = get_connection()
    try:
        conn.execute(
            "INSERT INTO admins (telegram_id, added_by) VALUES (?, ?)",
            (telegram_id, added_by),
        )
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        conn.close()
        return False


def remove_admin(telegram_id: int) -> bool:
    conn = get_connection()
    cur = conn.execute("DELETE FROM admins WHERE telegram_id = ?", (telegram_id,))
    conn.commit()
    removed = cur.rowcount > 0
    conn.close()
    return removed


def get_admin_ids() -> list[int]:
    conn = get_connection()
    rows = conn.execute("SELECT telegram_id FROM admins").fetchall()
    conn.close()
    return [r["telegram_id"] for r in rows]


def is_admin(telegram_id: int) -> bool:
    conn = get_connection()
    row = conn.execute("SELECT 1 FROM admins WHERE telegram_id = ?", (telegram_id,)).fetchone()
    conn.close()
    return row is not None


def add_tester(telegram_id: int, added_by: int) -> bool:
    conn = get_connection()
    try:
        conn.execute(
            "INSERT INTO testers (telegram_id, added_by) VALUES (?, ?)",
            (telegram_id, added_by),
        )
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        conn.close()
        return False


def remove_tester(telegram_id: int) -> bool:
    conn = get_connection()
    cur = conn.execute("DELETE FROM testers WHERE telegram_id = ?", (telegram_id,))
    conn.commit()
    removed = cur.rowcount > 0
    conn.close()
    return removed


def get_tester_ids() -> list[int]:
    conn = get_connection()
    rows = conn.execute("SELECT telegram_id FROM testers").fetchall()
    conn.close()
    return [r["telegram_id"] for r in rows]


def is_tester(telegram_id: int) -> bool:
    conn = get_connection()
    row = conn.execute("SELECT 1 FROM testers WHERE telegram_id = ?", (telegram_id,)).fetchone()
    conn.close()
    return row is not None


def reset_user_data(telegram_id: int):
    """Test akkaunt uchun — barcha ma'lumotlarni tozalaydi (user, survey, bookings, scheduled)."""
    conn = get_connection()
    user = conn.execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,)).fetchone()
    if user:
        user_id = user["id"]
        conn.execute("DELETE FROM survey_answers WHERE user_id = ?", (user_id,))
        conn.execute("DELETE FROM bookings WHERE user_id = ?", (user_id,))
        conn.execute("DELETE FROM users WHERE id = ?", (user_id,))
    conn.execute("DELETE FROM scheduled_messages WHERE telegram_id = ?", (telegram_id,))
    conn.commit()
    conn.close()


def save_user(telegram_id: int, full_name: str, phone: str) -> int:
    conn = get_connection()
    cur = conn.execute(
        """INSERT INTO users (telegram_id, full_name, phone)
           VALUES (?, ?, ?)
           ON CONFLICT(telegram_id) DO UPDATE SET full_name=excluded.full_name, phone=excluded.phone
           RETURNING id""",
        (telegram_id, full_name, phone),
    )
    user_id = cur.fetchone()[0]
    conn.commit()
    conn.close()
    return user_id


def save_user_extra_phone(telegram_id: int, extra_phone: str):
    conn = get_connection()
    conn.execute(
        "UPDATE users SET extra_phone = ? WHERE telegram_id = ?",
        (extra_phone, telegram_id),
    )
    conn.commit()
    conn.close()


def get_user_by_telegram_id(telegram_id: int) -> dict | None:
    conn = get_connection()
    row = conn.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def create_booking(telegram_id: int, date: str, time_slot: str) -> int | None:
    conn = get_connection()
    user = conn.execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,)).fetchone()
    if not user:
        conn.close()
        return None
    try:
        cur = conn.execute(
            "INSERT INTO bookings (user_id, date, time_slot) VALUES (?, ?, ?)",
            (user["id"], date, time_slot),
        )
        booking_id = cur.lastrowid
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        return None
    conn.close()
    return booking_id


def get_booked_slots(date: str) -> list[str]:
    conn = get_connection()
    rows = conn.execute("SELECT time_slot FROM bookings WHERE date = ?", (date,)).fetchall()
    conn.close()
    return [r["time_slot"] for r in rows]


def get_booking_with_user(booking_id: int) -> dict | None:
    conn = get_connection()
    row = conn.execute(
        """SELECT b.id, b.date, b.time_slot, b.created_at,
                  u.full_name, u.phone, u.extra_phone, u.telegram_id
           FROM bookings b JOIN users u ON b.user_id = u.id
           WHERE b.id = ?""",
        (booking_id,),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


# --- Booking management ---


def get_user_active_booking(telegram_id: int) -> dict | None:
    conn = get_connection()
    today = datetime.now().strftime("%Y-%m-%d")
    row = conn.execute(
        """SELECT b.id, b.date, b.time_slot, b.created_at
           FROM bookings b JOIN users u ON b.user_id = u.id
           WHERE u.telegram_id = ? AND b.date >= ?
           ORDER BY b.date, b.time_slot LIMIT 1""",
        (telegram_id, today),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def cancel_booking(booking_id: int) -> bool:
    conn = get_connection()
    cur = conn.execute("DELETE FROM bookings WHERE id = ?", (booking_id,))
    conn.commit()
    deleted = cur.rowcount > 0
    conn.close()
    return deleted


# --- Duplicate checks ---


def check_phone_exists(phone: str, exclude_telegram_id: int) -> dict | None:
    conn = get_connection()
    rows = conn.execute(
        "SELECT telegram_id, full_name, phone FROM users WHERE telegram_id != ?",
        (exclude_telegram_id,),
    ).fetchall()
    conn.close()
    clean = phone.replace("+", "").replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    if len(clean) < 9:
        return None
    suffix = clean[-9:]
    for r in rows:
        existing = r["phone"].replace("+", "").replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
        if len(existing) >= 9 and existing[-9:] == suffix:
            return dict(r)
    return None


def check_username_exists(username: str, exclude_telegram_id: int) -> dict | None:
    conn = get_connection()
    row = conn.execute(
        """SELECT u.telegram_id, u.full_name FROM survey_answers sa
           JOIN users u ON sa.user_id = u.id
           WHERE LOWER(sa.username) = LOWER(?) AND u.telegram_id != ?""",
        (username, exclude_telegram_id),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


# --- Scheduled messages ---


def schedule_followup_messages(telegram_id: int):
    conn = get_connection()
    # Agar allaqachon rejaga olingan bo'lsa — qayta qo'shmaslik
    existing = conn.execute(
        "SELECT 1 FROM scheduled_messages WHERE telegram_id = ?",
        (telegram_id,),
    ).fetchone()
    if existing:
        conn.close()
        return
    now = datetime.now()
    followups = get_followup_messages()
    for fu in followups:
        delay = fu.get("schedule_delay_minutes") or 30
        send_at = now + timedelta(minutes=delay)
        conn.execute(
            "INSERT INTO scheduled_messages (telegram_id, message_type, send_at) VALUES (?, ?, ?)",
            (telegram_id, fu["key"], send_at.strftime("%Y-%m-%d %H:%M:%S")),
        )
    conn.commit()
    conn.close()


def schedule_start_followup(telegram_id: int):
    """Start bosilganda 30 daqiqalik followup xabar rejaga olish."""
    conn = get_connection()
    # Agar allaqachon start_followup rejaga olingan bo'lsa — qayta qo'shmaslik
    existing = conn.execute(
        "SELECT 1 FROM scheduled_messages WHERE telegram_id = ? AND message_type = 'start_followup' AND sent = 0",
        (telegram_id,),
    ).fetchone()
    if existing:
        conn.close()
        return
    now = datetime.now()
    send_at = now + timedelta(minutes=30)
    conn.execute(
        "INSERT INTO scheduled_messages (telegram_id, message_type, send_at) VALUES (?, ?, ?)",
        (telegram_id, "start_followup", send_at.strftime("%Y-%m-%d %H:%M:%S")),
    )
    conn.commit()
    conn.close()


def cancel_pending_followups(telegram_id: int):
    """Yuborilmagan start_followup xabarlarni bekor qilish."""
    conn = get_connection()
    conn.execute(
        "DELETE FROM scheduled_messages WHERE telegram_id = ? AND message_type = 'start_followup' AND sent = 0",
        (telegram_id,),
    )
    conn.commit()
    conn.close()


def is_followup_consult_sent(telegram_id: int) -> bool:
    conn = get_connection()
    row = conn.execute(
        "SELECT 1 FROM scheduled_messages WHERE telegram_id = ? AND message_type = 'followup_consult' AND sent = 1",
        (telegram_id,),
    ).fetchone()
    conn.close()
    return row is not None


def get_pending_messages() -> list[dict]:
    conn = get_connection()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = conn.execute(
        "SELECT id, telegram_id, message_type FROM scheduled_messages WHERE sent = 0 AND send_at <= ?",
        (now,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def mark_message_sent(message_id: int):
    conn = get_connection()
    conn.execute("UPDATE scheduled_messages SET sent = 1 WHERE id = ?", (message_id,))
    conn.commit()
    conn.close()


# --- Stats & users ---


def get_user_count() -> int:
    conn = get_connection()
    count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    conn.close()
    return count


def get_all_user_ids() -> list[int]:
    conn = get_connection()
    rows = conn.execute("SELECT telegram_id FROM users").fetchall()
    conn.close()
    return [r["telegram_id"] for r in rows]


def get_stats() -> dict:
    conn = get_connection()
    users_count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    bookings_count = conn.execute("SELECT COUNT(*) FROM bookings").fetchone()[0]
    today = datetime.now().strftime("%Y-%m-%d")
    today_bookings = conn.execute(
        "SELECT COUNT(*) FROM bookings WHERE date = ?", (today,)
    ).fetchone()[0]
    pending_msgs = conn.execute(
        "SELECT COUNT(*) FROM scheduled_messages WHERE sent = 0"
    ).fetchone()[0]
    conn.close()
    return {
        "users_count": users_count,
        "bookings_count": bookings_count,
        "today_bookings": today_bookings,
        "pending_msgs": pending_msgs,
    }


def save_survey_answers(user_id: int, answers: dict):
    conn = get_connection()
    conn.execute(
        """INSERT OR REPLACE INTO survey_answers
           (user_id, username, age, workplace, methods_tried, previous_courses,
            exam_plan, exam_goal, importance, result_meaning, budget, video_watched)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            user_id,
            answers.get("username"),
            answers.get("age"),
            answers.get("workplace"),
            answers.get("methods_tried"),
            answers.get("previous_courses"),
            answers.get("exam_plan"),
            answers.get("exam_goal"),
            answers.get("importance"),
            answers.get("result_meaning"),
            answers.get("budget"),
            answers.get("video_watched"),
        ),
    )
    conn.commit()
    conn.close()


def get_survey_answers(telegram_id: int) -> dict | None:
    conn = get_connection()
    row = conn.execute(
        """SELECT sa.* FROM survey_answers sa
           JOIN users u ON sa.user_id = u.id
           WHERE u.telegram_id = ?""",
        (telegram_id,),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def get_bookings_by_date(date: str) -> list[dict]:
    conn = get_connection()
    rows = conn.execute(
        """SELECT b.id AS booking_id, b.date, b.time_slot, b.created_at AS booking_created,
                  u.full_name, u.phone, u.extra_phone, u.telegram_id
           FROM bookings b JOIN users u ON b.user_id = u.id
           WHERE b.date = ?
           ORDER BY b.time_slot""",
        (date,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_booking_detail_with_survey(booking_id: int) -> dict | None:
    conn = get_connection()
    row = conn.execute(
        """SELECT b.id AS booking_id, b.date, b.time_slot,
                  u.telegram_id, u.full_name, u.phone, u.extra_phone, u.created_at,
                  sa.username, sa.age, sa.workplace, sa.methods_tried,
                  sa.previous_courses, sa.exam_plan, sa.exam_goal,
                  sa.importance, sa.result_meaning, sa.budget, sa.video_watched
           FROM bookings b
           JOIN users u ON b.user_id = u.id
           LEFT JOIN survey_answers sa ON sa.user_id = u.id
           WHERE b.id = ?""",
        (booking_id,),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def get_week_booking_counts(start_date: str, days: int = 7) -> list[dict]:
    conn = get_connection()
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    result = []
    for i in range(days):
        day = start + timedelta(days=i)
        date_str = day.strftime("%Y-%m-%d")
        count = conn.execute(
            "SELECT COUNT(*) FROM bookings WHERE date = ?", (date_str,)
        ).fetchone()[0]
        result.append({"date": date_str, "count": count})
    conn.close()
    return result


def get_all_users_with_survey() -> list[dict]:
    conn = get_connection()
    rows = conn.execute(
        """SELECT u.telegram_id, u.full_name, u.phone, u.extra_phone, u.created_at,
                  sa.username, sa.age, sa.workplace, sa.methods_tried,
                  sa.previous_courses, sa.exam_plan, sa.exam_goal,
                  sa.importance, sa.result_meaning, sa.budget, sa.video_watched
           FROM users u
           LEFT JOIN survey_answers sa ON sa.user_id = u.id
           ORDER BY u.created_at DESC"""
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_user_survey_by_telegram_id(telegram_id: int) -> dict | None:
    conn = get_connection()
    row = conn.execute(
        """SELECT u.telegram_id, u.full_name, u.phone, u.extra_phone, u.created_at,
                  sa.username, sa.age, sa.workplace, sa.methods_tried,
                  sa.previous_courses, sa.exam_plan, sa.exam_goal,
                  sa.importance, sa.result_meaning, sa.budget, sa.video_watched
           FROM users u
           LEFT JOIN survey_answers sa ON sa.user_id = u.id
           WHERE u.telegram_id = ?""",
        (telegram_id,),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def get_users_without_survey() -> list[int]:
    conn = get_connection()
    rows = conn.execute(
        """SELECT u.telegram_id FROM users u
           LEFT JOIN survey_answers sa ON sa.user_id = u.id
           WHERE sa.id IS NULL"""
    ).fetchall()
    conn.close()
    return [r["telegram_id"] for r in rows]


def get_recent_users(limit: int = 20) -> list[dict]:
    conn = get_connection()
    rows = conn.execute(
        "SELECT telegram_id, full_name, phone, created_at FROM users ORDER BY created_at DESC LIMIT ?",
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_detailed_stats() -> dict:
    conn = get_connection()
    users_count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    surveys_count = conn.execute("SELECT COUNT(*) FROM survey_answers").fetchone()[0]
    bookings_count = conn.execute("SELECT COUNT(*) FROM bookings").fetchone()[0]
    today = datetime.now().strftime("%Y-%m-%d")
    today_bookings = conn.execute(
        "SELECT COUNT(*) FROM bookings WHERE date = ?", (today,)
    ).fetchone()[0]
    pending_msgs = conn.execute(
        "SELECT COUNT(*) FROM scheduled_messages WHERE sent = 0"
    ).fetchone()[0]
    sent_msgs = conn.execute(
        "SELECT COUNT(*) FROM scheduled_messages WHERE sent = 1"
    ).fetchone()[0]
    admins_count = conn.execute("SELECT COUNT(*) FROM admins").fetchone()[0]
    conn.close()
    return {
        "users_count": users_count,
        "surveys_count": surveys_count,
        "bookings_count": bookings_count,
        "today_bookings": today_bookings,
        "pending_msgs": pending_msgs,
        "sent_msgs": sent_msgs,
        "admins_count": admins_count,
    }


def get_users_paginated(offset: int = 0, limit: int = 10) -> list[dict]:
    conn = get_connection()
    rows = conn.execute(
        """SELECT u.telegram_id, u.full_name, u.phone, u.created_at,
                  sa.username
           FROM users u
           LEFT JOIN survey_answers sa ON sa.user_id = u.id
           ORDER BY u.created_at DESC
           LIMIT ? OFFSET ?""",
        (limit, offset),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def search_users(query: str, limit: int = 20) -> list[dict]:
    conn = get_connection()
    pattern = f"%{query}%"
    rows = conn.execute(
        """SELECT u.telegram_id, u.full_name, u.phone, u.created_at,
                  sa.username
           FROM users u
           LEFT JOIN survey_answers sa ON sa.user_id = u.id
           WHERE u.full_name LIKE ? OR u.phone LIKE ?
                 OR CAST(u.telegram_id AS TEXT) LIKE ?
                 OR sa.username LIKE ?
           ORDER BY u.created_at DESC
           LIMIT ?""",
        (pattern, pattern, pattern, pattern, limit),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]
