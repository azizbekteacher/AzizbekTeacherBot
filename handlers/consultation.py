from datetime import datetime, timedelta

from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

from config import VIDEO_LINK
from db import get_booked_slots, create_booking, get_booking_with_user, get_user_by_telegram_id, get_admin_ids, get_msg_text, cancel_booking

router = Router()

WEEKDAY_NAMES = ["Dushanba", "Seshanba", "Chorshanba", "Payshanba", "Juma", "Shanba", "Yakshanba"]
MONTH_NAMES = [
    "", "yanvar", "fevral", "mart", "aprel", "may", "iyun",
    "iyul", "avgust", "sentabr", "oktabr", "noyabr", "dekabr",
]

COMMON_BREAKS = {"16:30", "18:30", "20:00"}
ODD_DAY_EXTRA_BREAKS = {"20:30", "21:00"}

# Toq kunlar: dushanba(0), chorshanba(2), juma(4), yakshanba(6)
ODD_WEEKDAYS = {0, 2, 4, 6}


class Consultation(StatesGroup):
    watched_video = State()
    select_day = State()
    select_time = State()


def generate_all_slots() -> list[str]:
    slots = []
    hour, minute = 14, 0
    while (hour, minute) <= (21, 30):
        slots.append(f"{hour:02d}:{minute:02d}")
        minute += 30
        if minute == 60:
            hour += 1
            minute = 0
    return slots


ALL_SLOTS = generate_all_slots()


def get_available_slots(date_str: str) -> list[str]:
    date = datetime.strptime(date_str, "%Y-%m-%d").date()
    weekday = date.weekday()

    breaks = COMMON_BREAKS.copy()
    if weekday in ODD_WEEKDAYS:
        breaks |= ODD_DAY_EXTRA_BREAKS

    booked = set(get_booked_slots(date_str))
    available = [s for s in ALL_SLOTS if s not in breaks and s not in booked]

    # Bugun bo'lsa — o'tgan vaqtlarni chiqarib tashlash
    if date == datetime.now().date():
        now_time = datetime.now().strftime("%H:%M")
        available = [s for s in available if s > now_time]

    return available


def format_date_uz(date_str: str) -> str:
    date = datetime.strptime(date_str, "%Y-%m-%d").date()
    weekday_name = WEEKDAY_NAMES[date.weekday()]
    return f"{weekday_name}, {date.day}-{MONTH_NAMES[date.month]}"


def build_days_keyboard() -> InlineKeyboardMarkup:
    today = datetime.now().date()
    buttons = []
    for i in range(0, 8):
        day = today + timedelta(days=i)
        date_str = day.strftime("%Y-%m-%d")
        available = get_available_slots(date_str)
        if not available:
            continue
        label = format_date_uz(date_str)
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"day:{date_str}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def build_time_keyboard(date_str: str) -> InlineKeyboardMarkup:
    slots = get_available_slots(date_str)
    buttons = []
    row = []
    for slot in slots:
        row.append(InlineKeyboardButton(text=slot, callback_data=f"time:{date_str}:{slot}"))
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton(text="Orqaga", callback_data="back_to_days")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# --- Handlers ---


@router.callback_query(F.data == "consultation_start")
async def on_consultation_start(callback: CallbackQuery, state: FSMContext):
    user = get_user_by_telegram_id(callback.from_user.id)
    if not user:
        await callback.answer(get_msg_text("not_registered_alert"), show_alert=True)
        return

    from handlers.start import send_bot_msg
    await state.set_state(Consultation.watched_video)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Ha", callback_data="video_yes"),
            InlineKeyboardButton(text="Yo'q", callback_data="video_no"),
        ]
    ])
    await send_bot_msg(callback.message, "consult_video_question", reply_markup=kb, edit=True)
    await callback.answer()


@router.callback_query(Consultation.watched_video, F.data == "video_no")
async def on_video_no(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    from handlers.start import send_bot_msg
    inline_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Videoni ko'rish", url=VIDEO_LINK)]
    ])
    await send_bot_msg(callback.message, "start_welcome", reply_markup=inline_kb,
                       edit=True, video_link=VIDEO_LINK)
    await callback.answer()


@router.callback_query(Consultation.watched_video, F.data == "video_yes")
async def on_video_yes(callback: CallbackQuery, state: FSMContext):
    from handlers.start import send_bot_msg
    await state.set_state(Consultation.select_day)
    kb = build_days_keyboard()
    if not kb.inline_keyboard:
        await send_bot_msg(callback.message, "no_days_available", edit=True)
        await state.clear()
        await callback.answer()
        return
    await send_bot_msg(callback.message, "consult_day_prompt", reply_markup=kb, edit=True)
    await callback.answer()


@router.callback_query(Consultation.select_day, F.data.startswith("day:"))
async def on_day_selected(callback: CallbackQuery, state: FSMContext):
    from handlers.start import send_bot_msg
    date_str = callback.data.split(":", 1)[1]
    await state.update_data(selected_date=date_str)
    await state.set_state(Consultation.select_time)

    kb = build_time_keyboard(date_str)
    day_label = format_date_uz(date_str)
    await send_bot_msg(callback.message, "consult_time_prompt", reply_markup=kb,
                       edit=True, day_label=day_label)
    await callback.answer()


@router.callback_query(Consultation.select_time, F.data == "back_to_days")
async def on_back_to_days(callback: CallbackQuery, state: FSMContext):
    from handlers.start import send_bot_msg
    await state.set_state(Consultation.select_day)
    kb = build_days_keyboard()
    await send_bot_msg(callback.message, "consult_day_prompt", reply_markup=kb, edit=True)
    await callback.answer()


@router.callback_query(Consultation.select_time, F.data.startswith("time:"))
async def on_time_selected(callback: CallbackQuery, state: FSMContext):
    from handlers.start import send_bot_msg
    parts = callback.data.split(":")
    date_str = parts[1]
    time_slot = f"{parts[2]}:{parts[3]}"

    booking_id = create_booking(callback.from_user.id, date_str, time_slot)
    if booking_id is None:
        await callback.answer("Bu vaqt allaqachon band. Boshqa vaqt tanlang.", show_alert=True)
        data = await state.get_data()
        date_str = data.get("selected_date", date_str)
        kb = build_time_keyboard(date_str)
        day_label = format_date_uz(date_str)
        await send_bot_msg(callback.message, "consult_time_prompt", reply_markup=kb,
                           edit=True, day_label=day_label)
        return

    await state.clear()
    day_label = format_date_uz(date_str)
    await send_bot_msg(callback.message, "consult_success", edit=True,
                       day_label=day_label, time_slot=time_slot)
    await callback.answer()

    # Admin(lar)ga xabar
    booking = get_booking_with_user(booking_id)
    if booking:
        extra = f"\nQo'shimcha: {booking['extra_phone']}" if booking.get('extra_phone') else ""
        admin_text = (
            f"<b>Yangi konsultatsiya!</b>\n\n"
            f"{booking['full_name']}\n"
            f"Tel: {booking['phone']}{extra}\n"
            f"Kun: {day_label}\n"
            f"Vaqt: {time_slot}\n"
            f"Telegram: {booking['telegram_id']}"
        )
        for admin_id in get_admin_ids():
            try:
                await callback.bot.send_message(admin_id, admin_text)
            except Exception:
                pass


# --- Booking management ---


@router.callback_query(F.data.startswith("booking:cancel:"))
async def on_booking_cancel(callback: CallbackQuery, state: FSMContext):
    booking_id = int(callback.data.split(":")[2])
    booking = get_booking_with_user(booking_id)

    if not booking or booking["telegram_id"] != callback.from_user.id:
        await callback.answer("Booking topilmadi.", show_alert=True)
        return

    day_label = format_date_uz(booking["date"])
    cancel_booking(booking_id)
    await state.clear()

    from handlers.start import main_menu_kb
    await callback.message.edit_text(
        f"Konsultatsiya bekor qilindi.\n\n"
        f"Kun: {day_label}\n"
        f"Vaqt: {booking['time_slot']}\n\n"
        "Qayta band qilish uchun \"Konsultatsiya olish\" tugmasini bosing."
    )

    # Adminlarga xabar
    admin_text = (
        f"<b>Konsultatsiya bekor qilindi!</b>\n\n"
        f"{booking['full_name']}\n"
        f"Kun: {day_label}\n"
        f"Vaqt: {booking['time_slot']}"
    )
    for admin_id in get_admin_ids():
        try:
            await callback.bot.send_message(admin_id, admin_text)
        except Exception:
            pass

    await callback.answer()


@router.callback_query(F.data.startswith("booking:change:"))
async def on_booking_change(callback: CallbackQuery, state: FSMContext):
    booking_id = int(callback.data.split(":")[2])
    booking = get_booking_with_user(booking_id)

    if not booking or booking["telegram_id"] != callback.from_user.id:
        await callback.answer("Booking topilmadi.", show_alert=True)
        return

    old_day = format_date_uz(booking["date"])
    old_time = booking["time_slot"]
    cancel_booking(booking_id)

    # Yangi kun tanlash
    await state.set_state(Consultation.select_day)
    kb = build_days_keyboard()
    if not kb.inline_keyboard:
        await callback.message.edit_text(get_msg_text("no_days_available"))
        await state.clear()
    else:
        await callback.message.edit_text(
            f"Eski vaqt ({old_day}, {old_time}) bekor qilindi.\n\n"
            f"{get_msg_text('consult_day_prompt')}",
            reply_markup=kb,
        )

    # Adminlarga xabar
    admin_text = (
        f"<b>Konsultatsiya o'zgartirilmoqda!</b>\n\n"
        f"{booking['full_name']}\n"
        f"Eski: {old_day}, {old_time}"
    )
    for admin_id in get_admin_ids():
        try:
            await callback.bot.send_message(admin_id, admin_text)
        except Exception:
            pass

    await callback.answer()
