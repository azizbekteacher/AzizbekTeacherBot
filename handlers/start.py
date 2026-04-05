from aiogram import Router, F
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove,
    ForceReply,
)

from google_sheets import append_registration
from db import (
    save_user, get_user_by_telegram_id, is_admin,
    schedule_followup_messages, save_survey_answers, get_survey_answers,
    get_msg, get_msg_text, get_user_active_booking,
    check_phone_exists, get_admin_ids, get_start_messages,
    schedule_start_followup, cancel_pending_followups,
    is_tester, reset_user_data,
)

router = Router()


async def send_bot_msg(target: Message, key: str, reply_markup=None, edit: bool = False,
                       send_companion: bool = False, **fmt):
    """bot_messages jadvalidan xabarni content_type ga qarab yuborish (photo, video, voice, text).
    send_companion=True bo'lsa, {key}_extra xabar ham yuboriladi (agar mavjud va faol bo'lsa)."""
    msg_data = get_msg(key)
    text = get_msg_text(key)
    if fmt and text:
        try:
            text = text.format(**fmt)
        except (KeyError, IndexError):
            pass

    file_id = msg_data.get("media_file_id") if msg_data else None
    content_type = msg_data.get("content_type", "text") if msg_data else "text"
    has_media = file_id and content_type in ("photo", "video", "voice", "document")

    if edit and not has_media:
        if text:
            await target.edit_text(text, reply_markup=reply_markup)
        if send_companion:
            extra_msg = get_msg(f"{key}_extra")
            if extra_msg and extra_msg.get("is_active", 1):
                await send_bot_msg(target, f"{key}_extra")
        return

    if edit and has_media:
        try:
            await target.delete()
        except Exception:
            pass

    if content_type == "photo" and file_id:
        await target.answer_photo(file_id, caption=text or None, reply_markup=reply_markup)
    elif content_type == "video" and file_id:
        await target.answer_video(file_id, caption=text or None, reply_markup=reply_markup)
    elif content_type == "voice" and file_id:
        await target.answer_voice(file_id, reply_markup=reply_markup)
    elif content_type == "document" and file_id:
        await target.answer_document(file_id, caption=text or None, reply_markup=reply_markup)
    elif text:
        await target.answer(text, reply_markup=reply_markup)

    if send_companion and not edit:
        extra_msg = get_msg(f"{key}_extra")
        if extra_msg and extra_msg.get("is_active", 1):
            await send_bot_msg(target, f"{key}_extra")


def main_menu_kb(user_id: int) -> ReplyKeyboardMarkup | ReplyKeyboardRemove:
    if is_admin(user_id):
        buttons = [[KeyboardButton(text="Admin panel")]]
        return ReplyKeyboardMarkup(
            keyboard=buttons,
            resize_keyboard=True,
            is_persistent=True,
            input_field_placeholder="Buyruq yoki tugmani tanlang...",
        )

    buttons = [[KeyboardButton(text="Konsultatsiya olish")]]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


class Registration(StatesGroup):
    waiting_for_name = State()       # 1 (welcome xabari bilan birga)
    waiting_for_phone = State()      # 2
    waiting_for_goal = State()       # 3
    waiting_for_video = State()      # 4 (Ha/Yo'q inline)
    waiting_for_time = State()       # 5


async def finish_registration(target, state: FSMContext, user_telegram_id: int):
    """Registration tugashi — ma'lumotlarni saqlash va yakuniy xabar."""
    data = await state.get_data()
    await state.clear()

    full_name = data.get("full_name", "Noma'lum")
    phone = data.get("phone", "")

    user_id = save_user(user_telegram_id, full_name, phone)
    save_survey_answers(user_id, {
        "goal": data.get("goal"),
        "video_watched": data.get("video_watched"),
        "preferred_time": data.get("preferred_time"),
    })
    # Start followup bekor qilish + yangi followup rejalashtirish
    cancel_pending_followups(user_telegram_id)
    schedule_followup_messages(user_telegram_id)

    # Google Sheets ga yozish
    append_registration({
        "full_name": full_name,
        "phone": phone,
        "goal": data.get("goal", ""),
        "video_watched": data.get("video_watched", ""),
        "preferred_time": data.get("preferred_time", ""),
        "telegram_id": user_telegram_id,
    })

    # Yakuniy xabar
    await send_bot_msg(target, "reg_complete",
                       reply_markup=main_menu_kb(user_telegram_id),
                       send_companion=True)

    # Adminlarga xabar
    admin_text = (
        f"<b>Yangi so'rovnoma to'ldirildi!</b>\n\n"
        f"Ism: {full_name}\n"
        f"Tel: {phone}\n"
        f"Telegram: <code>{user_telegram_id}</code>"
    )
    for admin_id in get_admin_ids():
        try:
            await target.bot.send_message(admin_id, admin_text)
        except Exception:
            pass


# --- /start ---

@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    # Admin — so'rovnomasiz, faqat admin panel
    if is_admin(message.from_user.id):
        await state.clear()
        await message.answer(
            "Admin paneliga xush kelibsiz!",
            reply_markup=main_menu_kb(message.from_user.id),
        )
        return

    # Test akkaunt — har safar /start bosganida hamma ma'lumot tozalanadi
    if is_tester(message.from_user.id):
        reset_user_data(message.from_user.id)
        # Yangi user sifatida davom etadi (pastda)

    user = get_user_by_telegram_id(message.from_user.id)
    if user:
        await state.clear()
        await send_bot_msg(message, "welcome_back",
                           reply_markup=main_menu_kb(message.from_user.id),
                           full_name=user['full_name'])
        return

    # Yangi user — start xabarlar + followup + "Konsultatsiya olish" tugma
    await state.clear()
    schedule_start_followup(message.from_user.id)
    await message.answer(
        "Assalomu alaykum! Botga xush kelibsiz!\n\n"
        "Konsultatsiya olish uchun quyidagi tugmani bosing:",
        reply_markup=main_menu_kb(message.from_user.id),
    )
    # Admin paneldan qo'shilgan start xabarlarini yuborish
    for msg in get_start_messages():
        try:
            await send_bot_msg(message, msg["key"])
        except Exception:
            pass


# --- 1. Ism ---

@router.message(Registration.waiting_for_name)
async def process_name(message: Message, state: FSMContext):
    if not message.text:
        await send_bot_msg(message, "reg_name_error")
        return

    full_name = message.text.strip()
    if len(full_name) < 3:
        await send_bot_msg(message, "reg_name_error")
        return

    await state.update_data(full_name=full_name)
    await state.set_state(Registration.waiting_for_phone)
    await send_bot_msg(message, "reg_phone_prompt",
                       reply_markup=ForceReply(input_field_placeholder="+998"),
                       send_companion=True)


# --- 2. Telefon ---

@router.message(Registration.waiting_for_phone)
async def process_phone(message: Message, state: FSMContext):
    if message.contact:
        phone = message.contact.phone_number
    elif message.text:
        phone = message.text.strip()
        cleaned = phone.replace("+", "").replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
        if not cleaned.isdigit() or len(cleaned) < 9:
            await message.answer(
                "Iltimos, to'g'ri telefon raqam kiriting:",
                reply_markup=ForceReply(input_field_placeholder="+998"),
            )
            return
    else:
        await message.answer(
            "Iltimos, to'g'ri telefon raqam kiriting:",
            reply_markup=ForceReply(input_field_placeholder="+998"),
        )
        return

    # Dublikat tekshiruv
    existing = check_phone_exists(phone, message.from_user.id)
    if existing:
        await message.answer(
            f"Bu telefon raqam allaqachon ro'yxatdan o'tgan: <b>{existing['full_name']}</b>\n\n"
            "Agar bu siz bo'lsangiz, /start buyrug'ini bosing.\n"
            "Boshqa raqam kiriting:",
            reply_markup=ForceReply(input_field_placeholder="+998"),
        )
        return

    await state.update_data(phone=phone)
    await state.set_state(Registration.waiting_for_goal)
    await send_bot_msg(message, "reg_goal_prompt",
                       reply_markup=ReplyKeyboardRemove(),
                       send_companion=True)


# --- 3. Maqsad ---

@router.message(Registration.waiting_for_goal)
async def process_goal(message: Message, state: FSMContext):
    if not message.text:
        await send_bot_msg(message, "reg_goal_prompt")
        return

    await state.update_data(goal=message.text.strip())
    await state.set_state(Registration.waiting_for_video)

    video_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Ha", callback_data="video:yes")],
        [InlineKeyboardButton(text="Yo'q", callback_data="video:no")],
    ])
    await send_bot_msg(message, "reg_video_prompt",
                       reply_markup=video_kb,
                       send_companion=True)


# --- 4. Video ko'rganmi (inline) ---

@router.callback_query(Registration.waiting_for_video, F.data == "video:yes")
async def process_video_yes(callback: CallbackQuery, state: FSMContext):
    await state.update_data(video_watched="Ha")
    await callback.message.edit_text(
        f"Javobingiz: <b>Ha</b>"
    )
    await state.set_state(Registration.waiting_for_time)
    await send_bot_msg(callback.message, "reg_time_prompt",
                       reply_markup=ReplyKeyboardRemove(),
                       send_companion=True)
    await callback.answer()


@router.callback_query(Registration.waiting_for_video, F.data == "video:no")
async def process_video_no(callback: CallbackQuery, state: FSMContext):
    await state.update_data(video_watched="Yo'q")
    await callback.message.edit_text(
        f"Javobingiz: <b>Yo'q</b>"
    )
    consult_kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Konsultatsiya")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await send_bot_msg(callback.message, "reg_video_no",
                       reply_markup=consult_kb,
                       send_companion=True)
    await callback.answer()


@router.message(Registration.waiting_for_video, F.text == "Konsultatsiya")
async def process_video_consultation(message: Message, state: FSMContext):
    await state.set_state(Registration.waiting_for_time)
    await send_bot_msg(message, "reg_time_prompt",
                       reply_markup=ReplyKeyboardRemove(),
                       send_companion=True)


# --- 5. Qulay vaqt ---

@router.message(Registration.waiting_for_time)
async def process_time(message: Message, state: FSMContext):
    if not message.text:
        await send_bot_msg(message, "reg_time_prompt")
        return

    await state.update_data(preferred_time=message.text.strip())
    await finish_registration(message, state, message.from_user.id)


# --- So'rovnomani to'ldirish (inline button dari broadcast) ---

@router.callback_query(F.data == "survey_fill")
async def on_survey_fill(callback: CallbackQuery, state: FSMContext):
    survey = get_survey_answers(callback.from_user.id)
    if survey:
        await callback.answer("Siz allaqachon so'rovnomani to'ldirgansiz!", show_alert=True)
        return
    await state.set_state(Registration.waiting_for_name)
    await send_bot_msg(callback.message, "reg_welcome", reply_markup=ReplyKeyboardRemove(),
                       send_companion=True)
    await callback.answer()


# --- Foydalanuvchi javob berish (admin xabariga) ---

class UserReply(StatesGroup):
    waiting_for_reply = State()


@router.callback_query(F.data == "user_reply")
async def on_user_reply(callback: CallbackQuery, state: FSMContext):
    await state.set_state(UserReply.waiting_for_reply)
    await callback.message.edit_reply_markup(reply_markup=None)
    await callback.message.answer("Javobingizni yozing:")
    await callback.answer()


@router.message(UserReply.waiting_for_reply)
async def process_user_reply(message: Message, state: FSMContext):
    await state.clear()

    user = get_user_by_telegram_id(message.from_user.id)
    user_name = user["full_name"] if user else message.from_user.full_name
    user_phone = user.get("phone", "") if user else ""

    admin_header = (
        f"<b>Javob keldi!</b>\n\n"
        f"Ism: {user_name}\n"
        f"Tel: {user_phone}\n"
        f"ID: <code>{message.from_user.id}</code>"
    )

    for admin_id in get_admin_ids():
        try:
            await message.bot.send_message(admin_id, admin_header)
            await message.bot.copy_message(
                chat_id=admin_id,
                from_chat_id=message.chat.id,
                message_id=message.message_id,
            )
        except Exception:
            pass

    await message.answer(
        "Javobingiz qabul qilindi! Tez orada siz bilan bog'lanamiz.",
        reply_markup=main_menu_kb(message.from_user.id),
    )


# --- Konsultatsiya olish (button) ---

@router.message(Command("consultation"))
@router.message(F.text == "Konsultatsiya olish")
async def cmd_consultation(message: Message, state: FSMContext):
    if is_admin(message.from_user.id):
        return

    user = get_user_by_telegram_id(message.from_user.id)

    # So'rovnoma to'ldirilmagan — welcome matn + ism so'rash
    survey = get_survey_answers(message.from_user.id) if user else None
    if not survey:
        await state.set_state(Registration.waiting_for_name)
        await send_bot_msg(message, "reg_welcome", reply_markup=ReplyKeyboardRemove(),
                           send_companion=True)
        return

    # So'rovnoma to'ldirilgan — aktiv booking bormi?
    booking = get_user_active_booking(message.from_user.id)
    if booking:
        from handlers.consultation import format_date_uz
        day_label = format_date_uz(booking["date"])
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Vaqtini o'zgartirish", callback_data=f"booking:change:{booking['id']}")],
            [InlineKeyboardButton(text="Bekor qilish", callback_data=f"booking:cancel:{booking['id']}")],
        ])
        await message.answer(
            f"<b>Sizning konsultatsiyangiz:</b>\n\n"
            f"Kun: {day_label}\n"
            f"Vaqt: {booking['time_slot']}\n\n"
            "O'zgartirish yoki bekor qilishingiz mumkin:",
            reply_markup=kb,
        )
        return

    # Aktiv booking yo'q — muvaffaqiyatli ro'yxatdan o'tganlik xabari
    await send_bot_msg(message, "reg_complete",
                       reply_markup=main_menu_kb(message.from_user.id))


# --- Admin panel ---

@router.message(F.text == "Admin panel")
async def cmd_admin_panel(message: Message):
    if not is_admin(message.from_user.id):
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Konsultatsiyalar", callback_data="admin:consultations")],
    ])
    await message.answer(
        "<b>Admin panel</b>\n\n"
        "Mavjud buyruqlar:\n"
        "/admins -- Adminlar ro'yxati\n"
        "/addadmin <code>ID</code> -- Yangi admin qo'shish\n"
        "/removeadmin <code>ID</code> -- Adminni o'chirish\n"
        "/stats -- Statistika\n"
        "/users -- Foydalanuvchilar ro'yxati\n"
        "/search <code>ism</code> -- Qidirish\n"
        "/broadcast -- Xabar yuborish\n"
        "/messages -- Bot xabarlarini boshqarish\n"
        "/consultations -- Konsultatsiyalar jadvali\n"
        "/survey_remind -- So'rovnoma eslatma yuborish\n"
        "/testers -- Testerlar ro'yxati\n"
        "/addtester <code>ID</code> -- Tester qo'shish\n"
        "/removetester <code>ID</code> -- Tester o'chirish\n\n"
        "Telegram ID ni @userinfobot dan olish mumkin.",
        reply_markup=kb,
    )


# --- Default handler ---

@router.message()
async def default_handler(message: Message, state: FSMContext):
    """State yo'qolganda (restart) yoki noma'lum xabarlarda — foydalanuvchini yo'naltirish."""
    current_state = await state.get_state()
    if current_state is not None:
        return

    user = get_user_by_telegram_id(message.from_user.id)
    if user:
        await send_bot_msg(message, "default_message",
                           reply_markup=main_menu_kb(message.from_user.id))
    else:
        await message.answer(
            "Quyidagi tugmalardan foydalaning:",
            reply_markup=main_menu_kb(message.from_user.id),
        )
