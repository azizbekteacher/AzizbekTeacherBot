from aiogram import Router, F
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove,
    ForceReply,
)

from config import VIDEO_LINK
from db import (
    save_user, get_user_by_telegram_id, is_admin,
    schedule_followup_messages, save_survey_answers, get_survey_answers,
    get_msg, get_msg_text, get_user_active_booking, cancel_booking,
    check_phone_exists, check_username_exists,
    save_user_extra_phone, get_admin_ids, get_start_messages,
)

router = Router()


async def send_bot_msg(target: Message, key: str, reply_markup=None, edit: bool = False, **fmt):
    """bot_messages jadvalidan xabarni content_type ga qarab yuborish (photo, video, voice, text)."""
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
    waiting_for_name = State()          # 1
    waiting_for_phone = State()         # 2
    extra_phone_choice = State()        # 2.5
    waiting_for_extra_phone = State()   # 2.6
    waiting_for_username = State()      # 3
    waiting_for_age = State()           # 4
    waiting_for_workplace = State()     # 5
    waiting_for_methods = State()       # 6
    waiting_for_courses = State()       # 7
    waiting_for_exam = State()          # 8
    waiting_for_exam_result = State()   # 9
    waiting_for_importance = State()    # 10 (inline)
    waiting_for_result_meaning = State()  # 11
    waiting_for_budget = State()        # 12 (inline)
    waiting_for_video = State()         # 13 (inline)


# Registration oqimi — skip bo'lishi mumkin bo'lgan qadamlar
# (name va phone MAJBURIY, REG_FLOW da emas)
REG_FLOW = [
    ("reg_username_prompt", Registration.waiting_for_username, None),
    ("reg_age_prompt", Registration.waiting_for_age, None),
    ("reg_workplace_prompt", Registration.waiting_for_workplace, None),
    ("reg_methods_prompt", Registration.waiting_for_methods, None),
    ("reg_courses_prompt", Registration.waiting_for_courses, None),
    ("reg_exam_prompt", Registration.waiting_for_exam, None),
    ("reg_exam_result_prompt", Registration.waiting_for_exam_result, None),
    ("reg_importance_prompt", Registration.waiting_for_importance, "importance"),
    ("reg_result_meaning_prompt", Registration.waiting_for_result_meaning, None),
    ("reg_budget_prompt", Registration.waiting_for_budget, "budget"),
    ("reg_video_prompt", Registration.waiting_for_video, "video"),
]


def _build_step_kb(kb_type: str):
    """Inline keyboard yaratish — faqat inline savollar uchun."""
    if kb_type == "importance":
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="1-4 juda muhim emas", callback_data="importance:1-4")],
            [InlineKeyboardButton(text="5-7 o'rgansam yaxshi", callback_data="importance:5-7")],
            [InlineKeyboardButton(text="8-10 o'rganishim shart", callback_data="importance:8-10")],
        ])
    elif kb_type == "budget":
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Oyiga 500,000 so'mgacha", callback_data="budget:500k")],
            [InlineKeyboardButton(text="Oyiga 600,000 - 800,000 so'mgacha", callback_data="budget:600-800k")],
            [InlineKeyboardButton(text="Yaxshi ustoz bilan oyiga 1,000,000 so'mgacha", callback_data="budget:1mln")],
        ])
    elif kb_type == "video":
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Ha, oxirigacha ko'rdim", callback_data="video:yes")],
            [InlineKeyboardButton(text="Yarimigacha ko'rdim", callback_data="video:half")],
            [InlineKeyboardButton(text="Yo'q, hali ko'rmadim", callback_data="video:no")],
        ])
    return None


async def advance_reg(target, state: FSMContext, from_key: str):
    """from_key dan boshlab birinchi faol registration qadamga o'tadi.
    O'chirilgan qadamlarni skip qiladi.
    True qaytaradi agar barcha qolgan qadamlar o'chirilgan (reg tugadi)."""
    found = False
    for msg_key, next_state, kb_type in REG_FLOW:
        if not found:
            if msg_key == from_key:
                found = True
            continue
        msg = get_msg(msg_key)
        if msg and not msg.get("is_active", 1):
            continue
        await state.set_state(next_state)
        kb = _build_step_kb(kb_type) if kb_type else ReplyKeyboardRemove()
        await send_bot_msg(target, msg_key, reply_markup=kb)
        return False
    return True


async def finish_registration(target, state: FSMContext, user_telegram_id: int):
    """Registration tugashi — ma'lumotlarni saqlash va yakuniy xabar."""
    data = await state.get_data()
    await state.clear()

    user_id = save_user(user_telegram_id, data.get("full_name", "Noma'lum"), data.get("phone", ""))
    if data.get("extra_phone"):
        save_user_extra_phone(user_telegram_id, data["extra_phone"])
    save_survey_answers(user_id, {
        "username": data.get("username"),
        "age": data.get("age"),
        "workplace": data.get("workplace"),
        "methods_tried": data.get("methods_tried"),
        "previous_courses": data.get("previous_courses"),
        "exam_plan": data.get("exam_plan"),
        "exam_goal": data.get("exam_goal"),
        "importance": data.get("importance"),
        "result_meaning": data.get("result_meaning"),
        "budget": data.get("budget"),
        "video_watched": data.get("video_watched"),
    })
    schedule_followup_messages(user_telegram_id)

    video_watched = data.get("video_watched", "")
    if video_watched and "oxirigacha" in video_watched:
        await send_bot_msg(target, "reg_complete_yes",
                           reply_markup=main_menu_kb(user_telegram_id))
    else:
        inline_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Videoni ko'rish", url=VIDEO_LINK)]
        ])
        await send_bot_msg(target, "start_welcome",
                           reply_markup=inline_kb, video_link=VIDEO_LINK)
        await target.answer(
            "Videoni ko'rib bo'lganingizdan so'ng konsultatsiya olishingiz mumkin:",
            reply_markup=main_menu_kb(user_telegram_id),
        )


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

    user = get_user_by_telegram_id(message.from_user.id)
    if user:
        await state.clear()
        await send_bot_msg(message, "welcome_back",
                           reply_markup=main_menu_kb(message.from_user.id),
                           full_name=user['full_name'])
        return

    # Yangi user — saqlash + start xabarlarini yuborish
    await state.clear()
    tg_name = message.from_user.full_name or "Noma'lum"
    save_user(message.from_user.id, tg_name, "")
    schedule_followup_messages(message.from_user.id)

    # Admin paneldan qo'shilgan start xabarlarini yuborish
    start_msgs = get_start_messages()
    for i, msg in enumerate(start_msgs):
        try:
            is_last = i == len(start_msgs) - 1
            rm = main_menu_kb(message.from_user.id) if is_last else None
            await send_bot_msg(message, msg["key"], reply_markup=rm, video_link=VIDEO_LINK)
        except Exception:
            pass

    if not start_msgs:
        await message.answer(
            "Quyidagi tugmadan foydalaning:",
            reply_markup=main_menu_kb(message.from_user.id),
        )


# --- 1. Ism ---

@router.message(Registration.waiting_for_name)
async def process_name(message: Message, state: FSMContext):
    full_name = message.text.strip()

    if len(full_name) < 3:
        await send_bot_msg(message, "reg_name_error")
        return

    await state.update_data(full_name=full_name)
    await state.set_state(Registration.waiting_for_phone)

    await message.answer(
        "Siz bilan bog'lanishimiz mumkin bo'lgan telefon raqamini kiriting:",
        reply_markup=ForceReply(input_field_placeholder="+998"),
    )


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
    await state.set_state(Registration.extra_phone_choice)

    await message.answer(
        f"Telefon raqamingiz qabul qilindi: <b>{phone}</b>\n\n"
        "Agar sizda yana boshqa telefon raqam bo'lsa, uni ham qo'shishingiz mumkin.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Boshqa raqam qo'shish", callback_data="reg:extra_phone")],
            [InlineKeyboardButton(text="Keyingi", callback_data="reg:skip_extra")],
        ]),
    )


# --- 2.5 Qo'shimcha telefon ---

@router.callback_query(Registration.extra_phone_choice, F.data == "reg:extra_phone")
async def on_extra_phone(callback: CallbackQuery, state: FSMContext):
    await state.set_state(Registration.waiting_for_extra_phone)
    await callback.message.edit_reply_markup(reply_markup=None)
    await callback.message.answer(
        "Qo'shimcha telefon raqamini kiriting:",
        reply_markup=ForceReply(input_field_placeholder="+998"),
    )
    await callback.answer()


@router.callback_query(Registration.extra_phone_choice, F.data == "reg:skip_extra")
async def on_skip_extra_phone(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_reply_markup(reply_markup=None)
    if await advance_reg(callback.message, state, "reg_username_prompt"):
        await finish_registration(callback.message, state, callback.from_user.id)
    await callback.answer()


@router.message(Registration.waiting_for_extra_phone)
async def process_extra_phone(message: Message, state: FSMContext):
    phone = message.text.strip() if message.text else ""
    cleaned = phone.replace("+", "").replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    if not cleaned.isdigit() or len(cleaned) < 9:
        await message.answer(
            "Iltimos, to'g'ri telefon raqam kiriting:",
            reply_markup=ForceReply(input_field_placeholder="+998"),
        )
        return

    await state.update_data(extra_phone=phone)
    await message.answer(f"Qo'shimcha raqam qabul qilindi: <b>{phone}</b>")
    if await advance_reg(message, state, "reg_username_prompt"):
        await finish_registration(message, state, message.from_user.id)


# --- 3. Username ---

@router.message(Registration.waiting_for_username)
async def process_username(message: Message, state: FSMContext):
    username = message.text.strip()
    if not username.startswith("@"):
        await send_bot_msg(message, "reg_username_error")
        return

    # Dublikat tekshiruv
    existing = check_username_exists(username, message.from_user.id)
    if existing:
        await message.answer(
            f"Bu username allaqachon ro'yxatdan o'tgan: <b>{existing['full_name']}</b>\n\n"
            "Agar bu siz bo'lsangiz, /start buyrug'ini bosing.\n"
            "Boshqa username kiriting:"
        )
        return

    await state.update_data(username=username)
    if await advance_reg(message, state, "reg_age_prompt"):
        await finish_registration(message, state, message.from_user.id)


# --- 4. Yosh ---

@router.message(Registration.waiting_for_age)
async def process_age(message: Message, state: FSMContext):
    text = message.text.strip()
    if not text.isdigit():
        await send_bot_msg(message, "reg_age_error")
        return

    await state.update_data(age=int(text))
    if await advance_reg(message, state, "reg_workplace_prompt"):
        await finish_registration(message, state, message.from_user.id)


# --- 5. O'qish/Ish joyi ---

@router.message(Registration.waiting_for_workplace)
async def process_workplace(message: Message, state: FSMContext):
    await state.update_data(workplace=message.text.strip())
    if await advance_reg(message, state, "reg_methods_prompt"):
        await finish_registration(message, state, message.from_user.id)


# --- 6. Sinab ko'rgan usullar ---

@router.message(Registration.waiting_for_methods)
async def process_methods(message: Message, state: FSMContext):
    await state.update_data(methods_tried=message.text.strip())
    if await advance_reg(message, state, "reg_courses_prompt"):
        await finish_registration(message, state, message.from_user.id)


# --- 7. Kurslarda o'qiganmi ---

@router.message(Registration.waiting_for_courses)
async def process_courses(message: Message, state: FSMContext):
    await state.update_data(previous_courses=message.text.strip())
    if await advance_reg(message, state, "reg_exam_prompt"):
        await finish_registration(message, state, message.from_user.id)


# --- 8. Imtihon rejasi ---

@router.message(Registration.waiting_for_exam)
async def process_exam(message: Message, state: FSMContext):
    await state.update_data(exam_plan=message.text.strip())
    if await advance_reg(message, state, "reg_exam_result_prompt"):
        await finish_registration(message, state, message.from_user.id)


# --- 9. Imtihon natijasi ---

@router.message(Registration.waiting_for_exam_result)
async def process_exam_result(message: Message, state: FSMContext):
    await state.update_data(exam_goal=message.text.strip())
    if await advance_reg(message, state, "reg_importance_prompt"):
        await finish_registration(message, state, message.from_user.id)


# --- 10. Muhimlik (inline) ---

@router.callback_query(Registration.waiting_for_importance, F.data.startswith("importance:"))
async def process_importance(callback: CallbackQuery, state: FSMContext):
    importance = callback.data.split(":", 1)[1]
    await state.update_data(importance=importance)
    await callback.message.edit_text(
        f"Javobingiz: <b>{importance}</b>"
    )
    if await advance_reg(callback.message, state, "reg_result_meaning_prompt"):
        await finish_registration(callback.message, state, callback.from_user.id)
    await callback.answer()


# --- 11. Natija nimani anglatadi ---

@router.message(Registration.waiting_for_result_meaning)
async def process_result_meaning(message: Message, state: FSMContext):
    await state.update_data(result_meaning=message.text.strip())
    if await advance_reg(message, state, "reg_budget_prompt"):
        await finish_registration(message, state, message.from_user.id)


# --- 12. Byudjet (inline) ---

@router.callback_query(Registration.waiting_for_budget, F.data.startswith("budget:"))
async def process_budget(callback: CallbackQuery, state: FSMContext):
    budget = callback.data.split(":", 1)[1]
    await state.update_data(budget=budget)
    budget_labels = {
        "500k": "Oyiga 500,000 so'mgacha",
        "600-800k": "Oyiga 600,000 - 800,000 so'mgacha",
        "1mln": "Yaxshi ustoz bilan oyiga 1,000,000 so'mgacha",
    }
    await callback.message.edit_text(
        f"Javobingiz: <b>{budget_labels.get(budget, budget)}</b>"
    )
    if await advance_reg(callback.message, state, "reg_video_prompt"):
        await finish_registration(callback.message, state, callback.from_user.id)
    await callback.answer()


# --- 13. Video ko'rganmi (inline) ---

@router.callback_query(Registration.waiting_for_video, F.data.startswith("video:"))
async def process_video(callback: CallbackQuery, state: FSMContext):
    video_key = callback.data.split(":", 1)[1]
    video_labels = {
        "yes": "Ha, oxirigacha ko'rdim",
        "half": "Yarimigacha ko'rdim",
        "no": "Yo'q, hali ko'rmadim",
    }
    video_watched = video_labels.get(video_key, video_key)
    await state.update_data(video_watched=video_watched)
    await callback.message.edit_text(
        f"Javobingiz: <b>{video_watched}</b>"
    )
    await finish_registration(callback.message, state, callback.from_user.id)
    await callback.answer()


# --- So'rovnomani to'ldirish (inline button dari broadcast) ---

@router.callback_query(F.data == "survey_fill")
async def on_survey_fill(callback: CallbackQuery, state: FSMContext):
    survey = get_survey_answers(callback.from_user.id)
    if survey:
        await callback.answer("Siz allaqachon so'rovnomani to'ldirgansiz!", show_alert=True)
        return
    await state.set_state(Registration.waiting_for_name)
    await send_bot_msg(callback.message, "reg_name_prompt", reply_markup=ReplyKeyboardRemove())
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

    # So'rovnoma to'ldirilmagan — avval so'rovnomani boshlash
    survey = get_survey_answers(message.from_user.id) if user else None
    if not survey:
        await state.set_state(Registration.waiting_for_name)
        await send_bot_msg(message, "reg_name_prompt", reply_markup=ReplyKeyboardRemove())
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

    # Aktiv booking yo'q — kun tanlash
    from handlers.consultation import Consultation, build_days_keyboard
    await state.set_state(Consultation.select_day)
    kb = build_days_keyboard()
    if not kb.inline_keyboard:
        await send_bot_msg(message, "no_days_available")
        await state.clear()
        return
    await send_bot_msg(message, "consult_day_prompt", reply_markup=kb)


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
        "/survey_remind -- So'rovnoma eslatma yuborish\n\n"
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
