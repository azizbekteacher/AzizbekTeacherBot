import asyncio
import logging
from pathlib import Path

from aiogram import Bot
from aiogram.types import FSInputFile, ReplyKeyboardMarkup, KeyboardButton

from db import get_pending_messages, mark_message_sent, get_msg

logger = logging.getLogger(__name__)
MEDIA_DIR = Path(__file__).parent / "media"

# Fallback: DB da media_file_id yo'q bo'lganda lokal fayldan yuborish
FALLBACK_MEDIA = {}


async def send_scheduled_message(bot: Bot, scheduled: dict):
    chat_id = scheduled["telegram_id"]
    msg_type = scheduled["message_type"]

    msg = get_msg(msg_type)
    if not msg:
        logger.warning(f"bot_messages da '{msg_type}' topilmadi, o'tkazib yuborildi")
        return

    content_type = msg.get("content_type", "text")
    text = msg.get("text") or ""
    file_id = msg.get("media_file_id")

    if content_type == "voice":
        if file_id:
            await bot.send_voice(chat_id, file_id)
        elif msg_type in FALLBACK_MEDIA:
            path = MEDIA_DIR / FALLBACK_MEDIA[msg_type][1]
            if path.exists():
                await bot.send_voice(chat_id, FSInputFile(path))
            else:
                logger.warning(f"{path.name} topilmadi, o'tkazib yuborildi")
        elif text:
            await bot.send_message(chat_id, text, parse_mode="HTML")

    elif content_type == "photo":
        if file_id:
            await bot.send_photo(chat_id, file_id, caption=text or None, parse_mode="HTML")
        elif msg_type in FALLBACK_MEDIA:
            path = MEDIA_DIR / FALLBACK_MEDIA[msg_type][1]
            if path.exists():
                await bot.send_photo(chat_id, FSInputFile(path), caption=text or None, parse_mode="HTML")
            elif text:
                await bot.send_message(chat_id, text, parse_mode="HTML")
        elif text:
            await bot.send_message(chat_id, text, parse_mode="HTML")

    elif content_type == "video":
        if file_id:
            await bot.send_video(chat_id, file_id, caption=text or None, parse_mode="HTML")
        elif text:
            await bot.send_message(chat_id, text, parse_mode="HTML")

    elif content_type == "document":
        if file_id:
            await bot.send_document(chat_id, file_id, caption=text or None, parse_mode="HTML")
        elif text:
            await bot.send_message(chat_id, text, parse_mode="HTML")

    else:
        if text:
            # followup_consult yuborilganda "Konsultatsiya olish" tugmasini ko'rsatish
            reply_markup = None
            if msg_type == "followup_consult":
                reply_markup = ReplyKeyboardMarkup(
                    keyboard=[[KeyboardButton(text="Konsultatsiya olish")]],
                    resize_keyboard=True,
                )
            await bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=reply_markup)


async def run_scheduler(bot: Bot):
    """Har 30 sekundda DB dan pending xabarlarni tekshiradi va yuboradi."""
    while True:
        try:
            pending = get_pending_messages()
            for msg in pending:
                try:
                    await send_scheduled_message(bot, msg)
                except Exception as e:
                    logger.error(f"Scheduled message [{msg['id']}] xatolik: {e}")
                finally:
                    mark_message_sent(msg["id"])
        except Exception as e:
            logger.error(f"Scheduler xatolik: {e}")
        await asyncio.sleep(30)
