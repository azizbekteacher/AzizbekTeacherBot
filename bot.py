import asyncio
import logging

from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import BotCommand, BotCommandScopeChat

from config import BOT_TOKEN, ADMIN_IDS
from db import init_db, seed_admins, seed_bot_messages, get_admin_ids, migrate_scheduled_users
from google_sheets import migrate_existing_to_sheets
from scheduler import run_scheduler
from handlers import router

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

USER_COMMANDS = [
    BotCommand(command="start", description="Botni ishga tushirish"),
]

ADMIN_COMMANDS = USER_COMMANDS + [
    BotCommand(command="admins", description="Adminlar ro'yxati"),
    BotCommand(command="addadmin", description="Admin qo'shish"),
    BotCommand(command="removeadmin", description="Admin o'chirish"),
    BotCommand(command="stats", description="Statistika"),
    BotCommand(command="users", description="Foydalanuvchilar"),
    BotCommand(command="search", description="Qidirish"),
    BotCommand(command="broadcast", description="Xabar yuborish"),
    BotCommand(command="messages", description="Bot xabarlari"),
    BotCommand(command="consultations", description="Konsultatsiyalar"),
    BotCommand(command="survey_remind", description="So'rovnoma eslatma yuborish"),
    BotCommand(command="testers", description="Testerlar ro'yxati"),
    BotCommand(command="addtester", description="Tester qo'shish"),
    BotCommand(command="removetester", description="Tester o'chirish"),
]


async def set_bot_commands(bot: Bot):
    await bot.set_my_commands(USER_COMMANDS)
    for admin_id in get_admin_ids():
        try:
            await bot.set_my_commands(ADMIN_COMMANDS, scope=BotCommandScopeChat(chat_id=admin_id))
        except Exception:
            pass


async def main():
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    init_db()
    seed_bot_messages()
    migrate_scheduled_users()
    seed_admins(ADMIN_IDS)
    migrate_existing_to_sheets()
    await set_bot_commands(bot)

    # Scheduler — delayed xabarlarni DB dan o'qib yuboradi
    asyncio.create_task(run_scheduler(bot))

    logger.info("AzizbekTeacher bot ishga tushdi!")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
