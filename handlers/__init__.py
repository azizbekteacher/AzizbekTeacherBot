from aiogram import Router

from .start import router as start_router
from .consultation import router as consultation_router
from .admin import router as admin_router, cmd_router as admin_cmd_router

router = Router()
router.include_router(admin_cmd_router)
router.include_router(admin_router)
router.include_router(start_router)
router.include_router(consultation_router)
