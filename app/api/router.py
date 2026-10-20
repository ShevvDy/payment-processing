from fastapi import APIRouter

from app.api.payments import router as payments_router


api_router = APIRouter(prefix="/api/v1")
api_router.include_router(payments_router)
