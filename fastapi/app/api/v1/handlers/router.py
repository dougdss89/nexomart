from app.api.v1.handlers.user import user_router
from fastapi import APIRouter

router = APIRouter()

# include_router is a method imported from APIRouter.
router.include_router(user_router, prefix="/users", tags=["users"])