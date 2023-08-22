from fastapi import APIRouter
from src.routes.python_script import router as python_script

router = APIRouter()

router.include_router(python_script)
