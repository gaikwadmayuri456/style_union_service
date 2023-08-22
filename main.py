from datetime import datetime
from fastapi import FastAPI
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from uvicorn import run
from loguru import logger
import influxdb_client
from starlette.middleware.cors import CORSMiddleware
from src.routes.all_routes import router

# from src.routes.all_routes import router
app = FastAPI(
    title='SU Report Service'
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]
)
app.include_router(router)

if __name__ == "__main__":
    run("main:app", host="localhost", port=5066, reload=True)
