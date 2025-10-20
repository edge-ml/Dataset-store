from fastapi import APIRouter, FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from app.api import v1
from app.db import init_db
from app.routers import dataset


@asynccontextmanager
async def lifespan(app: FastAPI):
    db = await init_db()
    yield


app = FastAPI(lifespan=lifespan, title="edge-ml Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

app.include_router(v1.router, prefix="/api/v1")

# app.include_router(
#     dataset.router,
#     prefix='/ds/datasets',
#     tags=["Datasets"]
# )