import uvicorn
import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers import router

app = FastAPI()

# TODO: adapt to specific origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

app.include_router(
    router
)

print("started dataset store...")
@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.on_event("shutdown")
async def shutdown():
    print('goodbye...')


if __name__ == "__main__":
    reload = True
    if os.getenv("ENV") == "production":
        reload = False
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=reload)