import uvicorn

from app.internal.config import ENV
from fastapi import FastAPI, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

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


@app.exception_handler(ValueError)
async def value_error_exception_handler(request: Request, exc: ValueError):
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"message": str(exc)},
    )
@app.exception_handler(TypeError)
async def type_error_exception_handler(request: Request, exc: TypeError):
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST, content={"message": "Invalid input"}
    )

if __name__ == "__main__":
    reload = True
    if ENV == "production":
        reload = False
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=reload)