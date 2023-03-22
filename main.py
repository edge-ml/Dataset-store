import uvicorn
import argparse
from app.internal.config import loadConfig
parser = argparse.ArgumentParser(description="Run the database-store")
parser.add_argument('--env', default="dev", choices=["dev", "docker"])
args = parser.parse_args()
env = args.env
if env == "dev":
    loadConfig("development")
    print("load dev")
if env == "docker":
    loadConfig("docker")
    print("load docker")
from app.internal.config import PROJECT_DBNAME
print("project: ", PROJECT_DBNAME)

from fastapi import FastAPI, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from app.MessageQueue import main
import asyncio
import argparse
from app.routers import router
from hypercorn.asyncio import serve
from hypercorn.config import Config
from fastapi.middleware.gzip import GZipMiddleware


class DatasetStore(FastAPI):
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # TODO: adapt to specific origins
        self.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"]
        )

        self.add_middleware(GZipMiddleware, minimum_size=1000)

        self.include_router(
            router,
            prefix="/ds"
        )


app = DatasetStore()

rabbitMQTask = None

@app.exception_handler(ValueError)
async def value_error_exception_handler(request: Request, exc: ValueError):
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"message": str(exc)},
    )
@app.exception_handler(TypeError)
async def type_error_exception_handler(request: Request, exc: TypeError):
    print(exc)
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST, content={"message": "Invalid input"}
    )

@app.on_event('startup')
async def startup():
    global rabbitMQTask
    loop = asyncio.get_event_loop()
    rabbitMQTask = asyncio.create_task(main(loop))

@app.on_event('shutdown')
async def shutdown():
    rabbitMQTask.cancel()


if __name__ == "__main__":
    if env == "dev":
        uvicorn.run("main:app", host="0.0.0.0", port=3004, reload=True, log_level="trace", ws="wsproto", ws_ping_interval=10)
    if env == "docker":
        uvicorn.run("main:app", host="0.0.0.0", port=3004, workers=20)

