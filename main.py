import uvicorn

from app.internal.config import ENV
from fastapi import FastAPI, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from app.MessageQueue import main
import asyncio

from app.routers import router


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

        self.include_router(
            router
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

    reload = True
    if ENV == "production":
        reload = False
    print("Reload: ", reload)
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=reload)

