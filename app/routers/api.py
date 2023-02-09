from fastapi import APIRouter, Request, Header, Response
from fastapi.param_functions import Depends

from app.controller.dataset_controller import DatasetController
from app.utils.json_encoder import JSONEncoder
from app.routers.dependencies import validateApiKey

import json

router = APIRouter()

ctrl = DatasetController()

@router.post("/{api_key}")
async def externalUpload(api_key, body: Request, user_id=Depends(validateApiKey)):
    body = await body.json()
    print("body")
    print(body)
    res = ctrl.externalUpload(api_key, user_id, body)
    return Response(json.dumps(res, cls=JSONEncoder), media_type="application/json")