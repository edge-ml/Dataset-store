"""Audio and video recordings of a dataset (see controller/media_controller)."""
import json
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Header, HTTPException, Query, Request, Response, status
from fastapi.concurrency import run_in_threadpool
from fastapi.param_functions import Depends
from pydantic import BaseModel
from starlette.datastructures import UploadFile

from controller.media_controller import MediaController, too_large
from internal.config import MEDIA_MAX_SIZE_BYTES
from routers.dependencies import validate_user
from utils.json_encoder import JSONEncoder

# Mounted under /ds/datasets: needs the login and the project header
router = APIRouter()
# Mounted under /ds/media: playback through a link from `/link`
file_router = APIRouter()

ctrl = MediaController()


def _json(data, status_code=200):
    return Response(json.dumps(data, cls=JSONEncoder), status_code=status_code, media_type="application/json")


async def receive_media(request: Request, dataset_id, project_id, background_tasks: BackgroundTasks):
    """Read a multipart upload (file, and optionally kind, name, start) and
    store it. The body is only read here, after the caller is authenticated."""
    length = request.headers.get("content-length")
    # the form fields and boundaries add a little on top of the file
    if length and length.isdigit() and int(length) > MEDIA_MAX_SIZE_BYTES + 1024 * 1024:
        raise too_large()
    if "multipart/form-data" not in request.headers.get("content-type", ""):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Send the recording as multipart form data in the field 'file'")
    form = await request.form()
    try:
        file = form.get("file")
        if not isinstance(file, UploadFile):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Send the recording in the form field 'file'")
        start = form.get("start")
        if start not in (None, ""):
            try:
                start = int(float(start))
            except ValueError:
                raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="start must be a unix timestamp in milliseconds")
        else:
            start = None
        media = await run_in_threadpool(
            ctrl.upload, dataset_id, project_id, file.file, file.filename, background_tasks,
            kind=form.get("kind") or None, name=form.get("name") or None, start=start)
    finally:
        await form.close()
    return _json(media, status.HTTP_201_CREATED)


@router.post("/{id}/media")
async def upload_media(id, request: Request, background_tasks: BackgroundTasks, project: str = Header(...), user_data=Depends(validate_user)):
    return await receive_media(request, id, project, background_tasks)


class MediaUpdate(BaseModel):
    name: Optional[str] = None
    # unix ms; moves the recording against the sensor data
    start: Optional[int] = None


@router.put("/{id}/media/{media_id}")
def update_media(id, media_id, body: MediaUpdate, project: str = Header(...), user_data=Depends(validate_user)):
    return _json(ctrl.update(id, project, media_id, name=body.name, start=body.start))


@router.delete("/{id}/media/{media_id}")
def delete_media(id, media_id, project: str = Header(...), user_data=Depends(validate_user)):
    ctrl.delete(id, project, media_id)
    return {"message": "success"}


@router.get("/{id}/media/{media_id}/link")
def get_media_link(id, media_id, project: str = Header(...), user_data=Depends(validate_user)):
    return ctrl.link(id, project, media_id)


@router.get("/{id}/media/{media_id}/peaks")
def get_media_peaks(id, media_id, project: str = Header(...), user_data=Depends(validate_user)):
    return Response(ctrl.peaks(id, project, media_id), media_type="application/json")


@file_router.get("/{media_id}")
def play_media(media_id, request: Request, token: str = Query(...)):
    media = ctrl.from_token(media_id, token)
    return ctrl.stream(media, request.headers.get("range"))
