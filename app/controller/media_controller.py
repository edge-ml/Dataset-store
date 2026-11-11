"""Audio and video recordings in datasets.

A recording is stored as uploaded and listed in the dataset's `mediaSeries`.
It sits on the dataset timeline through its `start` (unix ms), so the labels,
which are plain time ranges, apply to it like they do to the sensor data.
"""
import json
import math
import os
import subprocess
import tempfile
import time
import logging

import jwt
import numpy as np
from bson.objectid import ObjectId
from bson.errors import InvalidId
from fastapi import HTTPException, status
from fastapi.responses import StreamingResponse

from db.dataset import DatasetDBManager
from controller.media_store import mediaStore, CHUNK_SIZE
from internal.config import SECRET_KEY, MEDIA_MAX_SIZE_BYTES, MEDIA_URL_TTL_SECONDS

logger = logging.getLogger(__name__)

KINDS = ("audio", "video")

# Only formats a browser can play are accepted; nothing is converted.
# container -> (codecs allowed for the picture, codecs allowed for the sound)
VIDEO_FORMATS = {
    "mp4": ({"h264", "av1", "vp9"}, {"aac", "mp3", "opus"}),
    "webm": ({"vp8", "vp9", "av1"}, {"opus", "vorbis"}),
}
AUDIO_FORMATS = {
    "mp3": {"mp3"},
    "wav": None,  # any PCM
    "ogg": {"vorbis", "opus", "flac"},
    "flac": {"flac"},
    "mp4": {"aac", "mp3", "opus"},
    "webm": {"opus", "vorbis"},
}
MIME_TYPES = {
    ("video", "mp4"): "video/mp4",
    ("video", "webm"): "video/webm",
    ("audio", "mp3"): "audio/mpeg",
    ("audio", "wav"): "audio/wav",
    ("audio", "ogg"): "audio/ogg",
    ("audio", "flac"): "audio/flac",
    ("audio", "mp4"): "audio/mp4",
    ("audio", "webm"): "audio/webm",
}
ACCEPTED_TEXT = "Accepted are MP4 (H.264 with AAC) and WebM video, and MP3, WAV, OGG, FLAC and M4A audio."

# Waveform: lowest and highest sample per bucket, taken from a mono 8 kHz copy
PEAKS_SAMPLE_RATE = 8000
PEAKS_PER_SECOND = 50
PEAKS_MAX_BUCKETS = 500_000

TOKEN_TYPE = "media"


def _bad_request(detail):
    return HTTPException(status.HTTP_400_BAD_REQUEST, detail=detail)


def _container(format_name):
    names = set((format_name or "").split(","))
    if "mp4" in names or "mov" in names:
        return "mp4"
    if "webm" in names or "matroska" in names:
        return "webm"
    for name in ("mp3", "wav", "ogg", "flac"):
        if name in names:
            return name
    return None


def _rate(value):
    """'30000/1001' -> 29.97"""
    try:
        num, _, den = str(value).partition("/")
        den = float(den) if den else 1.0
        return round(float(num) / den, 3) if den else None
    except ValueError:
        return None


def _run(cmd, timeout):
    try:
        return subprocess.run(cmd, capture_output=True, timeout=timeout)
    except FileNotFoundError:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="ffmpeg is not installed on the server")
    except subprocess.TimeoutExpired:
        raise _bad_request("The file took too long to read")


def _duration_from_packets(path):
    """Length of files that do not state it (e.g. WebM recorded in a browser)."""
    res = _run(["ffprobe", "-v", "error", "-show_entries", "packet=pts_time,duration_time",
                "-of", "csv=p=0", path], timeout=600)
    last = 0.0
    for line in res.stdout.decode(errors="ignore").splitlines():
        try:
            parts = [float(x) for x in line.split(",") if x and x != "N/A"]
        except ValueError:
            continue
        if parts:
            last = max(last, sum(parts[:2]))
    return last


def probe(path, kind=None):
    """Read what a file contains and check that a browser can play it.

    Returns {kind, durationMs, mimeType, hasAudio, meta}; raises a 400 with a
    readable reason otherwise.
    """
    res = _run(["ffprobe", "-v", "error", "-print_format", "json", "-show_format", "-show_streams", path], timeout=120)
    try:
        info = json.loads(res.stdout or b"{}")
    except ValueError:
        info = {}
    streams = info.get("streams") or []
    fmt = info.get("format") or {}
    # cover art in an audio file is reported as a video stream
    video = next((s for s in streams if s.get("codec_type") == "video"
                  and not (s.get("disposition") or {}).get("attached_pic")), None)
    audio = next((s for s in streams if s.get("codec_type") == "audio"), None)
    if res.returncode != 0 or (video is None and audio is None):
        raise _bad_request(f"This is not an audio or video file. {ACCEPTED_TEXT}")

    actual = "video" if video is not None else "audio"
    if kind is not None and kind != actual:
        raise _bad_request(f"The file is {actual}, not {kind}")
    container = _container(fmt.get("format_name"))
    video_codec = video.get("codec_name") if video else None
    audio_codec = audio.get("codec_name") if audio else None

    if actual == "video":
        allowed = VIDEO_FORMATS.get(container)
        ok = allowed is not None and video_codec in allowed[0] and (audio_codec is None or audio_codec in allowed[1])
    else:
        ok = container in AUDIO_FORMATS and (
            (audio_codec or "").startswith("pcm_") if AUDIO_FORMATS[container] is None
            else audio_codec in AUDIO_FORMATS[container])
    if not ok:
        found = "/".join(x for x in (fmt.get("format_name", "unknown").split(",")[0], video_codec, audio_codec) if x)
        raise _bad_request(f"This {actual} format ({found}) cannot be played in the browser. {ACCEPTED_TEXT}")

    durations = [fmt.get("duration")] + [s.get("duration") for s in streams]
    seconds = 0.0
    for d in durations:
        try:
            seconds = max(seconds, float(d))
        except (TypeError, ValueError):
            pass
    if seconds <= 0:
        seconds = _duration_from_packets(path)
    if seconds <= 0:
        raise _bad_request("The length of the recording could not be read")

    meta = {"container": container, "videoCodec": video_codec, "audioCodec": audio_codec}
    if video:
        meta.update({"width": video.get("width"), "height": video.get("height"),
                     "fps": _rate(video.get("avg_frame_rate"))})
    if audio:
        meta.update({"sampleRate": int(audio["sample_rate"]) if audio.get("sample_rate") else None,
                     "channels": audio.get("channels")})
    return {
        "kind": actual,
        "durationMs": int(round(seconds * 1000)),
        "mimeType": MIME_TYPES[(actual, container)],
        "hasAudio": audio is not None,
        "meta": meta,
    }


def compute_peaks(path, duration_ms):
    """Waveform of the sound track as JSON bytes: {rate, min[], max[]}.

    `rate` is buckets per second; min/max hold the lowest and highest sample of
    each bucket, scaled to -128..127.
    """
    buckets_per_second = min(PEAKS_PER_SECOND, PEAKS_MAX_BUCKETS / max(duration_ms / 1000, 1))
    samples = math.ceil(PEAKS_SAMPLE_RATE / buckets_per_second)
    proc = subprocess.Popen(
        ["ffmpeg", "-v", "error", "-i", path, "-vn", "-map", "0:a:0", "-ac", "1",
         "-ar", str(PEAKS_SAMPLE_RATE), "-f", "s16le", "-"],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    mins, maxs = [], []
    rest = b""
    block = samples * 2 * 512
    try:
        while True:
            data = proc.stdout.read(block)
            if not data:
                break
            data = rest + data
            usable = len(data) - len(data) % (samples * 2)
            rest = data[usable:]
            if usable:
                arr = np.frombuffer(data[:usable], dtype="<i2").reshape(-1, samples)
                mins.append(arr.min(axis=1))
                maxs.append(arr.max(axis=1))
        if len(rest) >= 2:
            arr = np.frombuffer(rest[:len(rest) - len(rest) % 2], dtype="<i2")
            mins.append(np.array([arr.min()]))
            maxs.append(np.array([arr.max()]))
    finally:
        proc.stdout.close()
        if proc.wait() != 0:
            raise RuntimeError("ffmpeg could not decode the sound track")
    lo = (np.concatenate(mins) // 256).tolist() if mins else []
    hi = (np.concatenate(maxs) // 256).tolist() if maxs else []
    return json.dumps({"rate": PEAKS_SAMPLE_RATE / samples, "min": lo, "max": hi},
                      separators=(",", ":")).encode()


def _parse_range(header, size):
    """Byte range of a Range header as (start, end) inclusive; None when absent
    or not understood (the whole file is sent then)."""
    if not header or not header.startswith("bytes=") or "," in header:
        return None
    first, sep, last = header[len("bytes="):].strip().partition("-")
    if not sep:
        return None
    try:
        if first == "":
            length = int(last)
            if length <= 0:
                raise HTTPException(status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE, headers={"Content-Range": f"bytes */{size}"})
            return max(size - length, 0), size - 1
        start = int(first)
        end = int(last) if last else size - 1
    except ValueError:
        return None
    if start >= size or end < start:
        raise HTTPException(status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE, headers={"Content-Range": f"bytes */{size}"})
    return start, min(end, size - 1)


class MediaController:

    def __init__(self):
        self.dbm = DatasetDBManager()

    # -- lookups ---------------------------------------------------------------
    def _dataset(self, dataset_id, project_id):
        try:
            dataset = self.dbm.getDatasetById(dataset_id, project_id)
        except InvalidId:
            dataset = None
        if dataset is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Dataset not found")
        return dataset

    def get(self, dataset_id, project_id, media_id):
        dataset = self._dataset(dataset_id, project_id)
        media = next((m for m in dataset.get("mediaSeries", []) if str(m["_id"]) == str(media_id)), None)
        if media is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Recording not found")
        return media

    # -- upload ----------------------------------------------------------------
    def upload(self, dataset_id, project_id, fileobj, filename, background_tasks, kind=None, name=None, start=None):
        """Store an uploaded file as a recording of the dataset.

        `fileobj` is the open upload. The checks happen before anything is
        stored, so a file that is listed can always be played.
        """
        dataset = self._dataset(dataset_id, project_id)
        if kind is not None and kind not in KINDS:
            raise _bad_request("kind must be 'audio' or 'video'")
        if start is None:
            start = self._default_start(dataset)

        fd, path = tempfile.mkstemp(prefix="media_")
        keep = False
        try:
            size = 0
            with os.fdopen(fd, "wb") as out:
                while True:
                    chunk = fileobj.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > MEDIA_MAX_SIZE_BYTES:
                        raise too_large()
                    out.write(chunk)
            if size == 0:
                raise _bad_request("The file is empty")
            info = probe(path, kind)
            media_id = ObjectId()
            mediaStore.save_file(str(media_id), path)
            media = self.dbm.addMedia(dataset_id, project_id, {
                "_id": media_id,
                "kind": info["kind"],
                "name": (name or "").strip() or os.path.splitext(os.path.basename(filename or ""))[0] or info["kind"],
                "start": int(start),
                "end": int(start) + info["durationMs"],
                "durationMs": info["durationMs"],
                "mimeType": info["mimeType"],
                "size": size,
                "status": "processing" if info["hasAudio"] else "ready",
                "meta": info["meta"],
            })
            if media is None:  # the dataset was deleted in the meantime
                mediaStore.delete(str(media_id))
                raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Dataset not found")
            if info["hasAudio"]:
                keep = True
                background_tasks.add_task(self.process, dataset_id, project_id, media_id, path, info["durationMs"])
            return media
        finally:
            if not keep:
                os.remove(path)

    def _default_start(self, dataset):
        """Where a recording goes when the upload does not say: the beginning
        of the dataset, or now when the dataset is still empty."""
        starts = [t["start"] for t in dataset.get("timeSeries", [])
                  if t.get("start") is not None and t.get("end") is not None and t["start"] <= t["end"]]
        starts += [m["start"] for m in dataset.get("mediaSeries", [])]
        return min(starts) if starts else int(time.time() * 1000)

    def process(self, dataset_id, project_id, media_id, path, duration_ms):
        """Compute the waveform after the upload has been answered."""
        has_peaks = False
        try:
            mediaStore.save_bytes(f"{media_id}.peaks", compute_peaks(path, duration_ms))
            has_peaks = True
        except Exception as e:
            logger.error(f"Waveform of recording {media_id} failed: {e}")
        finally:
            try:
                os.remove(path)
            except OSError:
                pass
        if not self.dbm.updateMedia(dataset_id, project_id, media_id, {"status": "ready", "hasPeaks": has_peaks}):
            self.deleteFiles(media_id)  # deleted while it was processed

    # -- changes ---------------------------------------------------------------
    def update(self, dataset_id, project_id, media_id, name=None, start=None):
        media = self.get(dataset_id, project_id, media_id)
        updates = {}
        if name is not None:
            if not name.strip():
                raise _bad_request("The name must not be empty")
            updates["name"] = name.strip()
        if start is not None:
            updates["start"] = int(start)
            updates["end"] = int(start) + media["durationMs"]
        if updates:
            self.dbm.updateMedia(dataset_id, project_id, media_id, updates)
        return {**media, **updates}

    def delete(self, dataset_id, project_id, media_id):
        self.get(dataset_id, project_id, media_id)
        self.dbm.deleteMedia(dataset_id, project_id, media_id)
        self.deleteFiles(media_id)

    def deleteFiles(self, media_id):
        for key in (str(media_id), f"{media_id}.peaks"):
            try:
                mediaStore.delete(key)
            except Exception as e:
                logger.error(f"Could not delete media file {key}: {e}")

    # -- reading ---------------------------------------------------------------
    def peaks(self, dataset_id, project_id, media_id):
        media = self.get(dataset_id, project_id, media_id)
        data = mediaStore.load_bytes(f"{media_id}.peaks") if media.get("hasPeaks") else None
        if data is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail="No waveform for this recording")
        return data

    def link(self, dataset_id, project_id, media_id):
        """A link that plays the recording without further credentials.

        A player element cannot send the login or the project header, so the
        link carries a token that is only good for this one file.
        """
        self.get(dataset_id, project_id, media_id)
        expires = int(time.time()) + MEDIA_URL_TTL_SECONDS
        token = jwt.encode({"typ": TOKEN_TYPE, "mid": str(media_id), "did": str(dataset_id),
                            "pid": str(project_id), "exp": expires}, SECRET_KEY, algorithm="HS256")
        return {"path": f"/ds/media/{media_id}?token={token}", "expires": expires * 1000}

    def from_token(self, media_id, token):
        try:
            data = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        except jwt.InvalidTokenError:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="The link is invalid or has expired")
        if data.get("typ") != TOKEN_TYPE or data.get("mid") != str(media_id):
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="The link is invalid or has expired")
        return self.get(data["did"], data["pid"], media_id)

    def stream(self, media, range_header=None, download=False):
        """The file itself, or the byte range a player asked for."""
        size = media["size"]
        headers = {
            "Accept-Ranges": "bytes",
            # keeps the gzip middleware away: a compressed stream cannot be seeked
            "Content-Encoding": "identity",
            "Cache-Control": "private, max-age=3600",
        }
        if download:
            headers["Content-Disposition"] = "attachment"
        byte_range = _parse_range(range_header, size)
        if byte_range is None:
            start, end, code = 0, size - 1, status.HTTP_200_OK
        else:
            start, end = byte_range
            code = status.HTTP_206_PARTIAL_CONTENT
            headers["Content-Range"] = f"bytes {start}-{end}/{size}"
        headers["Content-Length"] = str(end - start + 1)
        return StreamingResponse(mediaStore.read(str(media["_id"]), start, end),
                                 status_code=code, media_type=media["mimeType"], headers=headers)


def too_large():
    return HTTPException(status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                         detail=f"The file is larger than the allowed {MEDIA_MAX_SIZE_BYTES // (1024 * 1024)} MB")
