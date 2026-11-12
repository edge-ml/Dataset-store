"""Audio and video recordings in datasets."""
import json
import os
import shutil
import subprocess

import pytest
from bson.objectid import ObjectId

from controller import media_controller
from controller.media_controller import _parse_range, compute_peaks, probe
from controller.media_store import FileSystemMediaStore, mediaStore
from fastapi import HTTPException

pytestmark = pytest.mark.skipif(shutil.which("ffmpeg") is None, reason="needs ffmpeg")


def _ffmpeg(path, *args):
    subprocess.run(["ffmpeg", "-v", "error", "-y", *args, str(path)], check=True)
    return path


@pytest.fixture(scope="session")
def files(tmp_path_factory):
    d = tmp_path_factory.mktemp("media")
    tone = ["-f", "lavfi", "-i", "sine=frequency=440:duration=2"]
    picture = ["-f", "lavfi", "-i", "testsrc=duration=2:size=64x48:rate=10"]
    out = {
        "wav": _ffmpeg(d / "tone.wav", *tone),
        "mp3": _ffmpeg(d / "tone.mp3", *tone),
        "mp4": _ffmpeg(d / "clip.mp4", *picture, *tone, "-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "aac", "-shortest"),
        "silent": _ffmpeg(d / "silent.webm", *picture, "-c:v", "libvpx"),
        "avi": _ffmpeg(d / "clip.avi", *picture, "-c:v", "mpeg4"),
    }
    text = d / "notes.txt"
    text.write_text("not a recording")
    out["txt"] = text
    return out


def _upload(client, seeder, dataset, path, **fields):
    with open(path, "rb") as f:
        return client.post(f"/ds/datasets/{dataset['_id']}/media", headers=seeder.headers(), cookies=seeder.cookies(),
                           files={"file": (os.path.basename(path), f)}, data={k: str(v) for k, v in fields.items()})


def _media(seeder, dataset):
    return seeder.datasets.find_one({"_id": dataset["_id"]}).get("mediaSeries", [])


class TestProbe:
    def test_audio(self, files):
        info = probe(str(files["wav"]))
        assert info["kind"] == "audio" and info["mimeType"] == "audio/wav"
        assert abs(info["durationMs"] - 2000) < 50
        assert info["meta"]["sampleRate"] == 44100 and info["hasAudio"]

    def test_video(self, files):
        info = probe(str(files["mp4"]))
        assert info["kind"] == "video" and info["mimeType"] == "video/mp4"
        assert info["meta"]["width"] == 64 and info["meta"]["height"] == 48 and info["meta"]["fps"] == 10
        assert info["meta"]["videoCodec"] == "h264" and info["meta"]["audioCodec"] == "aac"

    def test_video_without_sound(self, files):
        info = probe(str(files["silent"]))
        assert info["mimeType"] == "video/webm" and not info["hasAudio"]

    def test_rejects_other_files(self, files):
        with pytest.raises(HTTPException) as e:
            probe(str(files["txt"]))
        assert e.value.status_code == 400

    def test_rejects_formats_browsers_cannot_play(self, files):
        with pytest.raises(HTTPException) as e:
            probe(str(files["avi"]))
        assert e.value.status_code == 400 and "cannot be played" in e.value.detail

    def test_rejects_wrong_kind(self, files):
        with pytest.raises(HTTPException) as e:
            probe(str(files["wav"]), "video")
        assert "audio, not video" in e.value.detail


class TestPeaks:
    def test_waveform(self, files):
        peaks = json.loads(compute_peaks(str(files["wav"]), 2000))
        assert peaks["rate"] == 50
        assert len(peaks["min"]) == len(peaks["max"]) == 100
        assert min(peaks["min"]) < -5 and max(peaks["max"]) > 5
        assert all(-128 <= v <= 127 for v in peaks["min"] + peaks["max"])

    def test_long_recordings_get_wider_buckets(self, files, monkeypatch):
        monkeypatch.setattr(media_controller, "PEAKS_MAX_BUCKETS", 20)
        peaks = json.loads(compute_peaks(str(files["wav"]), 2000))
        assert peaks["rate"] == 10 and len(peaks["min"]) == 20


class TestRange:
    def test_forms(self):
        assert _parse_range(None, 100) is None
        assert _parse_range("bytes=0-9", 100) == (0, 9)
        assert _parse_range("bytes=90-", 100) == (90, 99)
        assert _parse_range("bytes=-10", 100) == (90, 99)
        assert _parse_range("bytes=50-500", 100) == (50, 99)
        assert _parse_range("items=0-9", 100) is None

    def test_unsatisfiable(self):
        with pytest.raises(HTTPException) as e:
            _parse_range("bytes=100-", 100)
        assert e.value.status_code == 416


class TestStore:
    def test_file_system(self, tmp_path):
        store = FileSystemMediaStore(str(tmp_path))
        src = tmp_path / "src"
        src.write_bytes(bytes(range(200)))
        store.save_file("a", str(src))
        assert store.size("a") == 200
        assert b"".join(store.read("a", 10, 19)) == bytes(range(10, 20))
        store.save_bytes("a.peaks", b"{}")
        assert store.load_bytes("a.peaks") == b"{}"
        store.delete("a")
        store.delete("a")  # deleting twice is fine
        assert store.size("a") is None and store.load_bytes("missing") is None


class TestRoutes:
    def test_upload_audio(self, client, seeder, files):
        seeder.project()
        ds = seeder.dataset()
        r = _upload(client, seeder, ds, files["wav"], start=5000)
        assert r.status_code == 201, r.text
        body = r.json()
        assert body["kind"] == "audio" and body["name"] == "tone"
        assert body["start"] == 5000 and body["end"] == 5000 + body["durationMs"]
        assert body["size"] == os.path.getsize(files["wav"])
        # the waveform is computed right after the response
        stored = _media(seeder, ds)[0]
        assert stored["status"] == "ready" and stored["hasPeaks"]
        r = client.get(f"/ds/datasets/{ds['_id']}/media/{body['_id']}/peaks", headers=seeder.headers(), cookies=seeder.cookies())
        assert r.status_code == 200 and len(r.json()["min"]) == 100
        # and the dataset lists the recording
        r = client.get(f"/ds/datasets/{ds['_id']}", headers=seeder.headers(), cookies=seeder.cookies())
        assert [m["_id"] for m in r.json()["mediaSeries"]] == [body["_id"]]

    def test_upload_video_defaults(self, client, seeder, files):
        seeder.project()
        ds = seeder.dataset()
        r = _upload(client, seeder, ds, files["mp4"], name="front camera")
        assert r.status_code == 201, r.text
        body = r.json()
        assert body["kind"] == "video" and body["name"] == "front camera"
        assert body["start"] == 1000  # the start of the sensor data
        assert body["meta"]["width"] == 64

    def test_video_without_sound_has_no_waveform(self, client, seeder, files):
        seeder.project()
        ds = seeder.dataset()
        body = _upload(client, seeder, ds, files["silent"]).json()
        assert body["status"] == "ready" and not body["hasPeaks"]
        r = client.get(f"/ds/datasets/{ds['_id']}/media/{body['_id']}/peaks", headers=seeder.headers(), cookies=seeder.cookies())
        assert r.status_code == 404

    def test_rejected_uploads_store_nothing(self, client, seeder, files):
        seeder.project()
        ds = seeder.dataset()
        assert _upload(client, seeder, ds, files["txt"]).status_code == 400
        assert _upload(client, seeder, ds, files["avi"]).status_code == 400
        assert _upload(client, seeder, ds, files["wav"], kind="video").status_code == 400
        assert _upload(client, seeder, ds, files["wav"], kind="photo").status_code == 400
        assert _upload(client, seeder, ds, files["wav"], start="soon").status_code == 400
        assert _media(seeder, ds) == []
        assert not os.path.isdir(mediaStore.root) or os.listdir(mediaStore.root) == []

    def test_too_large(self, client, seeder, files, monkeypatch):
        seeder.project()
        ds = seeder.dataset()
        monkeypatch.setattr(media_controller, "MEDIA_MAX_SIZE_BYTES", 1000)
        assert _upload(client, seeder, ds, files["wav"]).status_code == 413
        assert _media(seeder, ds) == []

    def test_needs_login_and_dataset(self, client, seeder, files):
        seeder.project()
        ds = seeder.dataset()
        with open(files["wav"], "rb") as f:
            r = client.post(f"/ds/datasets/{ds['_id']}/media", headers=seeder.headers(), cookies={"jwt": "x"}, files={"file": f})
        assert r.status_code == 401
        missing = {"_id": ObjectId()}
        assert _upload(client, seeder, missing, files["wav"]).status_code == 404
        r = client.post(f"/ds/datasets/{ds['_id']}/media", headers=seeder.headers(), cookies=seeder.cookies(), json={})
        assert r.status_code == 400

    def test_playback_link_with_ranges(self, client, seeder, files):
        seeder.project()
        ds = seeder.dataset()
        body = _upload(client, seeder, ds, files["mp4"]).json()
        raw = open(files["mp4"], "rb").read()
        r = client.get(f"/ds/datasets/{ds['_id']}/media/{body['_id']}/link", headers=seeder.headers(), cookies=seeder.cookies())
        assert r.status_code == 200
        path = r.json()["path"]
        # the link needs neither cookie nor project header
        client.cookies.clear()
        r = client.get(path)
        assert r.status_code == 200 and r.content == raw
        assert r.headers["content-type"] == "video/mp4" and r.headers["accept-ranges"] == "bytes"
        r = client.get(path, headers={"Range": "bytes=10-19", "Accept-Encoding": "gzip"})
        assert r.status_code == 206 and r.content == raw[10:20]
        assert r.headers["content-range"] == f"bytes 10-19/{len(raw)}" and r.headers["content-length"] == "10"
        r = client.get(path, headers={"Range": "bytes=-5"})
        assert r.content == raw[-5:]
        assert client.get(path, headers={"Range": f"bytes={len(raw)}-"}).status_code == 416

    def test_link_is_bound_to_one_recording(self, client, seeder, files):
        seeder.project()
        ds = seeder.dataset()
        a = _upload(client, seeder, ds, files["wav"]).json()
        b = _upload(client, seeder, ds, files["mp3"]).json()
        path = client.get(f"/ds/datasets/{ds['_id']}/media/{a['_id']}/link", headers=seeder.headers(), cookies=seeder.cookies()).json()["path"]
        token = path.split("token=")[1]
        assert client.get(f"/ds/media/{b['_id']}?token={token}").status_code == 401
        assert client.get(f"/ds/media/{a['_id']}?token=nonsense").status_code == 401
        # a login token is not a playback link, and a link is not a login
        assert client.get(f"/ds/media/{a['_id']}?token={seeder.token()}").status_code == 401
        r = client.get("/ds/datasets/", headers=seeder.headers(), cookies={"jwt": token})
        assert r.status_code != 200

    def test_rename_and_move(self, client, seeder, files):
        seeder.project()
        ds = seeder.dataset()
        body = _upload(client, seeder, ds, files["wav"], start=1000).json()
        url = f"/ds/datasets/{ds['_id']}/media/{body['_id']}"
        r = client.put(url, headers=seeder.headers(), cookies=seeder.cookies(), json={"name": " mic ", "start": 4000})
        assert r.status_code == 200 and r.json()["name"] == "mic"
        stored = _media(seeder, ds)[0]
        assert stored["name"] == "mic" and stored["start"] == 4000 and stored["end"] == 4000 + stored["durationMs"]
        assert client.put(url, headers=seeder.headers(), cookies=seeder.cookies(), json={"name": " "}).status_code == 400
        other = f"/ds/datasets/{ds['_id']}/media/{ObjectId()}"
        assert client.put(other, headers=seeder.headers(), cookies=seeder.cookies(), json={"name": "x"}).status_code == 404

    def test_delete(self, client, seeder, files):
        seeder.project()
        ds = seeder.dataset()
        body = _upload(client, seeder, ds, files["wav"]).json()
        assert len(os.listdir(mediaStore.root)) == 2  # file and waveform
        url = f"/ds/datasets/{ds['_id']}/media/{body['_id']}"
        assert client.delete(url, headers=seeder.headers(), cookies=seeder.cookies()).status_code == 200
        assert _media(seeder, ds) == [] and os.listdir(mediaStore.root) == []
        assert client.delete(url, headers=seeder.headers(), cookies=seeder.cookies()).status_code == 404

    def test_deleting_the_dataset_deletes_the_files(self, client, seeder, files):
        from controller.binary_store import BinaryStore
        seeder.project()
        ds = seeder.dataset()
        BinaryStore(ds["timeSeries"][0]["_id"]).saveSeries()
        _upload(client, seeder, ds, files["wav"])
        r = client.delete(f"/ds/datasets/{ds['_id']}", headers=seeder.headers(), cookies=seeder.cookies())
        assert r.status_code == 200
        assert os.listdir(mediaStore.root) == []

    def test_deleting_the_project_deletes_the_files(self, client, seeder, files):
        from controller.binary_store import BinaryStore
        from controller.dataset_controller import DatasetController
        seeder.project()
        ds = seeder.dataset()
        BinaryStore(ds["timeSeries"][0]["_id"]).saveSeries()
        _upload(client, seeder, ds, files["wav"])
        DatasetController().deleteProjectDatasets(str(seeder.project_id))
        assert os.listdir(mediaStore.root) == []

    def test_other_dataset_updates_keep_recordings(self, client, seeder, files):
        """Clients send whole datasets back; ones that do not know about
        recordings must not wipe them."""
        seeder.project()
        ds = seeder.dataset()
        body = _upload(client, seeder, ds, files["wav"]).json()
        stale = {"name": "renamed", "timeSeries": [{**t, "_id": str(t["_id"])} for t in ds["timeSeries"]],
                 "projectId": str(ds["projectId"]), "userId": str(ds["userId"]), "labelings": []}
        r = client.put(f"/ds/datasets/{ds['_id']}", headers=seeder.headers(), cookies=seeder.cookies(), json=stale)
        assert r.status_code == 200
        doc = seeder.datasets.find_one({"_id": ds["_id"]})
        assert doc["name"] == "renamed" and [str(m["_id"]) for m in doc["mediaSeries"]] == [body["_id"]]


class TestDeviceApi:
    def test_upload_list_download(self, client, seeder, files):
        seeder.project()
        key = seeder.api_key()
        r = client.post(f"/ds/api/dataset/init/{key}", json={"name": "media only", "timeSeries": []})
        assert r.status_code == 200
        dataset_id = r.json()["id"]
        with open(files["mp3"], "rb") as f:
            r = client.post(f"/ds/api/dataset/media/{key}/{dataset_id}", files={"file": ("voice.mp3", f)},
                            data={"start": "1700000000000", "kind": "audio"})
        assert r.status_code == 201, r.text
        media = r.json()
        assert media["name"] == "voice" and media["start"] == 1700000000000 and media["mimeType"] == "audio/mpeg"

        listed = client.get(f"/ds/api/project/{key}").json()["datasets"]
        assert [m["_id"] for m in listed[0]["mediaSeries"]] == [media["_id"]]

        r = client.get(f"/ds/api/dataset/media/{key}/{dataset_id}/{media['_id']}")
        assert r.status_code == 200 and r.content == open(files["mp3"], "rb").read()
        r = client.get(f"/ds/api/dataset/media/{key}/{dataset_id}/{media['_id']}", headers={"Range": "bytes=0-3"})
        assert r.status_code == 206 and len(r.content) == 4

    def test_empty_dataset_places_recording_at_now(self, client, seeder, files):
        import time
        seeder.project()
        key = seeder.api_key()
        dataset_id = client.post(f"/ds/api/dataset/init/{key}", json={"name": "d", "timeSeries": ["x"]}).json()["id"]
        with open(files["wav"], "rb") as f:
            media = client.post(f"/ds/api/dataset/media/{key}/{dataset_id}", files={"file": f}).json()
        assert abs(media["start"] - time.time() * 1000) < 60_000

    def test_keys(self, client, seeder, files):
        seeder.project()
        read = seeder.api_key("readkey", access="read")
        ds = seeder.dataset()
        with open(files["wav"], "rb") as f:
            assert client.post(f"/ds/api/dataset/media/{read}/{ds['_id']}", files={"file": f}).status_code == 401
        with open(files["wav"], "rb") as f:
            assert client.post(f"/ds/api/dataset/media/nokey/{ds['_id']}", files={"file": f}).status_code == 401


class TestCsvExport:
    def test_dataset_with_only_recordings(self, client, seeder, files):
        """The CSV download has no sensor columns then, but must not fail."""
        from controller.dataset_controller import DatasetController
        seeder.project()
        ds = seeder.dataset(time_series=[])
        _upload(client, seeder, ds, files["wav"])
        stream, name = DatasetController().getCSV(str(seeder.project_id), str(ds["_id"]))
        assert name == "ds" and stream.getvalue().strip() == "time"


class TestDownload:
    """A dataset with recordings downloads as an archive of all it holds."""

    def _labelled(self, client, seeder, files, time_series=None):
        seeder.project()
        labeling_id, walking = ObjectId(), ObjectId()
        seeder.labelings.insert_one({"_id": labeling_id, "projectId": seeder.project_id, "name": "activity",
                                     "labels": [{"_id": walking, "name": "Walking", "color": "#f00"}]})
        ds = seeder.dataset(time_series=time_series, labelings=[{
            "labelingId": labeling_id,
            "labels": [{"_id": ObjectId(), "type": walking, "start": 1100, "end": 1900}],
        }])
        _upload(client, seeder, ds, files["silent"], name="cam", start=1000)
        return ds

    def _download(self, seeder, ds):
        from controller.downloadController import downloadDataset
        from db.csv import csvDB, DBEntryDataset
        csvDB().add(DBEntryDataset(downloadId="dl", projectId=seeder.project_id, userId=ObjectId(),
                                   projectName="p", datasetName="ds"))
        downloadDataset("dl", str(seeder.project_id), ds["_id"])
        return seeder.downloads.find_one({"downloadId": "dl"})

    def test_recordings_only(self, client, seeder, files):
        import zipfile
        ds = self._labelled(client, seeder, files, time_series=[])
        entry = self._download(seeder, ds)
        assert entry["status"] == 100 and entry["fileType"] == "zip" and not entry["error"]
        with zipfile.ZipFile(entry["filePath"]) as z:
            # no data CSV: there are no samples to put in it
            assert sorted(z.namelist()) == ["cam.webm", "ds_labels.csv"]
            assert z.read("ds_labels.csv").decode().splitlines() == [
                "labeling,label,start,end", "activity,Walking,1100,1900"]
            with open(files["silent"], "rb") as f:
                assert z.read("cam.webm") == f.read()
        os.remove(entry["filePath"])

    def test_with_sensor_data_served_as_zip(self, client, seeder, files):
        import asyncio
        import zipfile
        from controller.binary_store import BinaryStore
        from controller.downloadController import get_download_data
        series = [{"name": "a", "start": 1000, "end": 2000}]
        ds = self._labelled(client, seeder, files, time_series=series)
        BinaryStore(str(ds["timeSeries"][0]["_id"]))._appendValues([1000, 1500, 2000], [1.0, 2.0, 3.0])
        entry = self._download(seeder, ds)
        with zipfile.ZipFile(entry["filePath"]) as z:
            assert sorted(z.namelist()) == ["cam.webm", "ds.csv", "ds_labels.csv"]
            assert b"sensor_a" in z.read("ds.csv")
        response = asyncio.run(get_download_data("dl"))
        assert response.media_type == "application/zip"
        assert response.headers["Content-Disposition"].endswith("ds.zip")
        os.remove(entry["filePath"])

    def test_failure_is_reported(self, client, seeder, files):
        ds = self._labelled(client, seeder, files, time_series=[])
        mediaStore.delete(str(_media(seeder, ds)[0]["_id"]))
        entry = self._download(seeder, ds)
        assert entry["status"] < 100 and "missing" in entry["error"]
        assert not entry["filePath"]

    def test_progress_is_reported(self, client, seeder, files, monkeypatch):
        from controller import downloadController
        ds = self._labelled(client, seeder, files, time_series=[])
        seen = []
        update = downloadController.db.update
        monkeypatch.setattr(downloadController.db, "update",
                            lambda *a, **k: (seen.append(k.get("status")), update(*a, **k))[1])
        # the recording counts for nearly all of the work
        monkeypatch.setattr(downloadController, "CSV_WEIGHT", 1)
        entry = self._download(seeder, ds)
        steps = [s for s in seen if s is not None]
        assert steps == sorted(steps) and steps[-1] == 100 and any(0 < s < 100 for s in steps)
        os.remove(entry["filePath"])

    def test_project_download_holds_recordings(self, client, seeder, files):
        import asyncio
        import zipfile
        from controller.downloadController import downloadProject, get_download_data
        from db.csv import csvDB, DBEntryProject
        ds = self._labelled(client, seeder, files, time_series=[])
        seeder.dataset(name="plain")
        from controller.binary_store import BinaryStore
        plain = seeder.datasets.find_one({"name": "plain"})
        BinaryStore(str(plain["timeSeries"][0]["_id"]))._appendValues([1000, 2000], [1.0, 2.0])
        csvDB().add(DBEntryProject(downloadId="pr", projectId=seeder.project_id, userId=ObjectId(), projectName="p"))
        downloadProject("pr", str(seeder.project_id))
        entry = seeder.downloads.find_one({"downloadId": "pr"})
        assert entry["status"] == 100 and not entry["error"]
        with zipfile.ZipFile(entry["filePath"]) as z:
            # no CSV for the dataset that only holds recordings
            assert sorted(z.namelist()) == ["ds/cam.webm", "ds_labels.csv", "plain.csv"]
            assert b"Walking" in z.read("ds_labels.csv")
        response = asyncio.run(get_download_data("pr"))
        assert response.headers["Content-Length"] == str(os.path.getsize(entry["filePath"]))
        os.remove(entry["filePath"])
