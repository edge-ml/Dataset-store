import pytest
from bson.objectid import ObjectId
from fastapi import HTTPException

from controller.label_controller import (
    _checkLabelOverlap, createLabel, deleteLabel, updateLabel)
from controller.labelingController import (
    createLabeling, deleteLabeling, deleteProjectLabeling, getProjectLabelings,
    onLabelingChanged, onLabelingDeleted, updateLabeling)


def make_dataset(seeder, labeling_id, labels):
    return seeder.dataset(labelings=[{"labelingId": labeling_id, "labels": labels}])


class TestCheckOverlap:
    def test_overlap_detected(self):
        dataset = {"labelings": [{"labelingId": ObjectId(),
                                  "labels": [{"start": 0, "end": 10, "_id": ObjectId()}]}]}
        label = {"start": 5, "end": 15, "_id": ObjectId()}
        assert _checkLabelOverlap(dataset, label, str(dataset["labelings"][0]["labelingId"]))

    def test_same_label_ignored(self):
        oid = ObjectId()
        dataset = {"labelings": [{"labelingId": ObjectId(),
                                  "labels": [{"start": 0, "end": 10, "_id": oid}]}]}
        label = {"start": 0, "end": 10, "_id": oid}
        assert not _checkLabelOverlap(dataset, label, str(dataset["labelings"][0]["labelingId"]))

    def test_no_overlap(self):
        dataset = {"labelings": [{"labelingId": ObjectId(),
                                  "labels": [{"start": 0, "end": 10, "_id": ObjectId()}]}]}
        label = {"start": 11, "end": 15, "_id": ObjectId()}
        assert not _checkLabelOverlap(dataset, label, str(dataset["labelings"][0]["labelingId"]))


class TestCreateLabel:
    def test_dataset_missing(self):
        with pytest.raises(HTTPException) as e:
            createLabel(str(ObjectId()), str(ObjectId()), str(ObjectId()),
                        {"start": 1, "end": 2})
        assert e.value.status_code == 400

    def test_create_in_existing_labeling(self, seeder):
        labeling_id = ObjectId()
        doc = make_dataset(seeder, labeling_id, [])
        label = createLabel(str(doc["_id"]), str(seeder.project_id),
                            str(labeling_id), {"start": 1, "end": 2,
                                               "type": ObjectId()})
        assert "_id" in label
        stored = seeder.datasets.find_one({})
        assert len(stored["labelings"][0]["labels"]) == 1

    def test_creates_labeling_container_if_missing(self, seeder):
        doc = make_dataset(seeder, ObjectId(), [])  # unrelated labeling
        new_labeling = ObjectId()
        createLabel(str(doc["_id"]), str(seeder.project_id), str(new_labeling),
                    {"start": 1, "end": 2, "type": ObjectId()})
        stored = seeder.datasets.find_one({})
        assert any(str(x["labelingId"]) == str(new_labeling) for x in stored["labelings"])

    def test_overlap_rejected(self, seeder):
        labeling_id = ObjectId()
        type_id = ObjectId()
        doc = make_dataset(seeder, labeling_id, [
            {"start": 0, "end": 10, "type": type_id, "_id": ObjectId()}])
        with pytest.raises(HTTPException) as e:
            createLabel(str(doc["_id"]), str(seeder.project_id), str(labeling_id),
                        {"start": 5, "end": 6, "type": type_id})
        assert e.value.status_code == 400


class TestUpdateDeleteLabel:
    def _setup(self, seeder):
        labeling_id = ObjectId()
        type_id = ObjectId()
        label_id = ObjectId()
        doc = make_dataset(seeder, labeling_id, [
            {"start": 0, "end": 10, "type": type_id, "_id": label_id},
            {"start": 20, "end": 30, "type": type_id, "_id": ObjectId()},
        ])
        return doc, labeling_id, label_id

    def test_update(self, seeder):
        doc, labeling_id, label_id = self._setup(seeder)
        updateLabel(str(seeder.project_id), str(doc["_id"]), str(labeling_id),
                    str(label_id), {"start": 1, "end": 3, "type": ObjectId(),
                                    "_id": label_id})
        stored = seeder.datasets.find_one({})["labelings"][0]["labels"]
        assert [l["start"] for l in sorted(stored, key=lambda x: x["start"])] == [1, 20]

    def test_update_overlap_rejected(self, seeder):
        doc, labeling_id, label_id = self._setup(seeder)
        with pytest.raises(HTTPException):
            updateLabel(str(seeder.project_id), str(doc["_id"]), str(labeling_id),
                        str(label_id), {"start": 25, "end": 35, "type": ObjectId(),
                                        "_id": label_id})

    def test_delete_keeps_other_labels(self, seeder):
        doc, labeling_id, label_id = self._setup(seeder)
        deleteLabel(str(seeder.project_id), str(doc["_id"]), str(labeling_id), str(label_id))
        stored = seeder.datasets.find_one({})
        assert len(stored["labelings"][0]["labels"]) == 1

    def test_delete_removes_empty_labeling(self, seeder):
        labeling_id = ObjectId()
        label_id = ObjectId()
        doc = make_dataset(seeder, labeling_id, [
            {"start": 0, "end": 10, "type": ObjectId(), "_id": label_id}])
        deleteLabel(str(seeder.project_id), str(doc["_id"]), str(labeling_id), str(label_id))
        assert seeder.datasets.find_one({})["labelings"] == []


class TestLabelingController:
    def test_create_and_get(self, seeder):
        pid = str(seeder.project())
        res = createLabeling(pid, {"name": "L", "labels": [{"name": "a", "color": "#000000"}]})
        assert len(getProjectLabelings(pid)) == 1

    def test_update_propagates_to_datasets(self, seeder):
        pid = str(seeder.project())
        created = createLabeling(pid, {"name": "L", "labels": [
            {"name": "a", "color": "#000000"}, {"name": "b", "color": "#111111"}]})
        keep_id = created["labels"][0]["_id"]
        drop_id = created["labels"][1]["_id"]

        doc = make_dataset(seeder, created["_id"], [
            {"start": 0, "end": 5, "type": keep_id},
            {"start": 6, "end": 9, "type": drop_id},
        ])
        # an unrelated empty-ish labeling that should be dropped once its labels vanish
        other_type = ObjectId()
        doc["labelings"].append({"labelingId": ObjectId(),
                                 "labels": [{"type": other_type, "start": 0, "end": 1}]})
        seeder.datasets.replace_one({"_id": doc["_id"]}, doc)

        updated = updateLabeling(pid, str(created["_id"]), {
            "_id": created["_id"], "projectId": seeder.project_id, "name": "L",
            "labels": [{"name": "a", "color": "#000000", "_id": str(keep_id)}]})

        stored = seeder.datasets.find_one({"_id": doc["_id"]})
        ds_labeling = next(l for l in stored["labelings"]
                           if str(l["labelingId"]) == str(created["_id"]))
        assert all(str(x["type"]) == str(keep_id) for x in ds_labeling["labels"])
        # labelings unrelated to the changed labeling are left untouched
        assert any(str(l["labelingId"]) == str(doc["labelings"][1]["labelingId"])
                   for l in stored["labelings"])

    def test_delete_labeling_removes_from_datasets(self, seeder):
        pid = str(seeder.project())
        created = createLabeling(pid, {"name": "L", "labels": []})
        doc = make_dataset(seeder, created["_id"], [{"start": 0, "end": 1, "type": ObjectId()}])
        deleteLabeling(pid, str(created["_id"]))
        stored = seeder.datasets.find_one({"_id": doc["_id"]})
        assert stored["labelings"] == []
        assert getProjectLabelings(pid) == []

    def test_delete_project_labelings(self, seeder):
        pid = str(seeder.project())
        createLabeling(pid, {"name": "L", "labels": []})
        deleteProjectLabeling(pid)
        assert getProjectLabelings(pid) == []

    def test_on_labeling_deleted_noop_without_datasets(self, seeder):
        pid = str(seeder.project())
        onLabelingDeleted(pid, ObjectId())  # must not raise
