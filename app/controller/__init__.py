from .binary_store import BinaryStore
from app.db.timeseries import TimeseriesDBManager


class Controller():
    
    def __init__(self) -> None:
        self.dbManager = TimeseriesDBManager()

    def _splitMeta_Data(self, timeSeries):
        tsValues = timeSeries["data"]
        metaData = timeSeries
        del metaData["data"]
        return metaData, tsValues

    def addTimeSeriesBatch(self, timeSeries):
        for ts in timeSeries:
            metaData = {"_id": ts.id, "name": ts.name, "start": ts.start, "end": ts.end, "unit": ts.unit}
            values = ts.data
            doc = self.dbManager.addTimeSeries(metaData)
            binStore = BinaryStore(str(doc.inserted_id))
            start, end = binStore.append(values)
            self.dbManager.updateStartEnd(metaData["_id"], start, end)


    def addTimeSeries(self, timeSeries):

        metaData, tsValues = self._splitMeta_Data(timeSeries)

        doc = self.dbManager.addTimeSeries(metaData)
        binStore = BinaryStore(str(doc.inserted_id))
        binStore.append(tsValues)

    def appendTimeSeries(self, timeSeries):

        metaData, tsValues = self._splitMeta_Data(timeSeries)

        binStore = BinaryStore(str(timeSeries["_id"]))
        binStore.loadSeries()
        start, end = binStore.append(tsValues)

        self.dbManager.updateStartEnd(timeSeries["_id"], start, end)
    
    def getTimeSeriesFullBatch(self, ids):
        res = []
        for id in ids:
            ts = self.dbManager.gettimeSeriesById(id)
            binStore = BinaryStore(id)
            binStore.loadSeries()
            data = binStore.getFull()
            ts["data"] = [[x, y] for x, y in zip(data["time"].tolist(), data["data"].tolist())]
            ts["_id"] = str(ts["_id"])
            res.append(ts)
        return res

    def getTimeSeriesFull(self, datasetId):
        tsMeta = self.dbManager.getTimeSeries(datasetId)
        res = []
        for ts in tsMeta:
            binStore = BinaryStore(ts["_id"])
            binStore.loadSeries()
            tsData = binStore.getFull()
            ts["data"] = tsData
            res.append(ts)
        return res

    def getTimeSeriesPart(self, datasetId, start, end):
        tsMeta = self.dbManager.getTimeSeries(datasetId)
        res = []
        for ts in tsMeta:
            binStore = BinaryStore(ts["_id"])
            binStore.loadSeries()
            tsData = binStore.getPart(start, end)
            ts["data"] = tsData
            res.append(ts)
        return res

    def getDatasetTSIds(self, datasetId):
        tsMeta = self.dbManager.getTimeSeries(datasetId)
        return [x["_id"] for x in tsMeta]

    def deleteTimeSeries(self, id):
        self.dbManager.deleteTimeSeries(id);
        binStore = BinaryStore(id)
        binStore.delete()
        return {"message": "Delete successful"}

    def deleteTimeSeriesBatch(self, ids):
        for id in ids:
            self.deleteTimeSeries(id)