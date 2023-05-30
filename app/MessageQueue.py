import asyncio
import aio_pika
import aio_pika.abc
import os

import json
from bson.objectid import ObjectId


from app.controller.dataset_controller import DatasetController
from app.controller.labelingController import deleteProjectLabeling

datasetController = DatasetController()

async def main(loop):
    connection = await aio_pika.connect_robust(
        "amqp://guest:guest@127.0.0.1/", loop=loop
    )

    async with connection:
        queue_name = "edgeml"
        channel: aio_pika.abc.AbstractChannel = await connection.channel()

        queue: aio_pika.abc.AbstractQueue = await channel.declare_queue(
            queue_name,
            auto_delete=False,
            durable=True
        )

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    body = json.loads(message.body)
                    command = body["command"]
                    payload = body["payload"]

                    if command == "projectDelete":
                        datasetController.deleteProjectDatasets(ObjectId(payload))
                        deleteProjectLabeling(ObjectId(payload))


                
