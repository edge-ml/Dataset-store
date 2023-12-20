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

        print(f"Connected to RabbitMQ. Waiting for messages on queue: {queue_name}")

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    body = json.loads(message.body)
                    command = body["command"]
                    payload = body["payload"]
                    print(f"Received message - Command: {command}, Payload: {payload}")

                    if command == "projectDelete":
                        print(f"Deleting project with ID: {payload}")
                        datasetController.deleteProjectDatasets(ObjectId(payload))
                        
                        print(f"Deleting project labeling with ID: {payload}")
                        deleteProjectLabeling([ObjectId(payload)]) 
                    
                    # payload is user id
                    elif command == "userDelete":
                        print(f"Deleting user with ID: {payload}")
                        
                        projects = datasetController.deleteUserDatasets(ObjectId(payload))
                        print(projects)
                        deleteProjectLabeling(projects)