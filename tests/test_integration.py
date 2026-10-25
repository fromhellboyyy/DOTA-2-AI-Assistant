import json

import aio_pika
import pytest
from aio_pika.exceptions import DeliveryError
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_queue_overflow_rejects_publishes():
    with DockerContainer("rabbitmq:3.12-management-alpine").with_exposed_ports(5672) as container:
        wait_for_logs(container, "Server startup complete")
        host = container.get_container_host_ip()
        port = container.get_exposed_port(5672)
        url = f"amqp://guest:guest@{host}:{port}/"

        connection = await aio_pika.connect_robust(url)
        channel = await connection.channel()

        queue = await channel.declare_queue(
            "it_overflow_queue",
            durable=True,
            arguments={
                "x-max-length": 5,
                "x-overflow": "reject-publish",
            },
        )

        nacks = 0
        for i in range(30):
            try:
                await channel.default_exchange.publish(
                    aio_pika.Message(body=json.dumps({"task_id": f"t{i}"}).encode()),
                    routing_key=queue.name,
                )
            except DeliveryError:
                nacks += 1

        assert nacks > 0

        await connection.close()
