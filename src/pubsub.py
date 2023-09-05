import os
from typing import AsyncGenerator, TypeVar

import aioredis
from aiofauna.json import to_json
from aiofauna.typedefs import LazyProxy
from aiofauna.utils import handle_errors, setup_logging
from aioredis.client import PubSub

from .service import function_call

logger = setup_logging(__name__)

pool = aioredis.Redis.from_url(os.environ["REDIS_URL"])


class FunctionQueue(LazyProxy[PubSub]):
    """
    FunctionQueue Event Stream to catch function call event results
    and push them to the client via Server Sent Events.
    """

    def __init__(self, namespace: str):
        self.namespace = namespace
        self.pubsub = self.__load__()
        super().__init__()

    def __load__(self):
        """
        Lazy loading of the PubSub object.
        """
        return pool.pubsub()

    async def sub(self) -> AsyncGenerator[str, None]:
        """
        Subscribes to the PubSub channel and yields messages as they come in.
        """
        await self.pubsub.subscribe(self.namespace)
        logger.info("Subscribed to %s", self.namespace)
        async for message in self.pubsub.listen():
            try:
                data = message["data"]
                yield data.decode("utf-8")
            except (KeyError, AssertionError, UnicodeDecodeError, AttributeError):
                continue

    @handle_errors
    async def _send(self, message: str) -> None:
        """
        Protected method to send a message to the PubSub channel.
        """
        await pool.publish(self.namespace, message)
        logger.info("Message published to %s", self.namespace)

    @handle_errors
    async def pub(self, message: str) -> None:
        """
        Public method to send a function call result to the PubSub channel.
        """
        response = await function_call(text=message)
        logger.info("Sending response %s", response)
        await self._send(to_json(response))
        await self.pubsub.unsubscribe(self.namespace)
