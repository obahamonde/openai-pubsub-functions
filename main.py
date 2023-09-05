from aiofauna import *

from src import *

app = APIServer(servers=[{"url": "http://localhost:8888"}])


@app.sse("/functions/{namespace}")
async def function_queue_endpoint(namespace: str, sse:EventSourceResponse):
    """
    FunctionQueue Event Stream to catch function call event results
    and push them to the client via Server Sent Events.
    """
    queue = FunctionQueue(namespace)
    while True:
        async for message in queue.sub():
            await sse.send(message)

@app.post("/functions/{namespace}")
async def function_call_endpoint(namespace: str, text:str):
    """
    Public method to send a function call result to the PubSub channel.
    """
    queue = FunctionQueue(namespace)
    await queue.pub(text)
    return {"message": "success"}