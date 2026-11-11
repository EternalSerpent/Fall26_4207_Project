import json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app = FastAPI()

# All currently connected clients (phones, projector, browser tabs)
clients: set[WebSocket] = set()


async def broadcast(message: str) -> None:
    """Send a message to every connected client."""
    for ws in list(clients):
        try:
            await ws.send_text(message)
        except Exception:
            clients.discard(ws)


async def publish(message: str) -> None:
    """Where a stroke goes after it arrives from a client.

    Right now it goes straight to broadcast(). When the Kafka pipeline is
    ready, this function will send to the Kafka topic instead, and a Kafka
    consumer will call broadcast(). That swap is the only change needed.
    """
    await broadcast(message)


""" Not technically necessary, but it's good to have a health check endpoint for the load balancer. """
@app.get("/health")
async def health():
    return {"status": "ok"}


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    try:
        while True:
            text = await ws.receive_text()
            try:
                json.loads(text)  # ignore anything that isn't valid JSON
            except json.JSONDecodeError:
                continue
            await publish(text)
    except WebSocketDisconnect:
        clients.discard(ws)