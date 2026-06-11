import asyncio
import json
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from raft.cluster import Cluster

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

cluster = Cluster()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start the Raft cluster when the server starts
    cluster_task = asyncio.create_task(cluster.start())
    yield
    # Clean shutdown
    await cluster.stop()
    cluster_task.cancel()


app = FastAPI(lifespan=lifespan)


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    logger.info("WebSocket client connected")

    # Queue for sending events to this client
    queue: asyncio.Queue = asyncio.Queue()

    async def enqueue(payload: dict):
        await queue.put(payload)

    cluster.add_broadcast_listener(enqueue)

    try:
        # Send full snapshot immediately so the frontend has initial state
        await ws.send_text(json.dumps(cluster.full_snapshot()))

        # Two concurrent tasks: receive commands, send events
        async def receiver():
            async for raw in ws.iter_text():
                try:
                    msg = json.loads(raw)
                    await handle_command(msg)
                except json.JSONDecodeError:
                    logger.warning("Bad JSON from client: %s", raw)

        async def sender():
            while True:
                payload = await queue.get()
                await ws.send_text(json.dumps(payload))

        await asyncio.gather(receiver(), sender())

    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")
    finally:
        cluster.remove_broadcast_listener(enqueue)


async def handle_command(msg: dict):
    """Handle commands sent from the frontend."""
    action = msg.get("action")

    if action == "crash":
        await cluster.crash_node(int(msg["node_id"]))

    elif action == "recover":
        await cluster.recover_node(int(msg["node_id"]))

    elif action == "submit":
        command = msg.get("command", "SET key value")
        success = await cluster.submit_command(command)
        if not success:
            logger.warning("No leader available to accept command")

    elif action == "partition":
        cluster.partition(int(msg["node_a"]), int(msg["node_b"]))

    elif action == "heal":
        cluster.heal_partition()

    elif action == "set_delay":
        cluster.set_network_delay(int(msg.get("delay_ms", 50)))

    else:
        logger.warning("Unknown action: %s", action)


# Serve the frontend
app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")