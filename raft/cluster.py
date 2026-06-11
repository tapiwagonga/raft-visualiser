import asyncio
import logging
from typing import Callable

from .node import RaftNode, NodeEvent

logger = logging.getLogger(__name__)

NUM_NODES = 5


class Cluster:
    """
    Manages a 5-node Raft cluster.

    Responsibilities:
    - Create and wire up nodes (each node gets send_message + emit_event callbacks)
    - Route messages between nodes with a small simulated network delay
    - Broadcast NodeEvents to all connected WebSocket clients
    - Expose control actions: crash_node, recover_node, submit_command
    """

    def __init__(self):
        self.nodes: dict[int, RaftNode] = {}
        self._broadcast_callbacks: list[Callable] = []
        self._network_delay = 0.05   # 50ms simulated network latency
        self._partition: set[tuple] = set()  # pairs of (from, to) that are blocked

    def add_broadcast_listener(self, cb: Callable):
        """WebSocket handler registers here to receive events."""
        self._broadcast_callbacks.append(cb)

    def remove_broadcast_listener(self, cb: Callable):
        self._broadcast_callbacks.discard(cb) if hasattr(self._broadcast_callbacks, 'discard') else None
        if cb in self._broadcast_callbacks:
            self._broadcast_callbacks.remove(cb)

    async def start(self):
        """Create all nodes and start them."""
        node_ids = list(range(NUM_NODES))

        for nid in node_ids:
            peers = [p for p in node_ids if p != nid]
            node = RaftNode(
                node_id=nid,
                peer_ids=peers,
                send_message=self._make_sender(nid),
                emit_event=self._on_event,
            )
            self.nodes[nid] = node

        # Start all nodes — they'll begin their election timers
        await asyncio.gather(*[n.start() for n in self.nodes.values()])
        logger.info("Cluster started with %d nodes", NUM_NODES)

    async def stop(self):
        await asyncio.gather(*[n.stop() for n in self.nodes.values()])

    # ------------------------------------------------------------------ #
    # Control actions (called from WebSocket message handlers)
    # ------------------------------------------------------------------ #

    async def crash_node(self, node_id: int):
        if node_id in self.nodes:
            await self.nodes[node_id].crash()
            logger.info("Node %d crashed", node_id)

    async def recover_node(self, node_id: int):
        if node_id in self.nodes:
            await self.nodes[node_id].recover()
            logger.info("Node %d recovered", node_id)

    async def submit_command(self, command: str) -> bool:
        """Submit a command to whichever node is currently the leader."""
        for node in self.nodes.values():
            if not node.crashed:
                success = await node.submit_command(command)
                if success:
                    return True
        return False

    def partition(self, node_a: int, node_b: int):
        """Block messages between two nodes (network partition)."""
        self._partition.add((node_a, node_b))
        self._partition.add((node_b, node_a))
        logger.info("Partition created between nodes %d and %d", node_a, node_b)

    def heal_partition(self):
        """Remove all network partitions."""
        self._partition.clear()
        logger.info("All partitions healed")

    def set_network_delay(self, delay_ms: int):
        self._network_delay = delay_ms / 1000

    def full_snapshot(self) -> dict:
        """Complete cluster state — sent to newly connected WebSocket clients."""
        return {
            "type": "snapshot",
            "nodes": [n.snapshot() for n in self.nodes.values()],
            "partition": list(self._partition),
            "network_delay_ms": int(self._network_delay * 1000),
        }

    # ------------------------------------------------------------------ #
    # Internal routing
    # ------------------------------------------------------------------ #

    def _make_sender(self, from_id: int) -> Callable:
        """Returns a send_message function bound to from_id."""
        async def send(to_id: int, message: dict):
            if (from_id, to_id) in self._partition:
                return  # dropped — partitioned

            target = self.nodes.get(to_id)
            if target is None or target.crashed:
                return

            # Simulate network delay
            await asyncio.sleep(self._network_delay)
            await target.handle_message(message)

        return send

    async def _on_event(self, event: NodeEvent, timer_progress=None):
        """Called by any node when its state changes — broadcast to all WS clients."""
        payload = {
            "type":           "event",
            "node_id":        event.node_id,
            "state":          event.state,
            "term":           event.term,
            "voted_for":      event.voted_for,
            "votes_received": event.votes_received,
            "log_length":     event.log_length,
            "commit_index":   event.commit_index,
            "event":          event.event,
            "msg_from":       event.msg_from,
            "msg_to":         event.msg_to,
            "msg_type":       event.msg_type,
            "timer_progress": timer_progress,
        }
        for cb in list(self._broadcast_callbacks):
            try:
                await cb(payload)
            except Exception as e:
                logger.warning("Broadcast error: %s", e)