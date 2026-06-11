import asyncio
import random
import logging
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, Callable, Awaitable

from .messages import (
    VoteRequest, VoteResponse, Heartbeat, HeartbeatAck,
    AppendEntries, AppendEntriesAck, LogEntry
)

logger = logging.getLogger(__name__)


class NodeState(str, Enum):
    FOLLOWER  = "follower"
    CANDIDATE = "candidate"
    LEADER    = "leader"


# Raft timing constants (milliseconds → seconds for asyncio)
ELECTION_TIMEOUT_MIN = 1.5   # seconds
ELECTION_TIMEOUT_MAX = 3.0
HEARTBEAT_INTERVAL   = 0.5   # leader sends heartbeats this often


@dataclass
class NodeEvent:
    """Everything the visualiser needs to know about a state change."""
    node_id: int
    state: str
    term: int
    voted_for: Optional[int]
    votes_received: int
    log_length: int
    commit_index: int
    event: str          # human-readable description e.g. "became candidate"
    # For animating messages between nodes:
    msg_from: Optional[int] = None
    msg_to:   Optional[int] = None
    msg_type: Optional[str] = None


class RaftNode:
    """
    A single Raft node. Runs as an asyncio task.

    The node communicates with peers via send_message, which the Cluster
    wires up. All state transitions emit events via emit_event so the
    visualiser stays in sync.
    """

    def __init__(
        self,
        node_id: int,
        peer_ids: list[int],
        send_message: Callable,        # async fn(to_id, message)
        emit_event: Callable,          # async fn(NodeEvent)
    ):
        self.node_id      = node_id
        self.peer_ids     = peer_ids
        self.send_message = send_message
        self.emit_event   = emit_event

        # Persistent Raft state
        self.current_term = 0
        self.voted_for: Optional[int] = None
        self.log: list[LogEntry] = []

        # Volatile state
        self.state        = NodeState.FOLLOWER
        self.commit_index = 0
        self.last_applied = 0

        # Leader-only state
        self.next_index:  dict[int, int] = {}
        self.match_index: dict[int, int] = {}
        self.votes_received: set[int]    = set()

        # Timer control
        self._election_timer_task: Optional[asyncio.Task] = None
        self._heartbeat_task:      Optional[asyncio.Task] = None
        self._running = False

        # Whether this node is simulated as crashed
        self.crashed = False

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    async def start(self):
        self._running = True
        await self._emit("node started")
        self._reset_election_timer()

    async def stop(self):
        self._running = False
        self._cancel_timers()

    async def crash(self):
        """Simulate a node crash — stops all activity."""
        self.crashed = True
        self._cancel_timers()
        self.state = NodeState.FOLLOWER
        await self._emit("crashed")

    async def recover(self):
        """Bring a crashed node back online."""
        self.crashed = False
        self.state = NodeState.FOLLOWER
        self.voted_for = None
        await self._emit("recovered")
        self._reset_election_timer()

    # ------------------------------------------------------------------ #
    # Message handling — called by Cluster when a message arrives
    # ------------------------------------------------------------------ #

    async def handle_message(self, message: dict):
        if self.crashed:
            return

        msg_type = message.get("type")

        if msg_type == "vote_request":
            await self._handle_vote_request(VoteRequest(**message))
        elif msg_type == "vote_response":
            await self._handle_vote_response(VoteResponse(**message))
        elif msg_type == "heartbeat":
            await self._handle_heartbeat(Heartbeat(**message))
        elif msg_type == "heartbeat_ack":
            await self._handle_heartbeat_ack(HeartbeatAck(**message))
        elif msg_type == "log_entry":
            await self._handle_append_entries(AppendEntries(**message))
        elif msg_type == "log_ack":
            await self._handle_append_ack(AppendEntriesAck(**message))

    async def _handle_vote_request(self, req: VoteRequest):
        # If we see a higher term, step down
        if req.term > self.current_term:
            await self._step_down(req.term)

        grant = (
            req.term >= self.current_term
            and (self.voted_for is None or self.voted_for == req.candidate_id)
            and self._candidate_log_ok(req.last_log_index, req.last_log_term)
        )

        if grant:
            self.voted_for = req.candidate_id
            self._reset_election_timer()

        resp = VoteResponse(
            term=self.current_term,
            voter_id=self.node_id,
            granted=grant,
        )
        await self._send(req.candidate_id, resp, "vote_response")
        await self._emit(
            f"{'granted' if grant else 'denied'} vote to node {req.candidate_id}",
            msg_from=req.candidate_id, msg_type="vote_response"
        )

    async def _handle_vote_response(self, resp: VoteResponse):
        if resp.term > self.current_term:
            await self._step_down(resp.term)
            return

        if self.state != NodeState.CANDIDATE:
            return

        if resp.granted:
            self.votes_received.add(resp.voter_id)
            await self._emit(
                f"received vote from node {resp.voter_id} ({len(self.votes_received)}/{self._quorum()} needed)",
                msg_from=resp.voter_id, msg_type="vote_response"
            )
            if len(self.votes_received) >= self._quorum():
                await self._become_leader()

    async def _handle_heartbeat(self, hb: Heartbeat):
        if hb.term < self.current_term:
            return

        if hb.term > self.current_term:
            await self._step_down(hb.term)

        # Valid heartbeat from current leader — reset our timer
        self._reset_election_timer()
        if self.state == NodeState.CANDIDATE:
            self.state = NodeState.FOLLOWER
            await self._emit(f"stepped down — leader {hb.leader_id} is alive")

        ack = HeartbeatAck(term=self.current_term, follower_id=self.node_id)
        await self._send(hb.leader_id, ack, "heartbeat_ack")
        await self._emit(
            f"heartbeat from leader {hb.leader_id}",
            msg_from=hb.leader_id, msg_type="heartbeat"
        )

    async def _handle_heartbeat_ack(self, ack: HeartbeatAck):
        if ack.term > self.current_term:
            await self._step_down(ack.term)

    async def _handle_append_entries(self, req: AppendEntries):
        if req.term < self.current_term:
            nack = AppendEntriesAck(term=self.current_term, follower_id=self.node_id, success=False)
            await self._send(req.leader_id, nack, "log_ack")
            return

        self._reset_election_timer()
        if req.term > self.current_term:
            await self._step_down(req.term)

        # Append entries to log
        for entry_dict in req.entries:
            entry = LogEntry(**entry_dict)
            self.log.append(entry)

        if req.leader_commit > self.commit_index:
            self.commit_index = min(req.leader_commit, len(self.log))

        ack = AppendEntriesAck(
            term=self.current_term,
            follower_id=self.node_id,
            success=True,
            match_index=len(self.log),
        )
        await self._send(req.leader_id, ack, "log_ack")
        await self._emit(
            f"appended {len(req.entries)} entries from leader {req.leader_id}",
            msg_from=req.leader_id, msg_type="log_entry"
        )

    async def _handle_append_ack(self, ack: AppendEntriesAck):
        if ack.term > self.current_term:
            await self._step_down(ack.term)
            return

        if self.state != NodeState.LEADER:
            return

        if ack.success:
            self.match_index[ack.follower_id] = ack.match_index
            self.next_index[ack.follower_id]  = ack.match_index + 1
            await self._try_advance_commit()

    # ------------------------------------------------------------------ #
    # State transitions
    # ------------------------------------------------------------------ #

    async def _start_election(self):
        if self.crashed:
            return

        self.current_term += 1
        self.state         = NodeState.CANDIDATE
        self.voted_for     = self.node_id
        self.votes_received = {self.node_id}  # vote for ourselves

        await self._emit("election timeout — started election")

        req = VoteRequest(
            term=self.current_term,
            candidate_id=self.node_id,
            last_log_index=len(self.log),
            last_log_term=self.log[-1].term if self.log else 0,
        )
        for peer in self.peer_ids:
            await self._send(peer, req, "vote_request")

        self._reset_election_timer()

    async def _become_leader(self):
        self.state = NodeState.LEADER
        for peer in self.peer_ids:
            self.next_index[peer]  = len(self.log) + 1
            self.match_index[peer] = 0

        await self._emit(f"WON ELECTION — became leader of term {self.current_term}")
        self._cancel_timers()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _step_down(self, new_term: int):
        self.current_term = new_term
        self.state        = NodeState.FOLLOWER
        self.voted_for    = None
        self._cancel_timers()
        self._reset_election_timer()
        await self._emit(f"stepped down to follower at term {new_term}")

    # ------------------------------------------------------------------ #
    # Leader behaviour
    # ------------------------------------------------------------------ #

    async def _heartbeat_loop(self):
        """Leader sends heartbeats to all peers every HEARTBEAT_INTERVAL."""
        while self._running and self.state == NodeState.LEADER and not self.crashed:
            hb = Heartbeat(
                term=self.current_term,
                leader_id=self.node_id,
                prev_log_index=len(self.log),
                prev_log_term=self.log[-1].term if self.log else 0,
                leader_commit=self.commit_index,
            )
            for peer in self.peer_ids:
                await self._send(peer, hb, "heartbeat")
            await self._emit("sent heartbeats")
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def submit_command(self, command: str):
        """Called externally to append a command to the leader's log."""
        if self.state != NodeState.LEADER:
            return False

        entry = LogEntry(term=self.current_term, index=len(self.log) + 1, command=command)
        self.log.append(entry)
        await self._emit(f"appended command to log: '{command}'")

        # Replicate to all peers
        for peer in self.peer_ids:
            await self._replicate_to(peer)
        return True

    async def _replicate_to(self, peer_id: int):
        next_idx  = self.next_index.get(peer_id, 1)
        entries   = [vars(e) for e in self.log[next_idx - 1:]]
        prev_idx  = next_idx - 1
        prev_term = self.log[prev_idx - 1].term if prev_idx > 0 and self.log else 0

        req = AppendEntries(
            term=self.current_term,
            leader_id=self.node_id,
            prev_log_index=prev_idx,
            prev_log_term=prev_term,
            entries=entries,
            leader_commit=self.commit_index,
        )
        await self._send(peer_id, req, "log_entry")

    async def _try_advance_commit(self):
        """Advance commit_index if a majority have replicated up to a new entry."""
        for n in range(len(self.log), self.commit_index, -1):
            if self.log[n - 1].term != self.current_term:
                continue
            replicated = sum(1 for mid in self.match_index.values() if mid >= n)
            if replicated + 1 >= self._quorum():  # +1 for self
                self.commit_index = n
                await self._emit(f"committed log up to index {n}")
                break

    # ------------------------------------------------------------------ #
    # Election timer
    # ------------------------------------------------------------------ #

    def _reset_election_timer(self):
        if self._election_timer_task:
            self._election_timer_task.cancel()
        if self._running and not self.crashed:
            timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
            self._election_timeout_duration = timeout
            self._election_timer_start = asyncio.get_event_loop().time()
            self._election_timer_task = asyncio.create_task(self._election_timeout(timeout))

    async def _election_timeout(self, delay: float):
        tick = 0.1  # emit progress every 100ms
        elapsed = 0.0
        while elapsed < delay:
            await asyncio.sleep(tick)
            elapsed += tick
            if self.state == NodeState.LEADER or self.crashed:
                return
            progress = min(elapsed / delay, 1.0)
            await self.emit_event(NodeEvent(
                node_id=self.node_id,
                state=self.state.value,
                term=self.current_term,
                voted_for=self.voted_for,
                votes_received=len(self.votes_received),
                log_length=len(self.log),
                commit_index=self.commit_index,
                event="__timer__",
                msg_from=None, msg_to=None, msg_type=None,
            ), timer_progress=progress)
        if self.state != NodeState.LEADER and not self.crashed:
            await self._start_election()

    def _cancel_timers(self):
        if self._election_timer_task:
            self._election_timer_task.cancel()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #

    def _quorum(self) -> int:
        return (len(self.peer_ids) + 1) // 2 + 1

    def _candidate_log_ok(self, last_log_index: int, last_log_term: int) -> bool:
        my_last_term  = self.log[-1].term  if self.log else 0
        my_last_index = len(self.log)
        if last_log_term != my_last_term:
            return last_log_term > my_last_term
        return last_log_index >= my_last_index

    async def _send(self, to_id: int, message, msg_type: str):
        await self.send_message(to_id, vars(message))

    async def _emit(self, event: str, msg_from=None, msg_to=None, msg_type=None):
        await self.emit_event(NodeEvent(
            node_id=self.node_id,
            state=self.state.value,
            term=self.current_term,
            voted_for=self.voted_for,
            votes_received=len(self.votes_received),
            log_length=len(self.log),
            commit_index=self.commit_index,
            event=event,
            msg_from=msg_from,
            msg_to=msg_to if msg_to else self.node_id,
            msg_type=msg_type,
        ), timer_progress=None)

    def snapshot(self) -> dict:
        """Current state as a dict — for sending full cluster state to new WebSocket clients."""
        return {
            "node_id":        self.node_id,
            "state":          self.state.value,
            "term":           self.current_term,
            "voted_for":      self.voted_for,
            "votes_received": len(self.votes_received),
            "log_length":     len(self.log),
            "commit_index":   self.commit_index,
            "crashed":        self.crashed,
        }