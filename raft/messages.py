from dataclasses import dataclass, field
from typing import Optional
from enum import Enum


class MessageType(str, Enum):
    VOTE_REQUEST    = "vote_request"
    VOTE_RESPONSE   = "vote_response"
    HEARTBEAT       = "heartbeat"
    HEARTBEAT_ACK   = "heartbeat_ack"
    LOG_ENTRY       = "log_entry"
    LOG_ACK         = "log_ack"


@dataclass
class VoteRequest:
    type: str = MessageType.VOTE_REQUEST
    term: int = 0
    candidate_id: int = 0
    last_log_index: int = 0
    last_log_term: int = 0


@dataclass
class VoteResponse:
    type: str = MessageType.VOTE_RESPONSE
    term: int = 0
    voter_id: int = 0
    granted: bool = False


@dataclass
class Heartbeat:
    type: str = MessageType.HEARTBEAT
    term: int = 0
    leader_id: int = 0
    # Also serves as AppendEntries with no entries (log replication comes later)
    prev_log_index: int = 0
    prev_log_term: int = 0
    leader_commit: int = 0


@dataclass
class HeartbeatAck:
    type: str = MessageType.HEARTBEAT_ACK
    term: int = 0
    follower_id: int = 0
    success: bool = True


@dataclass
class LogEntry:
    term: int
    index: int
    command: str  # e.g. "SET foo bar"


@dataclass
class AppendEntries:
    type: str = MessageType.LOG_ENTRY
    term: int = 0
    leader_id: int = 0
    prev_log_index: int = 0
    prev_log_term: int = 0
    entries: list = field(default_factory=list)
    leader_commit: int = 0


@dataclass
class AppendEntriesAck:
    type: str = MessageType.LOG_ACK
    term: int = 0
    follower_id: int = 0
    success: bool = False
    match_index: int = 0