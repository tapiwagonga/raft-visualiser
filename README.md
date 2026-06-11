# Raft visualiser

A real-time visualiser for the [Raft consensus algorithm](https://raft.github.io/raft.pdf). Five nodes run genuine Raft state machines in Python — leader election, log replication, heartbeats — and stream every state change over WebSockets to an SVG frontend.

Built as a learning project to understand distributed systems from the inside out.

## Quick start

```bash
pip install -r requirements.txt
uvicorn main:app --reload
```

Open `http://localhost:8000`. Within a couple of seconds one node will win the first election and become leader.

## What you're looking at

Each circle is a Raft node. Three visual indicators surround every node:

- **Amber ring** — election timeout countdown. Each follower picks a random delay; when this fills, the node becomes a candidate and starts an election. Randomised timeouts are why Raft avoids split votes.
- **Green arc** — commit progress. Shows how much of the leader's log has been acknowledged by a majority and is safe to apply. Replication lag becomes visible.
- **Particles** — every RPC flying between nodes in real time. Purple = vote requests, blue = heartbeats, green = log replication.

A plain-English narrator at the bottom of the diagram explains what the cluster is doing as it happens.

## Things to try

**Kill the leader** — click a node, then hit "Crash node". The followers notice the missing heartbeats, their election timers fill, and one wins a new election. Watch the term number increment.

**Log replication** — type any command in the sidebar and send it to the leader. Watch green particles fan out to every follower and the commit arc fill as acknowledgements come back.

**Network partition** — split two nodes and watch each side elect its own leader (split-brain). Heal the partition and the node with the lower term steps down immediately.

**Slow the network** — 300ms latency makes elections chaotic and replication lag obvious.

## Architecture

```
raft/
├── messages.py   typed dataclasses for every Raft RPC
├── node.py       RaftNode state machine (follower → candidate → leader)
└── cluster.py    wires 5 nodes, routes messages, broadcasts events to clients
main.py           FastAPI — WebSocket endpoint + static file serving
frontend/
└── index.html    SVG visualiser, pure JS, connects over WebSocket
```

All Raft logic lives in Python. The frontend only renders what it receives — swapping the simulator for a real Go implementation requires no frontend changes.

Each `RaftNode` runs as an independent `asyncio` task with its own election timer. The cluster routes messages between nodes with a configurable simulated network delay. Every state change emits a typed event that gets broadcast to all connected WebSocket clients.

## Raft concepts implemented

- Leader election with randomised election timeouts
- Heartbeat suppression — followers reset their timer on every valid heartbeat from the current leader
- Log replication via AppendEntries RPC
- Commit index advancement when a majority of nodes acknowledge an entry
- Term-based authority — any node that sees a higher term steps down immediately
- Network partitions and healing

## Stack

- **Python 3.11+** with `asyncio` for concurrent node simulation
- **FastAPI** + **uvicorn** for the WebSocket server
- **Vanilla JS + SVG** — no frontend framework, no build step

## What's next

- [ ] Wire to the Go KV store built alongside this project
- [ ] Pause / step mode — advance one message at a time
- [ ] Log panel — each node's actual log entries side by side
- [ ] Scenario presets — one-click "kill the leader", "3-2 partition", "flood with writes"