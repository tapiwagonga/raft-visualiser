const SVG_W = 640, SVG_H = 520;
const CX = SVG_W / 2, CY = SVG_H / 2 - 10;
const RING_R = 162, NODE_R = 40, NUM_NODES = 5;

function nodePos(id) {
  const a = (id / NUM_NODES) * 2 * Math.PI - Math.PI / 2;
  return { x: CX + RING_R * Math.cos(a), y: CY + RING_R * Math.sin(a) };
}

const nodeState = {};
let selectedNode = null;
let totalEvents  = 0;
let ws = null;
const activePartitions = new Set();

const svg = document.getElementById("cluster");

function el(tag, attrs) {
  const e = document.createElementNS("http://www.w3.org/2000/svg", tag);
  for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v);
  return e;
}

function arc(cx, cy, r, a0, a1) {
  if (a1 - a0 >= 359.9) a1 = a0 + 359.9;
  const toR = d => (d - 90) * Math.PI / 180;
  const s  = { x: cx + r * Math.cos(toR(a1)), y: cy + r * Math.sin(toR(a1)) };
  const e2 = { x: cx + r * Math.cos(toR(a0)), y: cy + r * Math.sin(toR(a0)) };
  return `M ${s.x} ${s.y} A ${r} ${r} 0 ${a1 - a0 > 180 ? 1 : 0} 0 ${e2.x} ${e2.y}`;
}

const edgeGroup = el("g", { id: "edges" });
const partGroup = el("g", { id: "parts" });
const msgGroup  = el("g", { id: "msgs" });
const nodeGroup = el("g", { id: "nodes" });
svg.append(edgeGroup, partGroup, msgGroup, nodeGroup);

for (let a = 0; a < NUM_NODES; a++) {
  for (let b = a + 1; b < NUM_NODES; b++) {
    const pa = nodePos(a), pb = nodePos(b);
    edgeGroup.appendChild(el("line", {
      id: `edge-${a}-${b}`,
      x1: pa.x, y1: pa.y, x2: pb.x, y2: pb.y,
      stroke: "#D8D0C4", "stroke-width": "1.5",
    }));
  }
}

const COLORS = {
  leader:    { fill: "#E8F5E9", stroke: "#2D6A4F", label: "#1A4036" },
  candidate: { fill: "#FFFCE6", stroke: "#C4900A", label: "#5A3C00" },
  follower:  { fill: "#FDFAF6", stroke: "#9C8E84", label: "#4A4540" },
  crashed:   { fill: "#F2EFEC", stroke: "#C0B8AF", label: "#9C8E84" },
};

for (let id = 0; id < NUM_NODES; id++) {
  const { x, y } = nodePos(id);
  const g = el("g", { class: "node-ring", id: `node-${id}`, transform: `translate(${x},${y})` });

  g.appendChild(el("circle", { class: "timer-track", r: NODE_R + 11, fill: "none", stroke: "#E8E2DA", "stroke-width": "3" }));
  g.appendChild(el("path",   { class: "timer-arc",   fill: "none", stroke: "#D4A017", "stroke-width": "3", "stroke-linecap": "round", opacity: "0", d: "" }));
  g.appendChild(el("path",   { class: "commit-arc",  fill: "none", stroke: "#40916C", "stroke-width": "2.5", "stroke-linecap": "round", opacity: "0", d: "" }));
  g.appendChild(el("circle", { class: "vote-ring",   r: NODE_R + 6, fill: "none", stroke: "#C4900A", "stroke-width": "1.5", "stroke-dasharray": "5 3", opacity: "0" }));
  g.appendChild(el("circle", { class: "node-bg",     r: NODE_R, fill: "#FDFAF6", stroke: "#9C8E84", "stroke-width": "2.5" }));

  const lbl = el("text", { class: "node-label", "text-anchor": "middle", dy: "-7", "font-size": "13", fill: "#4A4540" });
  lbl.textContent = `N${id}`;
  g.appendChild(lbl);

  const roleEl = el("text", { class: "node-role", "text-anchor": "middle", dy: "7", "font-size": "10", fill: "#8C867E" });
  roleEl.textContent = "follower";
  g.appendChild(roleEl);

  const termEl = el("text", { class: "node-term", "text-anchor": "middle", dy: "19", "font-size": "9", fill: "#B0A898" });
  termEl.textContent = "term 0";
  g.appendChild(termEl);

  const badge = el("text", { class: "log-badge", "text-anchor": "middle", dy: NODE_R + 20, "font-size": "9", fill: "#B0A898" });
  badge.textContent = "";
  g.appendChild(badge);

  g.addEventListener("click", () => selectNode(id));
  nodeGroup.appendChild(g);
  nodeState[id] = { state: "follower", term: 0, crashed: false, log_length: 0, commit_index: 0 };
}

const partA = document.getElementById("part-a");
const partB = document.getElementById("part-b");
for (let i = 0; i < NUM_NODES; i++) {
  partA.innerHTML += `<option value="${i}">Node ${i}</option>`;
  partB.innerHTML += `<option value="${i}">Node ${i}</option>`;
}
partB.value = "1";

function renderNode(id, data) {
  const g = document.getElementById(`node-${id}`);
  if (!g) return;
  const ds = data.crashed ? "crashed" : data.state;
  const c  = COLORS[ds] || COLORS.follower;

  g.querySelector(".node-bg").setAttribute("fill",   c.fill);
  g.querySelector(".node-bg").setAttribute("stroke", c.stroke);
  g.querySelector(".node-label").setAttribute("fill", c.label);
  g.querySelector(".node-role").textContent = ds;
  g.querySelector(".node-role").setAttribute("fill", c.label);
  g.querySelector(".node-term").textContent = `term ${data.term}`;

  const ll = data.log_length   || 0;
  const ci = data.commit_index || 0;
  g.querySelector(".log-badge").textContent = ll > 0 ? `log ${ll}  committed ${ci}` : "";
  g.querySelector(".vote-ring").setAttribute("opacity", data.state === "candidate" ? "1" : "0");

  const ca = g.querySelector(".commit-arc");
  if (ll > 0 && !data.crashed) {
    const frac = Math.min(ci / ll, 1);
    ca.setAttribute("d", arc(0, 0, NODE_R + 18, 0, frac * 359.9));
    ca.setAttribute("opacity", frac > 0.02 ? "0.9" : "0");
  } else {
    ca.setAttribute("opacity", "0");
  }

  let sel = g.querySelector(".sel-ring");
  if (id === selectedNode) {
    if (!sel) {
      sel = el("circle", { class: "sel-ring", r: NODE_R + 26, fill: "none", stroke: "#B5651D", "stroke-width": "2", "stroke-dasharray": "5 4" });
      g.insertBefore(sel, g.firstChild);
    }
  } else {
    sel?.remove();
  }
}

function renderTimerArc(id, progress, state) {
  const g = document.getElementById(`node-${id}`);
  if (!g) return;
  const arcEl   = g.querySelector(".timer-arc");
  const trackEl = g.querySelector(".timer-track");
  if (state === "leader" || state === "crashed") {
    arcEl.setAttribute("opacity", "0");
    trackEl.setAttribute("stroke", "#E8E2DA");
    return;
  }
  const deg = progress * 359.9;
  const r = Math.round(180 + 74 * progress);
  const gv = Math.round(160 - 110 * progress);
  arcEl.setAttribute("d", arc(0, 0, NODE_R + 11, 0, deg));
  arcEl.setAttribute("stroke", `rgb(${r},${gv},23)`);
  arcEl.setAttribute("opacity", progress > 0.04 ? "1" : "0");
}

function selectNode(id) {
  selectedNode = id === selectedNode ? null : id;
  for (let i = 0; i < NUM_NODES; i++) renderNode(i, nodeState[i] || {});
}

function redrawPartitions() {
  partGroup.innerHTML = "";
  for (let a = 0; a < NUM_NODES; a++)
    for (let b = a + 1; b < NUM_NODES; b++)
      document.getElementById(`edge-${a}-${b}`)?.setAttribute("stroke", "#D8D0C4");

  for (const key of activePartitions) {
    const [a, b] = key.split("-").map(Number);
    const pa = nodePos(a), pb = nodePos(b);
    const mx = (pa.x + pb.x) / 2, my = (pa.y + pb.y) / 2;
    const dx = pb.x - pa.x, dy = pb.y - pa.y;
    const len = Math.sqrt(dx * dx + dy * dy);
    const nx = -dy / len * 16, ny = dx / len * 16;
    const pts = [
      { x: mx - dx*0.22, y: my - dy*0.22 },
      { x: mx + nx - dx*0.05, y: my + ny - dy*0.05 },
      { x: mx - nx + dx*0.05, y: my - ny + dy*0.05 },
      { x: mx + dx*0.22, y: my + dy*0.22 },
    ];
    partGroup.appendChild(el("path", {
      d: `M${pts[0].x} ${pts[0].y} L${pts[1].x} ${pts[1].y} L${pts[2].x} ${pts[2].y} L${pts[3].x} ${pts[3].y}`,
      fill: "none", stroke: "#C0392B", "stroke-width": "2.5",
      "stroke-linecap": "round", "stroke-linejoin": "round",
    }));
    const lt = el("text", { x: mx, y: my - 10, "text-anchor": "middle", "font-size": "11", fill: "#C0392B", "font-weight": "700", "font-family": "system-ui" });
    lt.textContent = "✕ partitioned";
    partGroup.appendChild(lt);
    const ekey = a < b ? `edge-${a}-${b}` : `edge-${b}-${a}`;
    document.getElementById(ekey)?.setAttribute("stroke", "#F5CACA");
  }
}

const narratorEl = document.getElementById("narrator");

function updateNarrator() {
  const nodes      = Object.values(nodeState);
  const leader     = nodes.find(n => n.state === "leader" && !n.crashed);
  const candidates = nodes.filter(n => n.state === "candidate" && !n.crashed);
  const crashed    = nodes.filter(n => n.crashed);
  const followers  = nodes.filter(n => n.state === "follower" && !n.crashed);
  let html = "";

  if (candidates.length > 0 && !leader) {
    const names = candidates.map(n => `<span class="hl">N${n.node_id}</span>`).join(", ");
    html = `${names} ${candidates.length > 1 ? "are" : "is"} requesting votes — election in progress. Nodes vote for the first candidate they hear from in a given term.`;
  } else if (leader) {
    const followerNames = followers.map(n => `N${n.node_id}`).join(", ");
    const crashedNames  = crashed.map(n => `N${n.node_id}`).join(", ");
    if (activePartitions.size > 0) {
      html = `<span class="hl-red">Network partitioned.</span> Nodes on each side will elect their own leader. When healed, the node with the lower term will step down.`;
    } else if (crashed.length > 0) {
      const remaining = Math.floor(NUM_NODES / 2) - crashed.length;
      html = `<span class="hl-green">N${leader.node_id}</span> is leader for term ${leader.term}. <span class="hl-red">${crashedNames}</span> ${crashed.length > 1 ? "are" : "is"} offline. The cluster can tolerate ${remaining} more failure${remaining === 0 ? " — at the limit" : "s"}.`;
    } else {
      html = `<span class="hl-green">N${leader.node_id}</span> is the leader for term ${leader.term}, sending heartbeats to keep ${followerNames} in sync. The amber ring shows each follower's election timeout countdown.`;
    }
  } else if (crashed.length === NUM_NODES) {
    html = `All nodes are offline — no quorum possible.`;
  } else {
    html = `Cluster starting — nodes are waiting for their randomised election timeout to fire.`;
  }

  narratorEl.innerHTML = html;
}

const MSG = {
  vote_request:  { color: "#8B6914", r: 5 },
  vote_response: { color: "#A0780A", r: 4 },
  heartbeat:     { color: "#5A8BA8", r: 3 },
  heartbeat_ack: { color: "#6A9AB8", r: 2.5 },
  log_entry:     { color: "#2D6A4F", r: 6 },
  log_ack:       { color: "#40916C", r: 4 },
};

function animateMessage(from, to, type) {
  if (from == null || to == null) return;
  const fp = nodePos(from), tp = nodePos(to);
  const m  = MSG[type] || { color: "#8C867E", r: 4 };
  const dot = el("circle", { class: "msg-particle", r: m.r, fill: m.color, opacity: "0.85", cx: fp.x, cy: fp.y });
  msgGroup.appendChild(dot);
  const dur = 380, t0 = performance.now();
  (function step(now) {
    const t = Math.min((now - t0) / dur, 1);
    const e = t < 0.5 ? 2*t*t : -1+(4-2*t)*t;
    dot.setAttribute("cx", fp.x + (tp.x - fp.x) * e);
    dot.setAttribute("cy", fp.y + (tp.y - fp.y) * e);
    dot.setAttribute("opacity", 0.85 * (1 - t * 0.45));
    t < 1 ? requestAnimationFrame(step) : dot.remove();
  })(t0);
}

const logEl = document.getElementById("event-log");
const countBadge = document.getElementById("event-count-badge");

function addLog(nodeId, text, term, state) {
  totalEvents++;
  countBadge.textContent = totalEvents;
  const stateClass = { leader: "is-leader", candidate: "is-cand", crashed: "is-crashed" }[state] ?? "";
  const row = document.createElement("div");
  row.className = "log-entry";
  row.innerHTML = `
    <div class="log-nid ${stateClass}">N${nodeId}</div>
    <div class="log-text">${text} <span class="log-term">t${term}</span></div>
  `;
  logEl.prepend(row);
  if (logEl.children.length > 120) logEl.lastChild.remove();
}

function connect() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  ws = new WebSocket(`${proto}://${location.host}/ws`);

  ws.onopen  = () => { narratorEl.innerHTML = `Connected — waiting for first election…`; };
  ws.onclose = () => { narratorEl.innerHTML = `Reconnecting…`; setTimeout(connect, 2000); };

  ws.onmessage = ({ data }) => {
    const msg = JSON.parse(data);

    if (msg.type === "snapshot") {
      activePartitions.clear();
      for (const p of (msg.partition || [])) activePartitions.add(`${Math.min(...p)}-${Math.max(...p)}`);
      for (const n of msg.nodes) { nodeState[n.node_id] = n; renderNode(n.node_id, n); }
      redrawPartitions();
      updateNarrator();
      return;
    }

    if (msg.type === "event") {
      const { node_id, state, term, event, msg_from, msg_to, msg_type, crashed, log_length, commit_index, timer_progress } = msg;

      if (event === "__timer__") {
        renderTimerArc(node_id, timer_progress, state);
        return;
      }

      nodeState[node_id] = { ...nodeState[node_id], state, term, crashed: crashed || false, log_length: log_length || 0, commit_index: commit_index || 0 };
      renderNode(node_id, nodeState[node_id]);
      renderTimerArc(node_id, 0, state);

      if (msg_from != null && msg_to != null && msg_type) animateMessage(msg_from, msg_to, msg_type);

      const logText = event.startsWith("WON ELECTION") ? "won election → leader" : event;
      addLog(node_id, logText, term, state);
      updateNarrator();
    }
  };
}

function send(o) { ws?.readyState === 1 && ws.send(JSON.stringify(o)); }

function crashSelected() {
  if (selectedNode == null) { narratorEl.innerHTML = `<span class="hl">Click a node in the diagram first</span>, then crash it.`; return; }
  send({ action: "crash", node_id: selectedNode });
}

function recoverSelected() {
  if (selectedNode == null) { narratorEl.innerHTML = `<span class="hl">Click a node in the diagram first</span>, then recover it.`; return; }
  send({ action: "recover", node_id: selectedNode });
}

function submitCommand() {
  const cmd = document.getElementById("cmd-input").value.trim();
  if (!cmd) return;
  send({ action: "submit", command: cmd });
}

function createPartition() {
  const a = parseInt(partA.value), b = parseInt(partB.value);
  if (a === b) { narratorEl.innerHTML = `Choose two <span class="hl">different</span> nodes to partition.`; return; }
  activePartitions.add(`${Math.min(a,b)}-${Math.max(a,b)}`);
  redrawPartitions();
  send({ action: "partition", node_a: a, node_b: b });
  updateNarrator();
}

function healPartition() {
  activePartitions.clear();
  redrawPartitions();
  send({ action: "heal" });
  updateNarrator();
}

function slowNetwork() { send({ action: "set_delay", delay_ms: 300 }); narratorEl.innerHTML = `Network slowed to <span class="hl">300 ms</span> per message. Elections will be more dramatic.`; }
function fastNetwork()  { send({ action: "set_delay", delay_ms: 20  }); narratorEl.innerHTML = `Network speed restored to <span class="hl">20 ms</span>.`; }

document.getElementById("cmd-input").addEventListener("keydown", e => { if (e.key === "Enter") submitCommand(); });

connect();