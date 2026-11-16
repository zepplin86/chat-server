// Fire many messages into one room at the same instant, one message per user.
// (One user may only send 5 per second, so the load is spread over many users.)
//
//   node scripts/blast.mjs <room name> [count=1000] [http://localhost:8080]

const roomName = process.argv[2];
const count = Number(process.argv[3] || 1000);
const base = process.argv[4] || 'http://localhost:8080';
const wsBase = base.replace(/^http/, 'ws');
if (!roomName) {
  console.error('usage: node scripts/blast.mjs <room name> [count] [base url]');
  process.exit(2);
}
const run = Date.now().toString(36);

async function api(path, token, body) {
  const res = await fetch(base + '/api' + path, {
    method: body === undefined ? 'GET' : 'POST',
    headers: { 'Content-Type': 'application/json', ...(token ? { Authorization: 'Bearer ' + token } : {}) },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`${path} -> ${res.status}`);
  return res.json();
}

/** Run `task` over `items` with at most `limit` in flight. */
async function pool(items, limit, task) {
  const results = new Array(items.length);
  let next = 0;
  await Promise.all(Array.from({ length: limit }, async () => {
    while (next < items.length) {
      const i = next++;
      results[i] = await task(items[i], i);
    }
  }));
  return results;
}

function open(token, onFrame) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`${wsBase}/ws?token=${encodeURIComponent(token)}`);
    ws.onmessage = (event) => onFrame(JSON.parse(event.data), ws);
    ws.onerror = () => reject(new Error('websocket connection failed'));
    ws.onopen = () => resolve(ws);
  });
}

function percentile(sorted, p) {
  return sorted.length ? sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))] : 0;
}

// ------------------------------------------------------------------ setup
const admin = await api('/login', null, { name: `blast-${run}` });
const room = (await api('/rooms', admin.token)).rooms.find((r) => r.name === roomName);
if (!room) {
  console.error(`room "${roomName}" not found`);
  process.exit(1);
}
console.log(`room "${room.name}" id=${room.id} type=${room.type}`);

console.log(`logging in ${count} users...`);
const users = await pool([...Array(count).keys()], 50, (i) => api('/login', null, { name: `blast-${run}-${i}` }));
if (room.type === 'group') {
  await pool(users, 50, (u) => api(`/rooms/${room.id}/join`, u.token, {}));
}

// One extra connection that only listens, to see what a viewer actually receives.
const seen = new Set();
let listenerJoined;
const listenerReady = new Promise((r) => (listenerJoined = r));
const listener = await open(admin.token, (f, ws) => {
  if (f.t === 'ready') ws.send(JSON.stringify({ t: 'join', room_id: room.id }));
  if (f.t === 'joined') listenerJoined(f);
  if (f.t === 'message') seen.add(f.seq);
});
const before = (await listenerReady).latest_seq;

console.log(`opening ${count} connections...`);
const sentAt = new Map();
const latencies = [];
const errors = {};
const gateways = {};
let acked = 0;
let maxSeq = before;
let closedEarly = 0;
let allDone;
const done = new Promise((r) => (allDone = r));
const settle = () => { if (acked + Object.values(errors).reduce((a, b) => a + b, 0) >= count) allDone(); };

const sockets = await pool(users, 100, (user, i) => new Promise((resolve, reject) => {
  open(user.token, (f, ws) => {
    if (f.t === 'ready') {
      gateways[f.gateway] = (gateways[f.gateway] || 0) + 1;
      ws.send(JSON.stringify({ t: 'join', room_id: room.id }));
    } else if (f.t === 'joined') {
      resolve(ws);
    } else if (f.t === 'ack') {
      acked++;
      maxSeq = Math.max(maxSeq, f.seq);
      latencies.push(performance.now() - sentAt.get(f.client_msg_id));
      settle();
    } else if (f.t === 'error') {
      errors[f.code] = (errors[f.code] || 0) + 1;
      if (f.client_msg_id) settle(); else reject(new Error(`join failed: ${f.code}`));
    }
  }).then((ws) => { ws.onclose = () => { closedEarly++; }; }, reject);
}));
console.log('connections per gateway:', gateways);

// ------------------------------------------------------------------ fire
console.log(`sending ${count} messages at once...`);
const start = performance.now();
sockets.forEach((ws, i) => {
  const id = `blast-${run}-${i}`;
  sentAt.set(id, performance.now());
  ws.send(JSON.stringify({ t: 'send', room_id: room.id, client_msg_id: id, body: `blast ${i + 1}/${count}` }));
});
const flushed = performance.now() - start;

const timedOut = await Promise.race([done.then(() => false), new Promise((r) => setTimeout(() => r(true), 30000))]);
const elapsed = performance.now() - start;
// Let fan-out to the listener finish.
await new Promise((r) => setTimeout(r, 2000));

// ------------------------------------------------------------------ report
latencies.sort((a, b) => a - b);
const failed = Object.values(errors).reduce((a, b) => a + b, 0);
console.log('\n--- result');
console.log(`accepted (ack)      : ${acked} / ${count}`);
console.log(`rejected            : ${failed} ${failed ? JSON.stringify(errors) : ''}`);
if (timedOut) console.log(`no answer in 30s    : ${count - acked - failed}`);
console.log(`client write time   : ${flushed.toFixed(0)} ms`);
console.log(`all answered after  : ${elapsed.toFixed(0)} ms  (${(acked / (elapsed / 1000)).toFixed(0)} msg/s)`);
console.log(`ack latency ms      : p50 ${percentile(latencies, 0.5).toFixed(0)}  p95 ${percentile(latencies, 0.95).toFixed(0)}  p99 ${percentile(latencies, 0.99).toFixed(0)}  max ${(latencies.at(-1) || 0).toFixed(0)}`);
console.log(`room seq            : ${before} -> ${maxSeq}`);
console.log(`listener received   : ${seen.size} / ${acked} messages over the realtime path`);
console.log(`connections dropped : ${closedEarly}`);

for (const ws of sockets) ws.onclose = null;
sockets.forEach((ws) => ws.close());
listener.close();
setTimeout(() => process.exit(acked === count ? 0 : 1), 500);
