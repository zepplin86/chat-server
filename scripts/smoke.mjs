// End-to-end smoke test against a running stack. Needs Node 22+ (built-in fetch and WebSocket).
//
//   node scripts/smoke.mjs [http://localhost:8080]

const base = process.argv[2] || 'http://localhost:8080';
const wsBase = base.replace(/^http/, 'ws');
const run = Date.now().toString(36);
let failures = 0;

function check(name, ok, detail = '') {
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? '  (' + detail + ')' : ''}`);
  if (!ok) failures++;
}

async function api(path, token, body) {
  const res = await fetch(base + '/api' + path, {
    method: body === undefined ? 'GET' : 'POST',
    headers: { 'Content-Type': 'application/json', ...(token ? { Authorization: 'Bearer ' + token } : {}) },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  return { status: res.status, data: await res.json().catch(() => ({})) };
}

async function login(name) {
  const { data } = await api('/login', null, { name: `${name}-${run}` });
  return data;
}

/** WebSocket wrapper that lets the test wait for a frame matching a predicate. */
async function connect(token) {
  const ws = new WebSocket(`${wsBase}/ws?token=${encodeURIComponent(token)}`);
  const frames = [];
  const waiters = [];
  ws.onmessage = (event) => {
    const frame = JSON.parse(event.data);
    const i = waiters.findIndex((w) => w.match(frame));
    if (i >= 0) waiters.splice(i, 1)[0].resolve(frame);
    else frames.push(frame);
  };
  const client = {
    send: (frame) => ws.send(JSON.stringify(frame)),
    close: () => ws.close(),
    wait(match, ms = 5000) {
      const i = frames.findIndex(match);
      if (i >= 0) return Promise.resolve(frames.splice(i, 1)[0]);
      return new Promise((resolve, reject) => {
        const waiter = { match, resolve };
        waiters.push(waiter);
        setTimeout(() => {
          const j = waiters.indexOf(waiter);
          if (j >= 0) {
            waiters.splice(j, 1);
            reject(new Error('timed out waiting for frame'));
          }
        }, ms);
      });
    },
  };
  await new Promise((resolve, reject) => {
    ws.onopen = resolve;
    ws.onerror = () => reject(new Error('websocket connection failed'));
  });
  client.ready = await client.wait((f) => f.t === 'ready');
  return client;
}

const type = (t) => (f) => f.t === t;

try {
  const alice = await login('alice');
  const bob = await login('bob');
  const carol = await login('carol');
  check('login returns a token', Boolean(alice.token && bob.token && carol.token));

  const group = (await api('/rooms', alice.token, { name: `group-${run}`, type: 'group' })).data;
  const live = (await api('/rooms', alice.token, { name: `live-${run}`, type: 'live' })).data;
  check('rooms created', group.id > 0 && live.id > 0);
  await api(`/rooms/${group.id}/join`, bob.token, {});

  const a = await connect(alice.token);
  const b = await connect(bob.token);
  const c = await connect(carol.token);
  const gateways = [a, b, c].map((x) => x.ready.gateway);
  check('connections spread over more than one gateway', new Set(gateways).size > 1, gateways.join(', '));

  // --- group room: membership, delivery, ack
  a.send({ t: 'join', room_id: group.id });
  b.send({ t: 'join', room_id: group.id });
  await a.wait(type('joined'));
  await b.wait(type('joined'));

  c.send({ t: 'join', room_id: group.id });
  const denied = await c.wait(type('error'));
  check('non-member cannot join a group room', denied.code === 'FORBIDDEN', denied.code);

  b.send({ t: 'send', room_id: group.id, client_msg_id: 'm1', body: 'hello' });
  const ack = await b.wait(type('ack'));
  const got = await a.wait(type('message'));
  await b.wait(type('message'));
  check('sender gets an ack with seq 1', ack.seq === 1 && ack.duplicate === false, `seq ${ack.seq}`);
  check('other member receives the message', got.body === 'hello' && got.seq === 1 && got.message_id === ack.message_id);

  // --- idempotent resend
  b.send({ t: 'send', room_id: group.id, client_msg_id: 'm1', body: 'hello' });
  const again = await b.wait(type('ack'));
  check('resending the same client_msg_id creates nothing new', again.duplicate === true && again.seq === 1, `seq ${again.seq}`);

  // --- ordering
  await new Promise((r) => setTimeout(r, 1100));
  for (let i = 2; i <= 5; i++) b.send({ t: 'send', room_id: group.id, client_msg_id: `m${i}`, body: `msg ${i}` });
  const seqs = [];
  for (let i = 2; i <= 5; i++) seqs.push((await a.wait(type('message'))).seq);
  check('messages arrive in seq order', seqs.join() === '2,3,4,5', seqs.join());
  for (let i = 2; i <= 5; i++) { await b.wait(type('ack')); await b.wait(type('message')); }

  // --- rate limit
  await new Promise((r) => setTimeout(r, 1100));
  for (let i = 0; i < 20; i++) b.send({ t: 'send', room_id: group.id, client_msg_id: `burst${i}`, body: 'spam' });
  let limited = 0;
  let accepted = 0;
  for (let i = 0; i < 20; i++) {
    const f = await b.wait((x) => x.t === 'ack' || x.t === 'error');
    if (f.t === 'error' && f.code === 'RATE_LIMITED') limited++;
    if (f.t === 'ack') accepted++;
  }
  check('burst is rate limited', limited > 0 && accepted > 0, `${accepted} accepted, ${limited} rejected`);

  // --- catch-up after being away
  const late = await connect(alice.token);
  late.send({ t: 'join', room_id: group.id });
  const joined = await late.wait(type('joined'));
  late.send({ t: 'sync', room_id: group.id, after_seq: 0, limit: 200 });
  const sync = await late.wait(type('sync'));
  const synced = sync.messages.map((m) => m.seq);
  check('sync returns the full history in order',
    synced.length === joined.latest_seq && synced.every((s, i) => s === i + 1),
    `${synced.length} of ${joined.latest_seq}`);

  // --- live room: open to everyone
  a.send({ t: 'join', room_id: live.id });
  c.send({ t: 'join', room_id: live.id });
  await a.wait(type('joined'));
  const liveJoined = await c.wait(type('joined'));
  check('anyone can join a live room', liveJoined.room_type === 'live');
  c.send({ t: 'send', room_id: live.id, client_msg_id: 'l1', body: 'live hello' });
  const liveMsg = await a.wait((f) => f.t === 'message' && f.room_id === live.id);
  check('live room message is delivered', liveMsg.body === 'live hello');

  console.log(`\nroom ids: group=${group.id} live=${live.id}`);
  [a, b, c, late].forEach((x) => x.close());
} catch (err) {
  check('test run completed', false, err.message);
}

console.log(failures === 0 ? '\nAll checks passed.' : `\n${failures} check(s) failed.`);
process.exit(failures === 0 ? 0 : 1);
