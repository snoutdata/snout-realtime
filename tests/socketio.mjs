// Driven by tests/socketio.sh: realtime handlers and the Socket.IO door against a real server and
// a real Postgres, with the real socket.io-client. Exits non-zero on the first failure.
import { createHmac, randomUUID } from 'node:crypto';
import { io } from 'socket.io-client';
import WebSocket from 'ws';
import pg from 'pg';

const RT = process.env.RT;
const SECRET = 'project-jwt-secret-for-the-socketio-test-0123456789';
const HOST = { 'x-forwarded-host': 't1.example.test' };
let failures = 0;

function b64(v) {
	return Buffer.from(typeof v === 'string' ? v : JSON.stringify(v)).toString('base64url');
}
function jwt(claims, secret = SECRET) {
	const head = b64({ alg: 'HS256', typ: 'JWT' });
	const body = b64({ exp: Math.floor(Date.now() / 1000) + 3600, ...claims });
	const sig = createHmac('sha256', secret).update(`${head}.${body}`).digest('base64url');
	return `${head}.${body}.${sig}`;
}
function check(name, ok, detail) {
	if (ok) {
		console.log(`PASS ${name}`);
	} else {
		failures++;
		console.log(`FAIL ${name}${detail === undefined ? '' : `: ${JSON.stringify(detail)}`}`);
	}
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
function within(ms, promise, what) {
	return Promise.race([promise, sleep(ms).then(() => { throw new Error(`timed out: ${what}`); })]);
}
function next(socket, event) {
	return new Promise((resolve) => socket.once(event, (...args) => resolve(args)));
}

const admin = jwt({ role: 'admin' }, process.env.API_JWT_SECRET);
const anonKey = jwt({ role: 'anon' });
const alice = randomUUID();
const bob = randomUUID();
const aliceToken = jwt({ role: 'authenticated', sub: alice });
const bobToken = jwt({ role: 'authenticated', sub: bob });

// --- the project ---------------------------------------------------------------------------
const put = await fetch(`${RT}/api/tenants`, {
	method: 'POST',
	headers: { authorization: `Bearer ${admin}`, 'content-type': 'application/json' },
	body: JSON.stringify({
		tenant: {
			external_id: 't1', name: 't1', jwt_secret: SECRET,
			max_concurrent_users: 100, max_channels_per_client: 100, max_events_per_second: 1000,
			extensions: [{ type: 'postgres_cdc_rls', settings: {
				db_host: new URL(process.env.PG).hostname, db_port: '5432', db_name: 'postgres',
				db_user: 'postgres', db_password: 'test', publication: 'realtime_test', slot_name: 'x'
			} }]
		}
	})
});
check('tenant registered', put.ok, put.status);
const health = await fetch(`${RT}/api/tenants/t1/health`, { headers: { authorization: `Bearer ${admin}` } });
check('project prepared', health.ok, await health.text());

const db = new pg.Client({ connectionString: process.env.PG });
await db.connect();
await db.query(`
	create schema auth;
	create function auth.uid() returns uuid language sql stable as
		$$ select nullif(current_setting('request.jwt.claim.sub', true), '')::uuid $$;
	grant usage on schema auth to anon, authenticated;
	create table messages (id bigint generated always as identity primary key, room text, body text, author uuid);
	alter table messages enable row level security;
	create policy "write your own" on messages for insert to authenticated with check (author = auth.uid());
	create policy "read" on messages for select using (true);
	grant select, insert on messages to anon, authenticated;

	create function on_connect(p jsonb) returns void language plpgsql as $$
	begin
		if p->>'room' is null then raise exception 'pick a room'; end if;
		perform realtime.join('room:' || (p->>'room'));
	end $$;
	create function send_message(p jsonb) returns jsonb language plpgsql as $$
	declare new_id bigint;
	begin
		insert into messages (room, body, author) values (p->>'room', p->>'body', auth.uid()) returning id into new_id;
		perform realtime.emit('room:' || (p->>'room'), 'message',
			jsonb_build_object('id', new_id, 'body', p->>'body', 'author', auth.uid()), true);
		return jsonb_build_object('id', new_id);
	end $$;
	create function whoami(p jsonb) returns jsonb language sql as $$
		select jsonb_build_object('uid', auth.uid(), 'role', current_user, 'event', realtime.event(),
			'args', realtime.args(), 'socket', realtime.socket_id(), 'payload', p)
	$$;
	create function slow(p jsonb) returns void language sql as $$ select pg_sleep(10) $$;
	create function on_leave(p jsonb) returns void language sql as $$
		select realtime.emit('room:1', 'left', jsonb_build_object('who', auth.uid()))
	$$;
	create function nothing(p jsonb) returns void language sql as $$ select $$;

	select realtime.on('connection', 'on_connect(jsonb)', '/');
	select realtime.on('disconnect', 'on_leave(jsonb)', '/');
	select realtime.on('send', 'send_message(jsonb)');
	select realtime.on('whoami', 'whoami(jsonb)');
	select realtime.on('slow', 'slow(jsonb)');
`);
check('handlers registered', true);

let refused;
try {
	await db.query(`set role authenticated; select realtime.on('send', 'nothing(jsonb)')`);
} catch (e) {
	refused = e.message;
} finally {
	await db.query('reset role');
}
check('a signed-in user cannot register a handler', /permission denied/.test(refused ?? ''), refused);

// The owner on SnoutData Cloud is not `postgres` and not a superuser: the role the pod names in
// snout_utils.privileged_role. It must be able to register handlers (found live, 0.1.7).
const owner = new pg.Client({ connectionString: process.env.PG.replace('postgres:test@', 'project_owner:owner@') });
await owner.connect();
let ownerError;
try {
	await owner.query(`create function public.owner_echo(p jsonb) returns jsonb language sql as $$ select p $$`);
	await owner.query(`select realtime.on('owner_echo', 'public.owner_echo(jsonb)')`);
	await owner.query(`select realtime.off('owner_echo')`);
	const sent = await owner.query(`insert into realtime.messages (topic, extension, event, payload, private)
		values ('owner:probe', 'broadcast', 'probe', '{}', true) returning id`);
	if (sent.rowCount !== 1) {
		throw new Error('the owner could not write realtime.messages');
	}
} catch (e) {
	ownerError = e.message;
}
await owner.end();
check('the privileged (non-superuser) owner role can register a handler and broadcast', ownerError === undefined, ownerError);

let badSignature;
try {
	await db.query(`select realtime.on('x', 'auth.uid()')`);
} catch (e) {
	badSignature = e.message;
}
check('a handler must take one jsonb', /exactly one jsonb argument/.test(badSignature ?? ''), badSignature);

// --- Socket.IO -----------------------------------------------------------------------------
function client(token, extra = {}) {
	return io(RT, {
		path: '/socket/socket.io/',
		query: { apikey: anonKey },
		extraHeaders: HOST,
		auth: { token, room: 1 },
		reconnection: false,
		...extra
	});
}

const a = client(aliceToken);
await within(5000, next(a, 'connect'), 'alice connects over polling');
check('connects over polling first', a.io.engine.transport.name === 'polling' || a.io.engine.transport.name === 'websocket');
if (a.io.engine.transport.name === 'polling') {
	await within(5000, next(a.io.engine, 'upgrade'), 'upgrade to websocket');
}
check('upgrades to websocket', a.io.engine.transport.name === 'websocket', a.io.engine.transport.name);

const b = client(bobToken, { transports: ['websocket'] });
await within(5000, next(b, 'connect'), 'bob connects over websocket');
check('connects over websocket only', b.connected && b.io.engine.transport.name === 'websocket');

const refusedClient = client(aliceToken, { auth: { token: aliceToken } });
const [err] = await within(5000, next(refusedClient, 'connect_error'), 'refused connect');
check('a connection handler that raises refuses the connection', err.message === 'pick a room', err.message);
refusedClient.close();

const badToken = client(jwt({ role: 'authenticated', sub: alice }, 'the-wrong-secret-0123456789abcdef'));
const [badErr] = await within(5000, next(badToken, 'connect_error'), 'bad token');
check('a token the project did not sign is refused', typeof badErr.message === 'string', badErr.message);
badToken.close();

const who = await within(5000, a.emitWithAck('whoami', { x: 1 }, 2), 'whoami ack');
check('a handler runs as the caller', who.uid === alice && who.role === 'authenticated', who);
check('realtime.event(), args() and socket_id()', who.event === 'whoami' && JSON.stringify(who.args) === '[{"x":1},2]' && who.socket === a.id, who);
check('the payload is the first argument', JSON.stringify(who.payload) === '{"x":1}', who.payload);

let aliceHeardHerself = false;
a.on('message', () => { aliceHeardHerself = true; });
const heard = next(b, 'message');
const sent = await within(5000, a.emitWithAck('send', { room: '1', body: 'hi' }), 'send ack');
check('the ack is the handler result', Number.isInteger(sent.id), sent);
const [msg] = await within(5000, heard, 'bob hears it');
check('an emit reaches the room', msg.body === 'hi' && msg.author === alice && msg.id === sent.id, msg);
await sleep(300);
check('except_self leaves the sender out', !aliceHeardHerself);

const stored = await db.query('select author from messages where id = $1', [sent.id]);
check('the write committed as the caller', stored.rows[0]?.author === alice, stored.rows);

const anonClient = client(anonKey);
await within(5000, next(anonClient, 'connect'), 'anon connects');
const denied = await within(5000, anonClient.emitWithAck('send', { room: '1', body: 'x' }), 'anon send ack');
check('row-level security refuses the anon write', denied?.error?.code === '42501' || denied?.error?.code === 'unauthorized', denied);
const before = await db.query('select count(*)::int as n from messages');

const none = await within(5000, a.emitWithAck('nope', {}), 'no handler ack');
check('an event with no handler is told so', none?.error?.code === 'no_handler', none);

const news = Promise.all([next(a, 'news'), next(b, 'news')]);
await db.query(`select realtime.send('{"n":1}'::jsonb, 'news', 'room:1', true)`);
const [[na], [nb]] = await within(8000, news, 'realtime.send reaches the room');
check('realtime.send() from SQL reaches Socket.IO rooms', na.n === 1 && nb.n === 1, { na, nb });

const blob = next(a, 'blob');
await db.query(`select realtime.send('\\x0102ff'::bytea, 'blob', 'room:1', true)`);
const [bytes] = await within(8000, blob, 'binary reaches the room');
check('realtime.send(bytea) arrives as binary', Buffer.isBuffer(bytes) && bytes.equals(Buffer.from([1, 2, 255])), bytes);

const binaryAck = await within(5000, a.emitWithAck('send', Buffer.from([1])), 'binary from a client');
check('binary from a client is refused with its code', binaryAck?.error?.code === 'binary_not_supported', binaryAck);

const t0 = Date.now();
const timedOut = await within(10000, a.emitWithAck('slow', {}), 'slow ack');
check('a handler is stopped at 5 s', timedOut?.error?.code === 'timeout' && Date.now() - t0 < 8000, timedOut);

// --- the Phoenix door ------------------------------------------------------------------------
const ws = new WebSocket(`${RT.replace('http', 'ws')}/socket/websocket?apikey=${anonKey}&vsn=1.0.0`, { headers: HOST });
await within(5000, new Promise((r) => ws.once('open', r)), 'phoenix socket');
const frames = [];
const waiters = [];
ws.on('message', (data) => {
	const f = JSON.parse(data.toString());
	frames.push(f);
	for (const w of [...waiters]) {
		if (w.match(f)) {
			waiters.splice(waiters.indexOf(w), 1);
			w.resolve(f);
		}
	}
});
function frame(match) {
	const found = frames.find(match);
	if (found) {
		frames.splice(frames.indexOf(found), 1);
		return Promise.resolve(found);
	}
	return new Promise((resolve) => waiters.push({ match, resolve }));
}
ws.send(JSON.stringify({ topic: 'realtime:room:1', event: 'phx_join', ref: '1', join_ref: '1',
	payload: { config: { broadcast: { self: false } }, access_token: bobToken } }));
const joined = await within(5000, frame((f) => f.ref === '1'), 'phoenix join');
check('a Phoenix channel joins', joined.payload.status === 'ok', joined.payload);

ws.send(JSON.stringify({ topic: 'realtime:room:1', event: 'broadcast', ref: '2', join_ref: '1',
	payload: { type: 'broadcast', event: 'whoami', payload: { from: 'phoenix' } } }));
const reply = await within(5000, frame((f) => f.ref === '2'), 'phoenix handler reply');
check('a channel broadcast runs its handler and replies with the result',
	reply.payload.status === 'ok' && reply.payload.response.result.uid === bob && reply.payload.response.result.payload.from === 'phoenix', reply.payload);

ws.send(JSON.stringify({ topic: 'realtime:room:1', event: 'broadcast', ref: '4', join_ref: '1',
	payload: { type: 'broadcast', event: 'nope', payload: {}, call: true } }));
const noHandler = await within(5000, frame((f) => f.ref === '4'), 'phoenix call with no handler');
check('a call() with no handler is answered no_handler', noHandler.payload.status === 'error' && noHandler.payload.response.error.code === 'no_handler', noHandler.payload);

const ioHeard = next(a, 'message');
const phoenixHeard = frame((f) => f.event === 'broadcast' && f.payload?.event === 'message');
await within(5000, b.emitWithAck('send', { room: '1', body: 'to everyone' }), 'bob send');
const [[fromBob], fromPhoenix] = await within(5000, Promise.all([ioHeard, phoenixHeard]), 'both doors hear it');
check('a Socket.IO emit reaches a Phoenix channel on the same topic',
	fromBob.body === 'to everyone' && fromPhoenix.payload.payload.body === 'to everyone', fromPhoenix.payload);

const unhandled = next(a, 'cursor');
ws.send(JSON.stringify({ topic: 'realtime:room:1', event: 'broadcast', ref: '3', join_ref: '1',
	payload: { type: 'broadcast', event: 'cursor', payload: { x: 1 } } }));
const [cursor] = await within(5000, unhandled, 'an unhandled channel broadcast');
check('an unhandled channel broadcast is relayed as before, Socket.IO rooms included', cursor.x === 1, cursor);

// --- disconnect ------------------------------------------------------------------------------
const left = next(b, 'left');
a.close();
const [l] = await within(5000, left, 'disconnect handler');
check('a disconnect handler runs as the caller who left', l.who === alice, l);

b.close();
anonClient.close();
ws.close();
const after = await db.query('select count(*)::int as n from messages');
check('nothing written by a refused call', after.rows[0].n === before.rows[0].n + 1, { before: before.rows[0].n, after: after.rows[0].n });
await db.end();
console.log(failures === 0 ? 'ALL PASSED' : `${failures} FAILED`);
process.exit(failures === 0 ? 0 : 1);
