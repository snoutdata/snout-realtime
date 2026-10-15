-- Realtime handlers: an event on a channel runs a SQL function of the customer's, as the caller.
-- Made in every project, and made again whenever this file changes (like 0002), so everything
-- here is idempotent.
--
--   snout_live.handlers          which function handles which event on which topics
--   snout_live.on() / off()      register and remove one (the project's owner only)
--   snout_live.event()           the event a handler is running for
--   snout_live.args()            every argument a Socket.IO emit carried (the payload is the first)
--   snout_live.socket_id()       the Socket.IO socket a handler is running for (null on a channel)
--   snout_live.emit()            send to a topic when, and only if, the handler's transaction commits
--   snout_live.join() / leave()  put the calling Socket.IO socket in a room, or take it out
--
-- emit, join and leave write nothing to a table: they append to a transaction-local setting the
-- server reads back after the handler returns, in the same transaction. So they need no policy on
-- snout_live.signals, they cost no WAL, and outside a handler they do nothing at all.
--
-- Runs as the project's admin role, the one the server connects as, which owns what it makes.

create table if not exists snout_live.handlers (
	event text not null,
	topic text not null default '*',
	handler regprocedure not null,
	created_at timestamptz not null default now(),
	primary key (event, topic)
);

-- No row-level security: nobody but the owner and the roles granted below can read it at all.
revoke all on snout_live.handlers from public;

-- Register `handler` for `event` on `topic`: an exact topic, '*', or a prefix ending in '*'
-- ('room:*'). For a Socket.IO namespace the topic is the namespace ('/', '/chat'). Replaces what
-- was registered for the same event and topic.
create or replace function snout_live.on(event text, handler regprocedure, topic text default '*')
	returns void
	language plpgsql
	security definer
	set search_path = pg_catalog, pg_temp
	as $body$
declare
	nargs int;
	arg oid;
	result oid;
begin
	if event is null or event = '' then
		raise exception 'snout_live.on: an event name is required';
	end if;
	if topic is null or topic = '' then
		raise exception 'snout_live.on: a topic is required (''*'' for every topic)';
	end if;
	if position('*' in topic) > 0 and position('*' in topic) <> length(topic) then
		raise exception 'snout_live.on: ''*'' may only end a topic, as in ''room:*''';
	end if;
	-- proargtypes is an oidvector, which is zero-based.
	select p.pronargs, p.proargtypes[0], p.prorettype into nargs, arg, result
	from pg_proc p where p.oid = handler;
	if nargs <> 1 or arg <> 'jsonb'::regtype then
		raise exception 'snout_live.on: % must take exactly one jsonb argument', handler;
	end if;
	if result not in ('jsonb'::regtype, 'void'::regtype) then
		raise exception 'snout_live.on: % must return jsonb or void', handler;
	end if;
	insert into snout_live.handlers (event, topic, handler) values (event, topic, handler)
	on conflict on constraint handlers_pkey do update set handler = excluded.handler, created_at = now();
end
$body$;

create or replace function snout_live.off(event text, topic text default '*')
	returns void
	language sql
	security definer
	set search_path = pg_catalog, pg_temp
	as $body$ delete from snout_live.handlers h where h.event = off.event and h.topic = off.topic $body$;

-- Only the project's owner registers handlers: a handled event is not relayed, so a handler is
-- a way to stop a channel's messages, and a signed-in user must never be able to install one.
-- The owner is `postgres` on a self-hosted stack, and on SnoutData Cloud the role the pod names in
-- `snout_utils.privileged_role` (`<ref>_owner`, which is not `postgres` and is not a superuser).
-- Missing it was found live: an owner on the cloud could not call snout_live.on().
revoke all on function snout_live.on(text, regprocedure, text) from public;
revoke all on function snout_live.off(text, text) from public;
do $body$
declare
	r text;
begin
	foreach r in array array['postgres', 'snout_server',
		coalesce(nullif(current_setting('snout_utils.privileged_role', true), ''), 'postgres')] loop
		if exists (select 1 from pg_roles where rolname = r) then
			execute format('grant execute on function snout_live.on(text, regprocedure, text) to %I', r);
			execute format('grant execute on function snout_live.off(text, text) to %I', r);
			execute format('grant select on snout_live.handlers to %I', r);
		end if;
	end loop;
end
$body$;

create or replace function snout_live.event() returns text
	language sql stable
	as $body$ select nullif(current_setting('realtime.event', true), '') $body$;

create or replace function snout_live.args() returns jsonb
	language sql stable
	as $body$ select coalesce(nullif(current_setting('realtime.args', true), '')::jsonb, '[]'::jsonb) $body$;

create or replace function snout_live.socket_id() returns text
	language sql stable
	as $body$ select nullif(current_setting('realtime.socket_id', true), '') $body$;

-- What a handler asked for, in order: [{"op":"emit"|"join"|"leave", ...}].
create or replace function snout_live.handler_out_push(op jsonb) returns void
	language sql
	as $body$
	select set_config(
		'realtime.out',
		(coalesce(nullif(current_setting('realtime.out', true), '')::jsonb, '[]'::jsonb) || jsonb_build_array(op))::text,
		true
	)
	$body$;

-- Send `payload` as `event` to everyone on `topic`, public and private subscribers alike: the
-- handler is the project's own server code and has already decided. `except_self` leaves out
-- the socket the handler is running for (Socket.IO's `socket.to(room).emit`).
create or replace function snout_live.emit(topic text, event text, payload jsonb default null, except_self boolean default false)
	returns void
	language sql
	as $body$
	select snout_live.handler_out_push(jsonb_build_object(
		'op', 'emit', 'topic', topic, 'event', event, 'payload', payload, 'except_self', except_self))
	$body$;

create or replace function snout_live.join(topic text) returns void
	language sql
	as $body$ select snout_live.handler_out_push(jsonb_build_object('op', 'join', 'topic', topic)) $body$;

create or replace function snout_live.leave(topic text) returns void
	language sql
	as $body$ select snout_live.handler_out_push(jsonb_build_object('op', 'leave', 'topic', topic)) $body$;

do $body$
declare
	r text;
begin
	foreach r in array array['snout_app', 'snout_member', 'snout_server', 'postgres',
		coalesce(nullif(current_setting('snout_utils.privileged_role', true), ''), 'postgres')] loop
		if exists (select 1 from pg_roles where rolname = r) then
			execute format('grant usage on schema snout_live to %I', r);
			-- What 0001 grants the API roles, for the owner too: 0001 runs once, before this role
			-- was thought of, and without it the owner's snout_live.signal() (the SQL editor, its own
			-- triggers and jobs) was refused and swallowed as a warning. Found live, 0.1.8.
			execute format('grant select, insert, update on snout_live.signals to %I', r);
			execute format('grant execute on function snout_live.event(), snout_live.args(), snout_live.socket_id(),
				snout_live.handler_out_push(jsonb), snout_live.emit(text, text, jsonb, boolean),
				snout_live.join(text), snout_live.leave(text) to %I', r);
		end if;
	end loop;
end
$body$;
