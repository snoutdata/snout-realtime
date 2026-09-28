-- The realtime schema in a project's database: the objects a customer's own SQL and policies
-- touch, and nothing else. Run once, by snout-realtime, when the project has no realtime.messages
-- yet (a project the pinned server already set up keeps its schema, which has the same tables).
--
-- What is here, and why it is the contract:
--   realtime.messages      what realtime.send() writes and private channels' policies are written on;
--                          one partition a day, made by the server, kept 72 hours for replay
--   realtime.subscription  one row per postgres_changes binding, as the subscriber; a customer
--                          may read it to see who is listening
--   realtime.topic()       the topic a policy on realtime.messages is being asked about
--   realtime.send()        broadcast from SQL (JSON or bytes)
--   realtime.broadcast_changes()  broadcast a row change from a trigger
--
-- Runs as the project's admin role, the one the server connects as, which owns what it makes.

create schema if not exists realtime;

create type realtime.equality_op as enum ('eq', 'neq', 'lt', 'lte', 'gt', 'gte', 'in');

create type realtime.action as enum ('INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', 'ERROR');

create type realtime.user_defined_filter as (column_name text, op realtime.equality_op, value text);

-- A role name as a regrole, for the subscription's generated column.
create function realtime.to_regrole(role_name text) returns regrole
	language sql immutable
	as $body$ select role_name::regrole $body$;

-- The topic being authorised, set by the server before it asks a policy.
create function realtime.topic() returns text
	language sql stable
	as $body$ select nullif(current_setting('realtime.topic', true), '') $body$;

create table realtime.messages (
	topic text not null,
	extension text not null,
	payload jsonb,
	event text,
	private boolean default false,
	updated_at timestamp without time zone not null default now(),
	inserted_at timestamp without time zone not null default now(),
	id uuid not null default gen_random_uuid(),
	binary_payload bytea,
	constraint messages_payload_exclusive check (payload is null or binary_payload is null),
	primary key (id, inserted_at)
) partition by range (inserted_at);

create index messages_inserted_at_topic_index on realtime.messages (inserted_at desc, topic)
	where extension = 'broadcast' and private is true;

alter table realtime.messages enable row level security;

create table realtime.subscription (
	id bigint generated always as identity primary key,
	subscription_id uuid not null,
	entity regclass not null,
	filters realtime.user_defined_filter[] not null default '{}',
	claims jsonb not null,
	claims_role regrole not null generated always as (realtime.to_regrole(claims ->> 'role')) stored,
	created_at timestamp without time zone not null default timezone('utc', now()),
	action_filter text default '*' check (action_filter in ('*', 'INSERT', 'UPDATE', 'DELETE')),
	selected_columns text[]
);

create index ix_realtime_subscription_entity on realtime.subscription (entity);

create unique index subscription_subscription_id_entity_filters_action_filter_selec
	on realtime.subscription (subscription_id, entity, filters, action_filter, coalesce(selected_columns, '{}'));

-- A binding is checked when it is written, against what the SUBSCRIBER may read: every filtered
-- column and every selected column must be one its role may select, a filter's value must be of
-- the column's type, and an `in` list holds at most 100 values. Filters and selected columns are
-- stored in a fixed order, so the unique index sees one binding however it was written.
--
-- Column privileges are read with has_column_privilege for the subscriber's role, never through
-- information_schema, which answers for whoever is running this (the server's own role) and so
-- refused real columns on a database where that role holds no grants.
create function realtime.subscription_check_filters() returns trigger
	language plpgsql
	as $body$
declare
	f realtime.user_defined_filter;
	col_type regtype;
	selected text;
	role_name text := new.claims ->> 'role';
begin
	for f in select * from unnest(new.filters) loop
		col_type := (
			select a.atttypid::regtype from pg_catalog.pg_attribute a
			where a.attrelid = new.entity and a.attname = f.column_name and a.attnum > 0 and not a.attisdropped
		);
		if col_type is null or not pg_catalog.has_column_privilege(role_name, new.entity, f.column_name, 'SELECT') then
			raise exception 'invalid column for filter %', f.column_name;
		end if;
		if f.op = 'in' then
			if coalesce(array_length(realtime.in_list(f.value, col_type), 1), 0) > 100 then
				raise exception 'too many values for `in` filter. Maximum 100';
			end if;
		else
			-- Refuses a value that is not of the column's type.
			execute format('select %L::%s', f.value, col_type);
		end if;
	end loop;

	if new.selected_columns is not null then
		foreach selected in array new.selected_columns loop
			if not exists (
				select 1 from pg_catalog.pg_attribute a
				where a.attrelid = new.entity and a.attname = selected and a.attnum > 0 and not a.attisdropped
			) or not pg_catalog.has_column_privilege(role_name, new.entity, selected, 'SELECT') then
				raise exception 'invalid column for select %', selected;
			end if;
		end loop;
	end if;

	new.filters := coalesce((select array_agg(x order by x.column_name, x.op, x.value) from unnest(new.filters) x), '{}');
	new.selected_columns := (select array_agg(c order by c) from unnest(new.selected_columns) c);
	return new;
end
$body$;

create trigger tr_check_filters before insert or update on realtime.subscription
	for each row execute function realtime.subscription_check_filters();

-- A filter's `in` list, cast to an array of the column's type (raises if a value is not one).
create function realtime.in_list(list text, type_ regtype) returns text[]
	language plpgsql immutable
	as $body$
declare
	out text[];
begin
	execute format('select %L::%s::text[]', list, type_::text || '[]') into out;
	return out;
end
$body$;

-- Broadcast from SQL, as JSON. The payload gets the message's id when it has none.
create function realtime.send(payload jsonb, event text, topic text, private boolean default true) returns void
	language plpgsql
	as $body$
declare
	message_id uuid := gen_random_uuid();
begin
	begin
		perform set_config('realtime.topic', topic, true);
		insert into realtime.messages (id, payload, event, topic, private, extension)
		values (
			message_id,
			case when payload ? 'id' then payload else jsonb_set(payload, '{id}', to_jsonb(message_id)) end,
			event, topic, private, 'broadcast'
		);
	exception when others then
		raise warning 'ErrorSendingBroadcastMessage: %', sqlerrm;
	end;
end
$body$;

-- Broadcast from SQL, as bytes.
create function realtime.send(payload bytea, event text, topic text, private boolean default true) returns void
	language plpgsql
	as $body$
begin
	begin
		perform set_config('realtime.topic', topic, true);
		insert into realtime.messages (id, binary_payload, event, topic, private, extension)
		values (gen_random_uuid(), payload, event, topic, private, 'broadcast');
	exception when others then
		raise warning 'ErrorSendingBroadcastMessage: %', sqlerrm;
	end;
end
$body$;

-- Broadcast a row change from a trigger: `realtime.broadcast_changes(topic, event, TG_OP,
-- TG_TABLE_NAME, TG_TABLE_SCHEMA, NEW, OLD)`, on a private topic.
create function realtime.broadcast_changes(
	topic_name text, event_name text, operation text, table_name text, table_schema text,
	new record, old record, level text default 'ROW'
) returns void
	language plpgsql
	as $body$
begin
	if level = 'STATEMENT' then
		raise exception 'function can only be triggered for each row, not for each statement';
	end if;
	if operation not in ('INSERT', 'UPDATE', 'DELETE') then
		raise exception 'Unexpected operation type: %', operation;
	end if;
	perform realtime.send(
		jsonb_build_object('old_record', old, 'record', new, 'operation', operation, 'table', table_name, 'schema', table_schema),
		event_name, topic_name
	);
exception when others then
	raise exception 'Failed to process the row: %', sqlerrm;
end
$body$;

-- Who may do what: the API roles read the schema, write broadcasts (their policies on
-- realtime.messages decide which), and read who is subscribed; postgres too, where it exists.
do $body$
declare
	grantee text := 'anon, authenticated, service_role'
		|| case when exists (select 1 from pg_roles where rolname = 'postgres') then ', postgres' else '' end;
begin
	execute 'grant usage on schema realtime to ' || grantee;
	execute 'grant select, insert, update on realtime.messages to ' || grantee;
	execute 'grant select on realtime.subscription to ' || grantee;
	execute 'grant usage on sequence realtime.subscription_id_seq to ' || grantee;
end
$body$;
