-- What snout-realtime runs in a project's database to decide who may see a change, and what
-- they see. In a schema of its own (snout_realtime), made in every project whichever server set
-- the realtime schema up, and replaced whenever this file changes (every function here is
-- `create or replace`, and the server records the file's hash).
--
-- The server streams the project's changes itself (pgoutput) and decides them in BATCHES: every
-- change to one table, of one kind, that has arrived and not been decided yet (one when changes
-- are sparse, as many as have queued when they are not). For each batch, on a connection of its
-- own:
--
--  1. decide_changes() answers, in one call, everything that does not need the subscriber's
--     identity: each subscriber's FILTERS against each change, typed as the column is typed (so a
--     char(21) key keeps its 21 characters), a subscriber's filters in a subtransaction of their
--     own so one that raises costs that subscriber alone; the subscribers who need no row check;
--     the rest grouped by role and claims, with ONE statement that checks rows by key; and the
--     payloads, one per change and per role and selected_columns: what the role may select, less
--     anything not asked for, a DELETE on a table with row-level security carrying only the key
--     (a deleted row cannot be checked).
--  2. The server runs the check AS each group (its role, its claims in request.jwt.claims), once
--     per distinct set of claims for the WHOLE batch, prepared once, all of a role's checks
--     pipelined in one round trip, each its own statement. Ten thousand anonymous subscribers
--     cost one check, a thousand users a thousand runs of one plan, a policy that raises costs
--     its group alone, and a busy table costs a check per batch rather than per change.

create schema if not exists snout_realtime;

create table if not exists snout_realtime.migrations (
	name text primary key,
	sha256 text not null,
	applied_at timestamptz not null default now()
);

-- One column of the table, with the change's value for it (new, and old where there is one).
do $body$
begin
	if to_regtype('snout_realtime.change_column') is null then
		create type snout_realtime.change_column as (
			name text, attnum int, type_text text, type_name text, is_pkey boolean,
			new_present boolean, new_value jsonb, new_raw text,
			old_present boolean, old_value jsonb, old_raw text
		);
	end if;
end
$body$;

-- What earlier versions of this file made, and the server no longer calls. decide_changes is
-- dropped too, since `create or replace` cannot change what a function returns.
drop function if exists snout_realtime.apply_change(regclass, text, timestamptz, jsonb, jsonb, integer);
drop function if exists snout_realtime.change_payloads(regclass, text, timestamptz, jsonb, jsonb, integer, uuid[]);
drop function if exists snout_realtime.change_subscribers(regclass, text, jsonb, jsonb);
drop function if exists snout_realtime.change_subscribers(regclass, text, timestamptz, jsonb, jsonb, integer);
drop function if exists snout_realtime.json_value(text, text);
drop function if exists snout_realtime.decide_changes(regclass, text, timestamptz[], jsonb[], jsonb[], integer);
drop type if exists snout_realtime.row_subscriber;

-- Does a filter hold for a value, both read as the column's type?
create or replace function snout_realtime.filter_holds(op realtime.equality_op, type_text text, value text, operand text) returns boolean
	language plpgsql immutable
	as $body$
declare
	res boolean;
	symbol text := case op
		when 'eq' then '=' when 'neq' then '!=' when 'lt' then '<' when 'lte' then '<='
		when 'gt' then '>' when 'gte' then '>=' when 'in' then '= any' end;
begin
	execute format('select %L::%s %s (%L::%s)', value, type_text, symbol, operand,
		case when op = 'in' then type_text || '[]' else type_text end) into res;
	return res;
end
$body$;

-- The table's columns, with a change's values. The values arrive as the text Postgres printed;
-- each is read as its column's type by making the row once (jsonb_populate_record), json and
-- jsonb read as the JSON they hold first, so the record carries the value a select would.
create or replace function snout_realtime.change_columns(entity_ regclass, new_values jsonb, old_values jsonb)
	returns snout_realtime.change_column[]
	language plpgsql stable
	as $body$
declare
	json_cols text[] := array(
		select a.attname from pg_attribute a
		where a.attrelid = entity_ and a.attnum > 0 and not a.attisdropped
		  and a.atttypid in ('json'::regtype, 'jsonb'::regtype));
	n jsonb := coalesce(new_values, '{}');
	o jsonb := coalesce(old_values, '{}');
	new_typed jsonb;
	old_typed jsonb;
begin
	if cardinality(json_cols) > 0 then
		n := n || coalesce((select jsonb_object_agg(k, (n ->> k)::jsonb) from unnest(json_cols) k where n ->> k is not null), '{}');
		o := o || coalesce((select jsonb_object_agg(k, (o ->> k)::jsonb) from unnest(json_cols) k where o ->> k is not null), '{}');
	end if;
	execute format('select to_jsonb(jsonb_populate_record(null::%s, $1)), to_jsonb(jsonb_populate_record(null::%s, $2))', entity_, entity_)
		into new_typed, old_typed using n, o;
	return (
		select coalesce(array_agg(row(a.attname, a.attnum, format_type(a.atttypid, a.atttypmod), t.typname,
			exists (select 1 from pg_constraint k where k.conrelid = entity_ and k.contype = 'p' and a.attnum = any (k.conkey)),
			-- A NULL stays SQL NULL, not the JSON null the made row holds, so an oversized record
			-- leaves it out as it leaves out every value it cannot carry.
			new_values ? a.attname, nullif(new_typed -> a.attname, 'null'::jsonb), new_values ->> a.attname,
			old_values ? a.attname, nullif(old_typed -> a.attname, 'null'::jsonb), old_values ->> a.attname
		)::snout_realtime.change_column order by a.attnum), '{}')
		from pg_attribute a join pg_type t on t.oid = a.atttypid
		where a.attrelid = entity_ and a.attnum > 0 and not a.attisdropped
	);
end
$body$;


-- Everything about a batch of changes to one table that does not need the subscriber's identity
-- (the n-th element of each array is the n-th change; `idx` counts from 1). One row per:
--   'send'     idx, subscription_ids, payload, errors: final (a change no subscriber of that role
--              may be shown);
--   'all'      subscription_ids see every change in `rows`, no row check needed;
--   'row'      subscription_ids have filters and passed them for change idx; if their role needs
--              no row check they see it, otherwise only if their group's check passes too;
--   'keys'     the row check (check_sql, run with the claims as $1 and `keys` as $2, answering
--              the positions in `keys` it finds) and `rows`, the change each key belongs to;
--   'check'    subscription_ids, all of role role_name with claims `claims`, see the changes
--              whose rows their check finds;
--   'group'    grp, subscription_ids: a group of subscribers who see a change the same way;
--   'payload'  idx, grp, payload, errors: what group grp sees of change idx.
create or replace function snout_realtime.decide_changes(
	entity_ regclass,
	action text,
	commit_times timestamptz[],
	new_values jsonb[],
	old_values jsonb[],
	max_record_bytes integer
) returns table (outcome text, idx integer, grp integer, role_name text, claims text, subscription_ids uuid[],
	payload jsonb, errors text[], check_sql text, keys jsonb, rows integer[])
	language plpgsql
	as $body$
declare
	n integer := coalesce(cardinality(commit_times), 0);
	rls boolean := (select c.relrowsecurity from pg_class c where c.oid = entity_);
	head jsonb := jsonb_build_object(
		'schema', (select ns.nspname from pg_class c join pg_namespace ns on ns.oid = c.relnamespace where c.oid = entity_),
		'table', (select c.relname from pg_class c where c.oid = entity_),
		'type', action);
	col_types jsonb := (
		select jsonb_object_agg(a.attname, format_type(a.atttypid, a.atttypmod)) from pg_attribute a
		where a.attrelid = entity_ and a.attnum > 0 and not a.attisdropped);
	pk text[] := array(
		select a.attname from pg_constraint k join pg_attribute a on a.attrelid = k.conrelid and a.attnum = any (k.conkey)
		where k.conrelid = entity_ and k.contype = 'p' order by a.attnum);
	-- The changes that can be seen: all of a DELETE's; of an INSERT or UPDATE, those whose new
	-- values carry the whole key (the rest cannot be checked). Their keys, in the same order.
	eligible integer[] := '{}';
	batch_keys jsonb := '[]';
	role_ regrole;
	rolname_ text;
	sub record;
	g record;
	i integer;
	seen integer[];
	-- Every subscriber who may see something, of every role: the payloads are made for them.
	candidates uuid[] := '{}';
	groups_ record;
	cols snout_realtime.change_column[];
	too_big boolean;
	stamp text;
	body jsonb;
	checked boolean := false;
begin
	if n = 0 then
		return;
	end if;
	for i in 1..n loop
		if action = 'DELETE' then
			eligible := eligible || i;
		elsif cardinality(pk) > 0 and (select bool_and(new_values[i] ->> c is not null) from unnest(pk) c) then
			eligible := eligible || i;
			batch_keys := batch_keys || jsonb_build_array((select jsonb_object_agg(c, new_values[i] ->> c) from unnest(pk) c));
		end if;
	end loop;

	for role_ in
		select s.claims_role from realtime.subscription s
		where s.entity = entity_ and (s.action_filter = '*' or s.action_filter = action)
		group by s.claims_role
		order by s.claims_role::text
	loop
		rolname_ := (select r.rolname::text from pg_roles r where r.oid = role_);
		-- A change whose key the role may not read may not be seen; one without a key cannot be
		-- checked.
		if action <> 'DELETE' and exists (select 1 from unnest(pk) c where not pg_catalog.has_column_privilege(role_, entity_, c, 'SELECT')) then
			return query select 'send', gs.i, null::integer, null::text, null::text,
					(select array_agg(s.subscription_id) from realtime.subscription s
						where s.entity = entity_ and s.claims_role = role_ and (s.action_filter = '*' or s.action_filter = action)),
					head, array['Error 401: Unauthorized'], null::text, null::jsonb, null::integer[]
				from generate_series(1, n) gs(i);
			continue;
		end if;
		if action <> 'DELETE' and cardinality(eligible) < n then
			return query select 'send', gs.i, null::integer, null::text, null::text,
					(select array_agg(s.subscription_id) from realtime.subscription s
						where s.entity = entity_ and s.claims_role = role_ and (s.action_filter = '*' or s.action_filter = action)),
					head, array['Error 400: Bad Request, no primary key'], null::text, null::jsonb, null::integer[]
				from generate_series(1, n) gs(i)
				where gs.i <> all (eligible);
		end if;
		if cardinality(eligible) = 0 then
			continue;
		end if;

		-- Subscribers without filters see every eligible change, as a set.
		return query select 'all', null::integer, null::integer, rolname_, null::text, array_agg(s.subscription_id),
				null::jsonb, null::text[], null::text, null::jsonb, eligible
			from realtime.subscription s
			where s.entity = entity_ and s.claims_role = role_ and (s.action_filter = '*' or s.action_filter = action)
			  and coalesce(cardinality(s.filters), 0) = 0
			  and (not rls or action = 'DELETE')
			having count(*) > 0;

		-- Filters: each subscriber that has some, against every change, in a subtransaction of
		-- its own.
		for sub in
			select s.subscription_id, s.filters from realtime.subscription s
			where s.entity = entity_ and s.claims_role = role_ and (s.action_filter = '*' or s.action_filter = action)
			  and coalesce(cardinality(s.filters), 0) > 0
		loop
			begin
				seen := array(
					select e from unnest(eligible) e
					where coalesce((
						select bool_and(coalesce(snout_realtime.filter_holds(f.op, col_types ->> f.column_name, new_values[e] ->> f.column_name, f.value), false))
						from unnest(sub.filters) f where new_values[e] ? f.column_name
					), false) or (action = 'DELETE' and coalesce((
						select bool_and(coalesce(snout_realtime.filter_holds(f.op, col_types ->> f.column_name, old_values[e] ->> f.column_name, f.value), false))
						from unnest(sub.filters) f where old_values[e] ? f.column_name
					), false)));
			exception when others then
				raise warning 'snout_realtime: a filter of subscription % could not be evaluated: %', sub.subscription_id, sqlerrm;
				seen := '{}';
			end;
			if cardinality(seen) > 0 then
				candidates := candidates || sub.subscription_id;
				return query select 'row', e, null::integer, rolname_, null::text, array[sub.subscription_id],
						null::jsonb, null::text[], null::text, null::jsonb, null::integer[]
					from unnest(seen) e;
			end if;
		end loop;
		candidates := candidates || array(
			select s.subscription_id from realtime.subscription s
			where s.entity = entity_ and s.claims_role = role_ and (s.action_filter = '*' or s.action_filter = action)
			  and coalesce(cardinality(s.filters), 0) = 0);

		-- Row-level security: one check per distinct set of claims, for the whole batch.
		if rls and action <> 'DELETE' then
			checked := true;
			return query select 'check', null::integer, null::integer, rolname_, s.claims::text, array_agg(s.subscription_id),
					null::jsonb, null::text[], null::text, null::jsonb, null::integer[]
				from realtime.subscription s
				where s.entity = entity_ and s.claims_role = role_ and (s.action_filter = '*' or s.action_filter = action)
				  and s.subscription_id in (select unnest(candidates))
				group by s.claims;
		end if;
	end loop;

	-- A batch of one, the usual case while changes are sparse, is checked by the plainer
	-- statement: one index probe, no unnesting. Either answers positions among the keys.
	if checked and cardinality(eligible) = 1 then
		return query select 'keys', null::integer, null::integer, null::text, null::text, null::uuid[], null::jsonb, null::text[],
			format('select case when exists (select 1 from %s where %s) then ''{1}''::integer[] else ''{}''::integer[] end',
				entity_, (select string_agg(format('%I = ($2::jsonb -> 0 ->> %L)::%s', c, c, col_types ->> c), ' and ') from unnest(pk) c)),
			batch_keys, eligible;
	elsif checked then
		return query select 'keys', null::integer, null::integer, null::text, null::text, null::uuid[], null::jsonb, null::text[],
			format('select coalesce(array_agg(k.i::integer), ''{}'') from jsonb_array_elements($2) with ordinality k(v, i) where exists (select 1 from %s where %s)',
				entity_, (select string_agg(format('%I = (k.v ->> %L)::%s', c, c, col_types ->> c), ' and ') from unnest(pk) c)),
			batch_keys, eligible;
	end if;

	-- The groups, once for the batch, and what each sees of each change.
	for groups_ in
		select row_number() over (order by s.claims_role::text, s.selected_columns nulls first)::integer as no,
			s.claims_role as role_, s.selected_columns as group_columns, array_agg(s.subscription_id) as ids
		from realtime.subscription s
		where s.subscription_id in (select unnest(candidates))
		group by s.claims_role, s.selected_columns
	loop
		return query select 'group', null::integer, groups_.no, null::text, null::text, groups_.ids,
			null::jsonb, null::text[], null::text, null::jsonb, null::integer[];
	end loop;
	if cardinality(candidates) = 0 then
		return;
	end if;
	foreach i in array eligible loop
		cols := snout_realtime.change_columns(entity_, new_values[i], old_values[i]);
		too_big := octet_length(coalesce(new_values[i]::text, '')) + octet_length(coalesce(old_values[i]::text, '')) > max_record_bytes;
		stamp := to_char(commit_times[i] at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"');
		for g in
			select row_number() over (order by s.claims_role::text, s.selected_columns nulls first)::integer as no,
				s.claims_role as role_, s.selected_columns as group_columns
			from realtime.subscription s
			where s.subscription_id in (select unnest(candidates))
			group by s.claims_role, s.selected_columns
		loop
			body := head || jsonb_build_object(
				'commit_timestamp', stamp,
				'columns', (
					select jsonb_agg(jsonb_build_object('name', c.name, 'type', c.type_name) order by c.attnum)
					from unnest(cols) c
					where pg_catalog.has_column_privilege(g.role_, entity_, c.name, 'SELECT')
					  and (g.group_columns is null or c.name = any (g.group_columns) or c.is_pkey)
				)
			);
			if action in ('INSERT', 'UPDATE') then
				body := body || jsonb_build_object('record', (
					select jsonb_object_agg(c.name, case when c.new_present then c.new_value else c.old_value end)
					from unnest(cols) c
					where (c.new_present or c.old_present)
					  and pg_catalog.has_column_privilege(g.role_, entity_, c.name, 'SELECT')
					  and (g.group_columns is null or c.name = any (g.group_columns) or c.is_pkey)
					  and (not too_big or octet_length((case when c.new_present then c.new_value else c.old_value end)::text) <= 64)
				));
			end if;
			if action in ('UPDATE', 'DELETE') then
				body := body || jsonb_build_object('old_record', (
					select jsonb_object_agg(c.name, c.old_value)
					from unnest(cols) c
					where c.old_present
					  and pg_catalog.has_column_privilege(g.role_, entity_, c.name, 'SELECT')
					  and (g.group_columns is null or c.name = any (g.group_columns) or c.is_pkey)
					  and (not too_big or octet_length(c.old_value::text) <= 64)
					  and (action <> 'DELETE' or not rls or c.is_pkey)
				));
			end if;
			return query select 'payload', i, g.no, null::text, null::text, null::uuid[], body,
				case when too_big then array['Error 413: Payload Too Large'] else '{}'::text[] end,
				null::text, null::jsonb, null::integer[];
		end loop;
	end loop;
end
$body$;
