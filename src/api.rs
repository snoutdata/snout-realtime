//! The HTTP side of the one port: the socket's upgrade, the tenant API the host agent calls,
//! broadcast over HTTP, `/metrics`, and the health paths.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use axum::Router;
use axum::body::Bytes;
use axum::extract::ws::WebSocketUpgrade;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde_json::{Map, Value, json};

use crate::App;
use crate::db::{self, AuthContext, Policies};
use crate::hub::{self, TopicKey};
use crate::jwt;
use crate::protocol::{Encoding, Vsn};
use crate::socket::{self, Accepted, now_secs};
use crate::tenants::{self, TenantBody};

pub fn router(app: Arc<App>) -> Router {
	Router::new()
		.route("/", get(|| async { "ok" }))
		.route(
			"/api/ping",
			get(|| async { axum::Json(json!({ "message": "Success" })) }),
		)
		.route("/socket/websocket", get(websocket))
		.route("/api/tenants", get(list_tenants).post(put_tenant))
		.route("/api/tenants/{id}", get(show_tenant).delete(delete_tenant))
		.route("/api/tenants/{id}/health", get(tenant_health))
		.route("/api/broadcast", post(broadcast))
		.route("/api/broadcast/{topic}/events/{event}", post(broadcast_one))
		.route("/metrics", get(metrics))
		.layer(axum::middleware::map_response(cache_control))
		.with_state(app)
}

/// Every HTTP answer says it may not be cached, as the pinned server's framework does.
async fn cache_control(mut response: Response) -> Response {
	response
		.headers_mut()
		.entry("cache-control")
		.or_insert(axum::http::HeaderValue::from_static(
			"max-age=0, private, must-revalidate",
		));
	response
}

fn json_response(status: StatusCode, body: Value) -> Response {
	(status, axum::Json(body)).into_response()
}

/// The project a request is for: the first label of its host, as the front door sets it.
fn external_id(headers: &HeaderMap) -> Option<String> {
	let host = headers
		.get("x-forwarded-host")
		.or_else(|| headers.get("host"))?
		.to_str()
		.ok()?;
	let first = host.split(':').next()?.split('.').next()?;
	if first.is_empty() {
		None
	} else {
		Some(first.to_string())
	}
}

/// The admin API's and the metrics page's bearer token, verified against their own secret.
fn admin_ok(headers: &HeaderMap, secret: &str) -> bool {
	let Some(value) = headers.get("authorization").and_then(|v| v.to_str().ok()) else {
		return false;
	};
	let Some(token) = value.strip_prefix("Bearer ") else {
		return false;
	};
	jwt::verify(token, secret, now_secs()).is_ok()
}

// --- the socket ---------------------------------------------------------------------------

async fn websocket(
	State(app): State<Arc<App>>,
	headers: HeaderMap,
	Query(params): Query<HashMap<String, String>>,
	upgrade: WebSocketUpgrade,
) -> Response {
	let forbidden = || StatusCode::FORBIDDEN.into_response();
	let Some(id) = external_id(&headers) else {
		return forbidden();
	};
	let tenant = match app.registry.get(&id).await {
		Ok(Some(t)) => t,
		_ => return forbidden(),
	};
	let token = headers
		.get("x-api-key")
		.and_then(|v| v.to_str().ok())
		.map(str::to_string)
		.or_else(|| params.get("apikey").cloned());
	let Some(token) = token else {
		return forbidden();
	};
	if jwt::authorize(&token, &tenant.jwt_secret, now_secs()).is_err() {
		return forbidden();
	}
	let rt = app.hub.tenant(tenant);
	// One client address may not hold every socket a project is allowed.
	let address = headers
		.get("x-forwarded-for")
		.and_then(|v| v.to_str().ok())
		.and_then(|v| v.split(',').next())
		.map(|s| s.trim().to_string());
	if let (Some(addr), cap) = (address.as_deref(), app.config.max_sockets_per_address)
		&& cap > 0
		&& rt.sockets_from(addr).1 >= cap
	{
		return (
			StatusCode::TOO_MANY_REQUESTS,
			"Too many connections from this address",
		)
			.into_response();
	}
	let mut x_headers = Map::new();
	for (name, value) in headers.iter() {
		if name.as_str().starts_with("x-")
			&& name.as_str() != "x-api-key"
			&& let Ok(v) = value.to_str()
		{
			x_headers.insert(name.as_str().to_string(), Value::String(v.to_string()));
		}
	}
	let accepted = Accepted {
		rt,
		vsn: Vsn::from_param(params.get("vsn").map(String::as_str)),
		token,
		headers: x_headers,
		address,
	};
	// 4 KiB buffers, not the default 128 KiB each way, which let a thousand sockets hold up to
	// 256 MiB between them. A frame is still read or written whole, whatever its size, and every
	// frame is flushed as it is sent.
	upgrade
		.read_buffer_size(4096)
		.write_buffer_size(4096)
		.on_upgrade(move |ws| socket::serve(app, ws, accepted))
}

// --- the tenant API -----------------------------------------------------------------------

async fn put_tenant(State(app): State<Arc<App>>, headers: HeaderMap, body: Bytes) -> Response {
	if !admin_ok(&headers, &app.config.api_jwt_secret) {
		return StatusCode::FORBIDDEN.into_response();
	}
	let parsed: Result<Value, _> = serde_json::from_slice(&body);
	let body: TenantBody = match parsed
		.ok()
		.and_then(|v| v.get("tenant").cloned().or(Some(v)))
		.map(serde_json::from_value)
	{
		Some(Ok(b)) => b,
		_ => {
			return json_response(
				StatusCode::UNPROCESSABLE_ENTITY,
				json!({ "errors": { "tenant": ["is invalid"] } }),
			);
		}
	};
	let tenant = match tenants::from_body(&body) {
		Ok(t) => t,
		Err(e) => {
			return json_response(
				StatusCode::UNPROCESSABLE_ENTITY,
				json!({ "errors": { "tenant": [e.to_string()] } }),
			);
		}
	};
	match app.registry.put(tenant).await {
		Ok(t) => {
			if let Some(rt) = app.hub.get(&t.external_id) {
				rt.set_tenant(t.clone());
			}
			json_response(StatusCode::CREATED, json!({ "data": t.public_json() }))
		}
		Err(e) => json_response(
			StatusCode::INTERNAL_SERVER_ERROR,
			json!({ "message": e.to_string() }),
		),
	}
}

async fn list_tenants(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
	if !admin_ok(&headers, &app.config.api_jwt_secret) {
		return StatusCode::FORBIDDEN.into_response();
	}
	match app.registry.list().await {
		Ok(list) => json_response(StatusCode::OK, json!({ "data": list })),
		Err(e) => json_response(
			StatusCode::INTERNAL_SERVER_ERROR,
			json!({ "message": e.to_string() }),
		),
	}
}

async fn show_tenant(
	State(app): State<Arc<App>>,
	headers: HeaderMap,
	Path(id): Path<String>,
) -> Response {
	if !admin_ok(&headers, &app.config.api_jwt_secret) {
		return StatusCode::FORBIDDEN.into_response();
	}
	match app.registry.get(&id).await {
		Ok(Some(t)) => json_response(StatusCode::OK, json!({ "data": t.public_json() })),
		Ok(None) => json_response(StatusCode::NOT_FOUND, json!({ "error": "not found" })),
		Err(e) => json_response(
			StatusCode::INTERNAL_SERVER_ERROR,
			json!({ "message": e.to_string() }),
		),
	}
}

async fn delete_tenant(
	State(app): State<Arc<App>>,
	headers: HeaderMap,
	Path(id): Path<String>,
) -> Response {
	if !admin_ok(&headers, &app.config.api_jwt_secret) {
		return StatusCode::FORBIDDEN.into_response();
	}
	if let Some(rt) = app.hub.remove(&id) {
		rt.disconnect_all();
		let mut streams = rt.streams.lock().await;
		if let Some(task) = streams.messages.take() {
			task.abort();
		}
		streams.changes = None;
	}
	app.dbs.forget(&id).await;
	match app.registry.delete(&id).await {
		Ok(_) => StatusCode::NO_CONTENT.into_response(),
		Err(e) => json_response(
			StatusCode::INTERNAL_SERVER_ERROR,
			json!({ "message": e.to_string() }),
		),
	}
}

async fn tenant_health(
	State(app): State<Arc<App>>,
	headers: HeaderMap,
	Path(id): Path<String>,
) -> Response {
	if !admin_ok(&headers, &app.config.api_jwt_secret) {
		return StatusCode::FORBIDDEN.into_response();
	}
	let tenant = match app.registry.get(&id).await {
		Ok(Some(t)) => t,
		Ok(None) => return json_response(StatusCode::NOT_FOUND, json!({ "error": "not found" })),
		Err(e) => {
			return json_response(
				StatusCode::INTERNAL_SERVER_ERROR,
				json!({ "message": e.to_string() }),
			);
		}
	};
	let rt = app.hub.tenant(tenant.clone());
	let connected = match &tenant.database {
		None => false,
		Some(database) => match app.dbs.pool(&tenant.external_id, database).await {
			Ok(pool) => {
				let mut prepared = rt.prepared.lock().await;
				if !*prepared {
					*prepared = db::prepare(&pool).await.is_ok();
				}
				*prepared
			}
			Err(_) => false,
		},
	};
	let replication = rt
		.streams
		.lock()
		.await
		.messages
		.as_ref()
		.is_some_and(|t| !t.is_finished());
	json_response(
		StatusCode::OK,
		json!({ "data": { "healthy": connected, "db_connected": connected, "replication_connected": replication, "connected_cluster": rt.connections() } }),
	)
}

// --- broadcast over HTTP ------------------------------------------------------------------

/// The project and the caller of an HTTP broadcast: the tenant from the host, the token from
/// `Authorization: Bearer` or `apikey`, verified with the project's secret.
async fn http_caller(
	app: &App,
	headers: &HeaderMap,
) -> Result<(Arc<tenants::Tenant>, Map<String, Value>), Box<Response>> {
	let not_found = || {
		Box::new(json_response(
			StatusCode::UNAUTHORIZED,
			json!({ "message": "Tenant not found in database" }),
		))
	};
	let unauthorized = || {
		Box::new(json_response(
			StatusCode::UNAUTHORIZED,
			json!({ "message": "Unauthorized" }),
		))
	};
	let id = external_id(headers).ok_or_else(not_found)?;
	let tenant = match app.registry.get(&id).await {
		Ok(Some(t)) => t,
		_ => return Err(not_found()),
	};
	let token = headers
		.get("authorization")
		.and_then(|v| v.to_str().ok())
		.and_then(|v| v.split_once(' '))
		.filter(|(scheme, _)| scheme.eq_ignore_ascii_case("bearer"))
		.map(|(_, t)| t.to_string())
		.or_else(|| {
			headers
				.get("apikey")
				.and_then(|v| v.to_str().ok())
				.map(str::to_string)
		})
		.ok_or_else(unauthorized)?;
	let claims =
		jwt::authorize(&token, &tenant.jwt_secret, now_secs()).map_err(|_| unauthorized())?;
	Ok((tenant, claims))
}

async fn broadcast(State(app): State<Arc<App>>, headers: HeaderMap, body: Bytes) -> Response {
	let (tenant, claims) = match http_caller(&app, &headers).await {
		Ok(c) => c,
		Err(r) => return *r,
	};
	let messages = match serde_json::from_slice::<Value>(&body)
		.ok()
		.and_then(|v| v.get("messages").cloned())
	{
		Some(Value::Array(m)) if !m.is_empty() => m,
		_ => {
			return json_response(
				StatusCode::UNPROCESSABLE_ENTITY,
				json!({ "errors": { "messages": ["can't be blank"] } }),
			);
		}
	};
	let max = tenant.max_payload_size_in_kb * 1000 + 500;
	for m in &messages {
		for field in ["topic", "payload", "event"] {
			if m.get(field).is_none_or(Value::is_null) {
				return json_response(
					StatusCode::UNPROCESSABLE_ENTITY,
					json!({ "errors": { "messages": [{ field: ["can't be blank"] }] } }),
				);
			}
		}
		if m["payload"].to_string().len() as i64 > max {
			return json_response(
				StatusCode::UNPROCESSABLE_ENTITY,
				json!({ "errors": { "messages": [{ "payload": ["Payload size exceeds tenant limit"] }] } }),
			);
		}
	}
	if app
		.hub
		.tenant(tenant.clone())
		.events
		.add(messages.len() as u64, tenant.max_events_per_second)
	{
		return json_response(
			StatusCode::TOO_MANY_REQUESTS,
			json!({ "message": "Too many messages to broadcast, please reduce the batch size" }),
		);
	}
	let rt = app.hub.tenant(tenant.clone());
	let mut allowed: HashMap<String, bool> = HashMap::new();
	for m in messages {
		let topic = m["topic"]
			.as_str()
			.map(str::to_string)
			.unwrap_or_else(|| m["topic"].to_string());
		let private = m.get("private").and_then(Value::as_bool).unwrap_or(false);
		if private {
			let ok = match allowed.get(&topic) {
				Some(ok) => *ok,
				None => {
					let ok = write_allowed(&app, &tenant, &claims, &headers, &topic).await;
					allowed.insert(topic.clone(), ok);
					ok
				}
			};
			if !ok {
				continue;
			}
		}
		let mut payload =
			json!({ "payload": m["payload"], "event": m["event"], "type": "broadcast" });
		if let Some(id) = m.get("id").filter(|v| !v.is_null()) {
			payload["meta"] = json!({ "id": id });
		}
		hub::deliver_json(
			&rt,
			&TopicKey {
				name: topic,
				private,
			},
			None,
			payload,
		);
	}
	StatusCode::ACCEPTED.into_response()
}

/// Maps with up to 32 keys, keys sorted, recursively: how the pinned server encodes a JSON
/// payload it re-encodes, which reaches a V2 client byte for byte.
fn sorted(value: Value) -> Value {
	match value {
		Value::Object(map) if map.len() <= 32 => {
			let mut entries: Vec<(String, Value)> = map.into_iter().collect();
			entries.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
			Value::Object(entries.into_iter().map(|(k, v)| (k, sorted(v))).collect())
		}
		Value::Object(map) => Value::Object(map.into_iter().map(|(k, v)| (k, sorted(v))).collect()),
		Value::Array(items) => Value::Array(items.into_iter().map(sorted).collect()),
		other => other,
	}
}

/// One broadcast, its topic and event in the path, its payload the body: JSON, or bytes with
/// `application/octet-stream`. `?private=true` asks the caller's write policy first.
async fn broadcast_one(
	State(app): State<Arc<App>>,
	headers: HeaderMap,
	Path((topic, event)): Path<(String, String)>,
	Query(params): Query<HashMap<String, String>>,
	body: Bytes,
) -> Response {
	let content_type = headers
		.get("content-type")
		.and_then(|v| v.to_str().ok())
		.map(|v| {
			v.split(';')
				.next()
				.unwrap_or("")
				.trim()
				.to_ascii_lowercase()
		});
	let binary = match content_type.as_deref() {
		None | Some("application/json") => false,
		Some("application/octet-stream") => true,
		Some(_) => {
			return json_response(
				StatusCode::UNSUPPORTED_MEDIA_TYPE,
				json!({ "error": "Unsupported Media Type. Use application/json or application/octet-stream" }),
			);
		}
	};
	let (tenant, claims) = match http_caller(&app, &headers).await {
		Ok(c) => c,
		Err(r) => return *r,
	};
	let (encoding, payload) = if binary {
		(Encoding::Binary, body.to_vec())
	} else {
		let value: Value = if body.is_empty() {
			json!({})
		} else {
			serde_json::from_slice(&body).unwrap_or(Value::Null)
		};
		if !value.is_object() {
			return json_response(
				StatusCode::UNPROCESSABLE_ENTITY,
				json!({ "errors": { "payload": ["is invalid"] } }),
			);
		}
		(Encoding::Json, sorted(value).to_string().into_bytes())
	};
	if payload.len() as i64 > tenant.max_payload_size_in_kb * 1000 + 500 {
		return json_response(
			StatusCode::UNPROCESSABLE_ENTITY,
			json!({ "errors": { "payload": ["Payload size exceeds tenant limit"] } }),
		);
	}
	let rt = app.hub.tenant(tenant.clone());
	if rt.events.add(1, tenant.max_events_per_second) {
		return json_response(
			StatusCode::TOO_MANY_REQUESTS,
			json!({ "message": "You have exceeded your rate limit" }),
		);
	}
	let private = matches!(params.get("private").map(String::as_str), Some("true"));
	if private && !write_allowed(&app, &tenant, &claims, &headers, &topic).await {
		return json_response(StatusCode::FORBIDDEN, json!({ "message": "Unauthorized" }));
	}
	hub::deliver_user(
		&rt,
		&TopicKey {
			name: topic,
			private,
		},
		None,
		hub::UserMessage {
			event: &event,
			encoding,
			payload: &payload,
			metadata: None,
			id: None,
		},
	);
	StatusCode::ACCEPTED.into_response()
}

async fn write_allowed(
	app: &App,
	tenant: &tenants::Tenant,
	claims: &Map<String, Value>,
	headers: &HeaderMap,
	topic: &str,
) -> bool {
	let Some(database) = &tenant.database else {
		return false;
	};
	let Ok(pool) = app.dbs.pool(&tenant.external_id, database).await else {
		return false;
	};
	let mut h = Map::new();
	for (name, value) in headers.iter() {
		if let Ok(v) = value.to_str() {
			h.insert(name.as_str().to_string(), Value::String(v.to_string()));
		}
	}
	let ctx = AuthContext {
		topic: topic.to_string(),
		role: claims
			.get("role")
			.and_then(Value::as_str)
			.unwrap_or_default()
			.to_string(),
		sub: claims
			.get("sub")
			.and_then(Value::as_str)
			.map(str::to_string),
		claims: claims.clone(),
		headers: h,
	};
	matches!(db::authorize_write(&pool, &ctx, "broadcast", Policies::default()).await, Ok(p) if p.broadcast_write == Some(true))
}

// --- metrics ------------------------------------------------------------------------------

async fn metrics(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
	if !admin_ok(&headers, &app.config.metrics_jwt_secret) {
		return StatusCode::FORBIDDEN.into_response();
	}
	let mut out = String::new();
	let series: [(&str, &str); 6] = [
		("realtime_connections_connected", "gauge"),
		("realtime_channel_events", "counter"),
		("realtime_channel_presence_events", "counter"),
		("realtime_channel_db_events", "counter"),
		("realtime_channel_joins", "counter"),
		("realtime_channel_output_bytes", "counter"),
	];
	let all = app.hub.all();
	for (name, kind) in series {
		out.push_str(&format!("# TYPE {name} {kind}\n"));
		for rt in &all {
			let c = &rt.counters;
			let v = match name {
				"realtime_connections_connected" => rt.connections() as u64,
				"realtime_channel_events" => c.events.load(Ordering::Relaxed),
				"realtime_channel_presence_events" => c.presence_events.load(Ordering::Relaxed),
				"realtime_channel_db_events" => c.db_events.load(Ordering::Relaxed),
				"realtime_channel_joins" => c.joins.load(Ordering::Relaxed),
				_ => c.output_bytes.load(Ordering::Relaxed),
			};
			out.push_str(&format!(
				"{name}{{tenant=\"{}\"}} {v}\n",
				rt.id.replace('"', "")
			));
		}
	}
	([("content-type", "text/plain; version=0.0.4")], out).into_response()
}
