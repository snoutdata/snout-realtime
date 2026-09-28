//! The snout-realtime binary. Wiring only; everything it runs is in the library.

use std::sync::Arc;

use snout_realtime::config::Config;
use snout_realtime::tenants::Registry;
use snout_realtime::{App, api, db, hub};

async fn serve() -> Result<(), String> {
	let config = Config::from_env().map_err(|e| e.to_string())?;
	let registry = Registry::new(&config.metadata_url, &config.encryption_key);
	registry
		.migrate()
		.await
		.map_err(|e| format!("metadata migrations: {e}"))?;
	let address = format!("{}:{}", config.host, config.port);
	let app = Arc::new(App {
		config,
		registry,
		hub: hub::Hub::default(),
		dbs: db::Databases::default(),
	});
	let listener = tokio::net::TcpListener::bind(&address)
		.await
		.map_err(|e| format!("{address}: {e}"))?;
	tracing::info!(%address, "snout-realtime listening");
	tokio::spawn(snout_realtime::upkeep(app.clone()));
	tokio::select! {
		result = axum::serve(listener, api::router(app)) => result.map_err(|e| e.to_string()),
		_ = stopped() => Ok(()),
	}
}

/// Ctrl-C, or SIGTERM: as PID 1 in a container the kernel gives SIGTERM no default action, so
/// without this `podman stop` waited out its timeout and killed the process.
async fn stopped() {
	#[cfg(unix)]
	{
		let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
			.expect("a SIGTERM handler");
		tokio::select! {
			_ = tokio::signal::ctrl_c() => {}
			_ = term.recv() => {}
		}
	}
	#[cfg(not(unix))]
	let _ = tokio::signal::ctrl_c().await;
}

#[tokio::main]
async fn main() {
	tracing_subscriber::fmt()
		.with_env_filter(
			tracing_subscriber::EnvFilter::try_from_env("LOG_LEVEL")
				.unwrap_or_else(|_| "info".into()),
		)
		.json()
		.init();
	if let Err(e) = serve().await {
		eprintln!("snout-realtime: {e}");
		std::process::exit(1);
	}
}
