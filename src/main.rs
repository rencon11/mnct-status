mod db;
mod monitor;
mod web;

use std::env;
use std::net::SocketAddr;

use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::db::Database;
use crate::web::AppState;

fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(default)
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let db_path = env::var("DATABASE_PATH").unwrap_or_else(|_| "status.db".to_string());
    let port = u16::try_from(env_u64("PORT", 3000)).unwrap_or(3000);
    let interval_secs = env_u64("CHECK_INTERVAL_SECS", 60);
    let timeout_secs = env_u64("REQUEST_TIMEOUT_SECS", 10);

    let db = Database::open(&db_path).expect("failed to initialize SQLite database");
    info!(path = %db_path, "SQLite database initialized");

    monitor::spawn_monitor(db.clone(), interval_secs, timeout_secs);
    info!(interval_secs, timeout_secs, "Background monitor started");

    let app = web::router(AppState { db, interval_secs });
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr)
        .await
        .expect("failed to bind TCP listener");

    info!("HTTP server listening on http://{addr}");
    axum::serve(listener, app)
        .await
        .expect("HTTP server exited with error");
}
