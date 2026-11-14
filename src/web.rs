use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Json};
use axum::routing::get;
use axum::Router;
use chrono::Utc;
use serde::Serialize;

use crate::db::{Database, IncidentRecord, PingRecord};
use crate::monitor::{TARGETS, target_name};

#[derive(Clone)]
pub struct AppState {
    pub db: Database,
    pub interval_secs: u64,
}

#[derive(Serialize)]
struct StatusResponse {
    generated_at: String,
    interval_secs: u64,
    targets: Vec<TargetStatus>,
    incidents: Vec<IncidentView>,
}

#[derive(Serialize)]
struct TargetStatus {
    url: &'static str,
    name: &'static str,
    latest_ping: Option<PingRecord>,
    uptime_12h_pct: Option<f64>,
    avg_latency_12h_ms: Option<f64>,
    min_latency_12h_ms: Option<i64>,
    max_latency_12h_ms: Option<i64>,
    pings_12h: Vec<PingRecord>,
}

#[derive(Serialize)]
struct IncidentView {
    id: i64,
    target: String,
    target_name: &'static str,
    started_at: String,
    ended_at: Option<String>,
    cause: String,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(index_handler))
        .route("/api/status", get(status_api_handler))
        .with_state(state)
}

async fn index_handler() -> Html<&'static str> {
    Html(include_str!("../static/index.html"))
}

async fn status_api_handler(State(state): State<AppState>) -> impl IntoResponse {
    let db = state.db.clone();
    let interval_secs = state.interval_secs;

    let result = tokio::task::spawn_blocking(move || -> rusqlite::Result<StatusResponse> {
        let mut targets = Vec::with_capacity(TARGETS.len());

        for target in TARGETS {
            let pings_12h = db.get_recent_pings(target.url, 12)?;
            let latest_ping = pings_12h.last().cloned();

            let total = pings_12h.len();
            let uptime_12h_pct = if total > 0 {
                let ok_count = pings_12h.iter().filter(|p| p.success).count();
                Some((ok_count as f64 / total as f64) * 100.0)
            } else {
                None
            };

            let latencies: Vec<i64> = pings_12h
                .iter()
                .filter(|p| p.success)
                .filter_map(|p| p.latency_ms)
                .collect();

            let (avg_latency_12h_ms, min_latency_12h_ms, max_latency_12h_ms) = if latencies.is_empty() {
                (None, None, None)
            } else {
                let sum: i64 = latencies.iter().sum();
                let min = latencies.iter().copied().min();
                let max = latencies.iter().copied().max();
                (Some(sum as f64 / latencies.len() as f64), min, max)
            };

            targets.push(TargetStatus {
                url: target.url,
                name: target.name,
                latest_ping,
                uptime_12h_pct,
                avg_latency_12h_ms,
                min_latency_12h_ms,
                max_latency_12h_ms,
                pings_12h,
            });
        }

        let raw_incidents: Vec<IncidentRecord> = db.get_incidents(100)?;
        let incidents = raw_incidents
            .into_iter()
            .map(|inc| IncidentView {
                target_name: target_name(&inc.target),
                id: inc.id,
                target: inc.target,
                started_at: inc.started_at,
                ended_at: inc.ended_at,
                cause: inc.cause,
            })
            .collect();

        Ok(StatusResponse {
            generated_at: Utc::now().to_rfc3339(),
            interval_secs,
            targets,
            incidents,
        })
    })
    .await;

    match result {
        Ok(Ok(payload)) => (StatusCode::OK, Json(payload)).into_response(),
        Ok(Err(err)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Database error: {err}"),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Task error: {err}"),
        )
            .into_response(),
    }
}
