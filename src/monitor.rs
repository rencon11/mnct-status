use std::time::{Duration, Instant};

use chrono::Utc;
use reqwest::{Client, StatusCode};
use tracing::{error, info, warn};

use crate::db::{Database, NewPing};

#[derive(Debug, Clone, Copy)]
pub struct Target {
    pub url: &'static str,
    pub name: &'static str,
    pub allow_403: bool,
}

impl Target {
    /// Returns true when the HTTP status code represents a reachable/healthy server.
    pub fn is_healthy_status(self, status: StatusCode) -> bool {
        status.is_success()
            || status.is_redirection()
            || (self.allow_403 && status == StatusCode::FORBIDDEN)
    }
}

pub const TARGETS: &[Target] = &[
    Target {
        url: "https://www.maizuru-ct.ac.jp/",
        name: "舞鶴高専 公式サイト",
        allow_403: false,
    },
    Target {
        url: "https://kidp.maizuru-ct.ac.jp/",
        name: "KIDP",
        allow_403: true,
    },
    Target {
        url: "https://moodle2.maizuru-ct.ac.jp/",
        name: "Moodle 2",
        allow_403: false,
    },
    Target {
        url: "https://gaihaku-maizuru.mogsmile.com/",
        name: "外泊願システム",
        allow_403: false,
    },
];

pub fn target_name(url: &str) -> &'static str {
    TARGETS
        .iter()
        .find(|t| t.url == url)
        .map(|t| t.name)
        .unwrap_or("Unknown")
}

/// Spawns the background monitoring loop that sends HEAD requests to all targets
/// every `interval_secs` seconds.
pub fn spawn_monitor(db: Database, interval_secs: u64, timeout_secs: u64) {
    let client = Client::builder()
        .timeout(Duration::from_secs(timeout_secs))
        .user_agent("mnct-status-monitor/0.1")
        .build()
        .expect("failed to build reqwest client");

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            ticker.tick().await;
            let futures = TARGETS.iter().map(|target| {
                check_and_record(client.clone(), db.clone(), *target, timeout_secs)
            });
            futures_join_all(futures).await;
        }
    });
}

async fn futures_join_all<I, F>(futures: I)
where
    I: IntoIterator<Item = F>,
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let handles: Vec<_> = futures.into_iter().map(tokio::spawn).collect();
    for handle in handles {
        if let Err(err) = handle.await {
            error!("Monitor task panicked: {err}");
        }
    }
}

async fn check_and_record(client: Client, db: Database, target: Target, timeout_secs: u64) {
    let checked_at = Utc::now();
    let start = Instant::now();
    let result = client.head(target.url).send().await;
    let elapsed_ms = start.elapsed().as_millis() as i64;

    let ping = match result {
        Ok(resp) => {
            let status = resp.status();
            let success = target.is_healthy_status(status);
            let error_message = if success {
                None
            } else {
                Some(format!("HTTP {}", status))
            };

            if success {
                info!(target = target.url, status = status.as_u16(), latency_ms = elapsed_ms, "HEAD check succeeded");
            } else {
                warn!(target = target.url, status = status.as_u16(), latency_ms = elapsed_ms, "HEAD check returned error status");
            }

            NewPing {
                target: target.url.to_string(),
                checked_at,
                latency_ms: Some(elapsed_ms),
                status_code: Some(status.as_u16()),
                success,
                error_message,
            }
        }
        Err(err) => {
            let cause = classify_reqwest_error(&err, timeout_secs);
            warn!(target = target.url, cause = %cause, "HEAD check failed");

            NewPing {
                target: target.url.to_string(),
                checked_at,
                latency_ms: None,
                status_code: err.status().map(|s| s.as_u16()),
                success: false,
                error_message: Some(cause),
            }
        }
    };

    let res = tokio::task::spawn_blocking(move || db.record_check(&ping)).await;
    match res {
        Ok(Ok(())) => {}
        Ok(Err(err)) => error!(target = target.url, "Failed to save check result to SQLite: {err}"),
        Err(err) => error!(target = target.url, "Database task failed: {err}"),
    }
}

fn classify_reqwest_error(err: &reqwest::Error, timeout_secs: u64) -> String {
    if err.is_timeout() {
        format!("接続タイムアウト ({timeout_secs}秒)")
    } else if err.is_connect() {
        format!("接続エラー: {err}")
    } else if err.is_request() {
        format!("リクエストエラー: {err}")
    } else {
        format!("通信エラー: {err}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kidp_treats_403_as_healthy_while_others_do_not() {
        let kidp = TARGETS
            .iter()
            .find(|t| t.url == "https://kidp.maizuru-ct.ac.jp/")
            .expect("kidp target should exist");
        let gaihaku = TARGETS
            .iter()
            .find(|t| t.url == "https://gaihaku-maizuru.mogsmile.com/")
            .expect("gaihaku target should exist");

        assert!(kidp.is_healthy_status(StatusCode::FORBIDDEN));
        assert!(kidp.is_healthy_status(StatusCode::OK));
        assert!(!kidp.is_healthy_status(StatusCode::SERVICE_UNAVAILABLE));

        assert!(!gaihaku.is_healthy_status(StatusCode::FORBIDDEN));
        assert!(gaihaku.is_healthy_status(StatusCode::OK));
        assert!(gaihaku.is_healthy_status(StatusCode::FOUND));
    }
}
