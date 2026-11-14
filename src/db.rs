use std::path::Path;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Duration, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;

#[derive(Clone)]
pub struct Database {
    conn: Arc<Mutex<Connection>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PingRecord {
    pub id: i64,
    pub target: String,
    pub checked_at: String,
    pub latency_ms: Option<i64>,
    pub status_code: Option<u16>,
    pub success: bool,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct IncidentRecord {
    pub id: i64,
    pub target: String,
    pub started_at: String,
    pub ended_at: Option<String>,
    pub cause: String,
}

#[derive(Debug, Clone)]
pub struct NewPing {
    pub target: String,
    pub checked_at: DateTime<Utc>,
    pub latency_ms: Option<i64>,
    pub status_code: Option<u16>,
    pub success: bool,
    pub error_message: Option<String>,
}

impl Database {
    /// Opens (or creates) the SQLite database and initializes the schema.
    pub fn open(path: impl AsRef<Path>) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch(
            "
            PRAGMA journal_mode = WAL;
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS pings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                target TEXT NOT NULL,
                checked_at TEXT NOT NULL,
                latency_ms INTEGER,
                status_code INTEGER,
                success INTEGER NOT NULL,
                error_message TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_pings_target_checked_at
                ON pings(target, checked_at);

            CREATE TABLE IF NOT EXISTS incidents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                target TEXT NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                cause TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_incidents_target_started_at
                ON incidents(target, started_at);
            ",
        )?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Records a ping result and opens or closes an incident as needed.
    pub fn record_check(&self, ping: &NewPing) -> rusqlite::Result<()> {
        let mut conn = self.conn.lock().expect("database mutex poisoned");
        let tx = conn.transaction()?;

        let timestamp = ping.checked_at.to_rfc3339();
        let status_code = ping.status_code.map(i64::from);

        tx.execute(
            "INSERT INTO pings (target, checked_at, latency_ms, status_code, success, error_message)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                ping.target,
                timestamp,
                ping.latency_ms,
                status_code,
                i32::from(ping.success),
                ping.error_message,
            ],
        )?;

        let open_incident_id: Option<i64> = tx
            .query_row(
                "SELECT id FROM incidents
                 WHERE target = ?1 AND ended_at IS NULL
                 ORDER BY id DESC LIMIT 1",
                params![ping.target],
                |row| row.get(0),
            )
            .optional()?;

        if ping.success {
            if let Some(id) = open_incident_id {
                tx.execute(
                    "UPDATE incidents SET ended_at = ?1 WHERE id = ?2",
                    params![timestamp, id],
                )?;
            }
        } else if open_incident_id.is_none() {
            let cause = ping
                .error_message
                .clone()
                .unwrap_or_else(|| "Unknown connection failure".to_string());
            tx.execute(
                "INSERT INTO incidents (target, started_at, ended_at, cause)
                 VALUES (?1, ?2, NULL, ?3)",
                params![ping.target, timestamp, cause],
            )?;
        }

        tx.commit()?;
        Ok(())
    }

    /// Returns all pings for `target` within the past `hours` hours, ordered oldest to newest.
    pub fn get_recent_pings(&self, target: &str, hours: i64) -> rusqlite::Result<Vec<PingRecord>> {
        let conn = self.conn.lock().expect("database mutex poisoned");
        let cutoff = (Utc::now() - Duration::hours(hours)).to_rfc3339();

        let mut stmt = conn.prepare(
            "SELECT id, target, checked_at, latency_ms, status_code, success, error_message
             FROM pings
             WHERE target = ?1 AND checked_at >= ?2
             ORDER BY checked_at ASC",
        )?;

        let rows = stmt.query_map(params![target, cutoff], |row| {
            let status_code: Option<i64> = row.get(4)?;
            let success_int: i32 = row.get(5)?;
            Ok(PingRecord {
                id: row.get(0)?,
                target: row.get(1)?,
                checked_at: row.get(2)?,
                latency_ms: row.get(3)?,
                status_code: status_code.and_then(|v| u16::try_from(v).ok()),
                success: success_int != 0,
                error_message: row.get(6)?,
            })
        })?;

        rows.collect()
    }

    /// Returns recent incidents (newest first), up to `limit` rows.
    pub fn get_incidents(&self, limit: i64) -> rusqlite::Result<Vec<IncidentRecord>> {
        let conn = self.conn.lock().expect("database mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT id, target, started_at, ended_at, cause
             FROM incidents
             ORDER BY started_at DESC, id DESC
             LIMIT ?1",
        )?;

        let rows = stmt.query_map(params![limit], |row| {
            Ok(IncidentRecord {
                id: row.get(0)?,
                target: row.get(1)?,
                started_at: row.get(2)?,
                ended_at: row.get(3)?,
                cause: row.get(4)?,
            })
        })?;

        rows.collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_pings_and_manages_incident_lifecycle() {
        let db = Database::open(":memory:").unwrap();
        let target = "https://www.maizuru-ct.ac.jp/";
        let t0 = Utc::now() - Duration::minutes(10);
        let t1 = t0 + Duration::minutes(1);
        let t2 = t0 + Duration::minutes(2);
        let t3 = t0 + Duration::minutes(3);

        // Initial healthy check: no incident created.
        db.record_check(&NewPing {
            target: target.to_string(),
            checked_at: t0,
            latency_ms: Some(42),
            status_code: Some(200),
            success: true,
            error_message: None,
        })
        .unwrap();
        assert!(db.get_incidents(10).unwrap().is_empty());

        // First failure: opens an incident.
        db.record_check(&NewPing {
            target: target.to_string(),
            checked_at: t1,
            latency_ms: None,
            status_code: None,
            success: false,
            error_message: Some("接続タイムアウト".to_string()),
        })
        .unwrap();
        let incidents = db.get_incidents(10).unwrap();
        assert_eq!(incidents.len(), 1);
        assert!(incidents[0].ended_at.is_none());
        assert_eq!(incidents[0].cause, "接続タイムアウト");

        // Consecutive failure: does not duplicate incident.
        db.record_check(&NewPing {
            target: target.to_string(),
            checked_at: t2,
            latency_ms: Some(120),
            status_code: Some(503),
            success: false,
            error_message: Some("HTTP 503 Service Unavailable".to_string()),
        })
        .unwrap();
        assert_eq!(db.get_incidents(10).unwrap().len(), 1);

        // Recovery: closes the open incident with ended_at = t3.
        db.record_check(&NewPing {
            target: target.to_string(),
            checked_at: t3,
            latency_ms: Some(35),
            status_code: Some(200),
            success: true,
            error_message: None,
        })
        .unwrap();
        let incidents = db.get_incidents(10).unwrap();
        assert_eq!(incidents.len(), 1);
        assert_eq!(incidents[0].ended_at.as_deref(), Some(t3.to_rfc3339().as_str()));

        let pings = db.get_recent_pings(target, 12).unwrap();
        assert_eq!(pings.len(), 4);
        assert_eq!(pings[0].latency_ms, Some(42));
        assert!(!pings[1].success);
        assert!(pings[3].success);
    }
}
