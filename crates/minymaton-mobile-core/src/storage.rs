//! On-device state in SQLite: bus profiles, the active profile, joined
//! subscriptions, received and sent messages, and each subscription's
//! acknowledgement still owed to the bus. Tokens are never stored here: the
//! platform keystore holds them and hands them over in memory.

use std::path::Path;
use std::sync::Mutex;

use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
use serde_json::Value;

use crate::CoreError;

pub struct Storage {
    connection: Mutex<Connection>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Profile {
    pub id: String,
    pub name: String,
    pub url: String,
    #[serde(skip)]
    pub ca_pem: Option<String>,
    pub has_ca: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct Joined {
    pub stream: String,
    pub label: String,
    pub handle: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct StoredMessage {
    pub message_id: String,
    pub sequence: u64,
    pub published_at: String,
    pub payload: Value,
    pub publisher: Option<String>,
    pub direction: String,
}

fn storage_error(error: &rusqlite::Error) -> CoreError {
    CoreError::new("storage_failed", format!("device storage failed: {error}"))
}

impl Storage {
    pub fn open(path: &Path) -> Result<Self, CoreError> {
        let connection = Connection::open(path).map_err(|error| storage_error(&error))?;
        connection
            .execute_batch(
                "PRAGMA journal_mode = WAL;
                 PRAGMA foreign_keys = ON;
                 CREATE TABLE IF NOT EXISTS profiles (
                     id TEXT PRIMARY KEY,
                     name TEXT NOT NULL,
                     url TEXT NOT NULL,
                     ca_pem TEXT
                 );
                 CREATE TABLE IF NOT EXISTS state (
                     key TEXT PRIMARY KEY,
                     value TEXT NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS subscriptions (
                     profile_id TEXT NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
                     stream TEXT NOT NULL,
                     label TEXT NOT NULL,
                     handle TEXT NOT NULL,
                     pending_ack TEXT,
                     PRIMARY KEY (profile_id, stream)
                 );
                 CREATE TABLE IF NOT EXISTS messages (
                     profile_id TEXT NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
                     stream TEXT NOT NULL,
                     message_id TEXT NOT NULL,
                     sequence INTEGER NOT NULL,
                     published_at TEXT NOT NULL,
                     payload TEXT NOT NULL,
                     publisher TEXT,
                     direction TEXT NOT NULL,
                     PRIMARY KEY (profile_id, stream, message_id)
                 );",
            )
            .map_err(|error| storage_error(&error))?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    fn with<T>(
        &self,
        work: impl FnOnce(&mut Connection) -> rusqlite::Result<T>,
    ) -> Result<T, CoreError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| CoreError::new("storage_failed", "device storage lock was poisoned"))?;
        work(&mut connection).map_err(|error| storage_error(&error))
    }

    pub fn add_profile(&self, profile: &Profile) -> Result<(), CoreError> {
        self.with(|db| {
            db.execute(
                "INSERT INTO profiles (id, name, url, ca_pem) VALUES (?1, ?2, ?3, ?4)",
                params![profile.id, profile.name, profile.url, profile.ca_pem],
            )
            .map(drop)
        })
    }

    pub fn remove_profile(&self, id: &str) -> Result<bool, CoreError> {
        self.with(|db| {
            let tx = db.transaction()?;
            tx.execute(
                "DELETE FROM state WHERE key = 'active_profile' AND value = ?1",
                params![id],
            )?;
            let removed = tx.execute("DELETE FROM profiles WHERE id = ?1", params![id])?;
            tx.commit()?;
            Ok(removed > 0)
        })
    }

    pub fn profiles(&self) -> Result<Vec<Profile>, CoreError> {
        self.with(|db| {
            let mut statement =
                db.prepare("SELECT id, name, url, ca_pem FROM profiles ORDER BY rowid")?;
            statement
                .query_map([], |row| {
                    let ca_pem: Option<String> = row.get(3)?;
                    Ok(Profile {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        url: row.get(2)?,
                        has_ca: ca_pem.is_some(),
                        ca_pem,
                    })
                })?
                .collect()
        })
    }

    pub fn profile(&self, id: &str) -> Result<Option<Profile>, CoreError> {
        Ok(self
            .profiles()?
            .into_iter()
            .find(|profile| profile.id == id))
    }

    pub fn set_active_profile(&self, id: Option<&str>) -> Result<(), CoreError> {
        self.with(|db| match id {
            Some(id) => db
                .execute(
                    "INSERT INTO state (key, value) VALUES ('active_profile', ?1)
                     ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    params![id],
                )
                .map(drop),
            None => db
                .execute("DELETE FROM state WHERE key = 'active_profile'", [])
                .map(drop),
        })
    }

    pub fn active_profile(&self) -> Result<Option<String>, CoreError> {
        self.with(|db| {
            db.query_row(
                "SELECT value FROM state WHERE key = 'active_profile'",
                [],
                |row| row.get(0),
            )
            .optional()
        })
    }

    pub fn save_subscription(&self, profile_id: &str, joined: &Joined) -> Result<(), CoreError> {
        self.with(|db| {
            db.execute(
                "INSERT INTO subscriptions (profile_id, stream, label, handle) VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(profile_id, stream) DO UPDATE SET
                     label = excluded.label,
                     handle = excluded.handle,
                     pending_ack = CASE WHEN subscriptions.handle = excluded.handle
                                        THEN subscriptions.pending_ack END",
                params![profile_id, joined.stream, joined.label, joined.handle],
            )
            .map(drop)
        })
    }

    pub fn remove_subscription(&self, profile_id: &str, stream: &str) -> Result<(), CoreError> {
        self.with(|db| {
            db.execute(
                "DELETE FROM subscriptions WHERE profile_id = ?1 AND stream = ?2",
                params![profile_id, stream],
            )
            .map(drop)
        })
    }

    pub fn remove_streams(&self, profile_id: &str, streams: &[String]) -> Result<(), CoreError> {
        if streams.is_empty() {
            return Ok(());
        }
        self.with(|db| {
            let tx = db.transaction()?;
            for stream in streams {
                tx.execute(
                    "DELETE FROM subscriptions WHERE profile_id = ?1 AND stream = ?2",
                    params![profile_id, stream],
                )?;
                tx.execute(
                    "DELETE FROM messages WHERE profile_id = ?1 AND stream = ?2",
                    params![profile_id, stream],
                )?;
            }
            tx.commit()
        })
    }

    pub fn remove_subscriptions(
        &self,
        profile_id: &str,
        stream: &str,
        labels: &[String],
    ) -> Result<Vec<String>, CoreError> {
        if labels.is_empty() {
            return Ok(Vec::new());
        }
        self.with(|db| {
            let tx = db.transaction()?;
            let mut removed = Vec::new();
            for label in labels {
                let count = tx.execute(
                    "DELETE FROM subscriptions WHERE profile_id = ?1 AND stream = ?2 AND label = ?3",
                    params![profile_id, stream, label],
                )?;
                if count > 0 {
                    removed.push(label.clone());
                }
            }
            tx.commit()?;
            Ok(removed)
        })
    }

    pub fn subscriptions(&self, profile_id: &str) -> Result<Vec<Joined>, CoreError> {
        self.with(|db| {
            let mut statement = db.prepare(
                "SELECT stream, label, handle FROM subscriptions WHERE profile_id = ?1 ORDER BY stream",
            )?;
            statement
                .query_map(params![profile_id], |row| {
                    Ok(Joined {
                        stream: row.get(0)?,
                        label: row.get(1)?,
                        handle: row.get(2)?,
                    })
                })?
                .collect()
        })
    }

    pub fn pending_ack(&self, profile_id: &str, stream: &str) -> Result<Option<String>, CoreError> {
        self.with(|db| {
            db.query_row(
                "SELECT pending_ack FROM subscriptions WHERE profile_id = ?1 AND stream = ?2",
                params![profile_id, stream],
                |row| row.get(0),
            )
            .optional()
            .map(Option::flatten)
        })
    }

    pub fn clear_pending_ack(&self, profile_id: &str, stream: &str) -> Result<(), CoreError> {
        self.with(|db| {
            db.execute(
                "UPDATE subscriptions SET pending_ack = NULL WHERE profile_id = ?1 AND stream = ?2",
                params![profile_id, stream],
            )
            .map(drop)
        })
    }

    /// Persists a received batch and the acknowledgement it now owes, in one
    /// transaction. The batch is acknowledged to the bus only after this
    /// commits, so a storage failure can never lose a message.
    pub fn record_batch(
        &self,
        profile_id: &str,
        stream: &str,
        messages: &[StoredMessage],
        ack: &str,
    ) -> Result<Vec<StoredMessage>, CoreError> {
        self.with(|db| {
            let tx = db.transaction()?;
            let mut fresh = Vec::new();
            for message in messages {
                if insert_message(&tx, profile_id, stream, message)? {
                    fresh.push(message.clone());
                }
            }
            tx.execute(
                "UPDATE subscriptions SET pending_ack = ?3 WHERE profile_id = ?1 AND stream = ?2",
                params![profile_id, stream, ack],
            )?;
            tx.commit()?;
            Ok(fresh)
        })
    }

    /// Persists retained history for a stream without touching the pending
    /// acknowledgement of any joined subscription: history sync reads with
    /// a temporary subscription, so its batches are not the watcher's.
    pub fn record_history(
        &self,
        profile_id: &str,
        stream: &str,
        messages: &[StoredMessage],
    ) -> Result<usize, CoreError> {
        self.with(|db| {
            let tx = db.transaction()?;
            let mut fresh = 0;
            for message in messages {
                if insert_message(&tx, profile_id, stream, message)? {
                    fresh += 1;
                }
            }
            tx.commit()?;
            Ok(fresh)
        })
    }

    pub fn record_sent(
        &self,
        profile_id: &str,
        stream: &str,
        message: &StoredMessage,
    ) -> Result<(), CoreError> {
        self.with(|db| insert_message(db, profile_id, stream, message).map(drop))
    }

    pub fn messages(
        &self,
        profile_id: &str,
        stream: &str,
        limit: u32,
    ) -> Result<Vec<StoredMessage>, CoreError> {
        self.with(|db| {
            let mut statement = db.prepare(
                "SELECT message_id, sequence, published_at, payload, publisher, direction FROM (
                     SELECT * FROM messages WHERE profile_id = ?1 AND stream = ?2
                     ORDER BY sequence DESC LIMIT ?3
                 ) ORDER BY sequence ASC",
            )?;
            statement
                .query_map(params![profile_id, stream, limit], |row| {
                    let payload: String = row.get(3)?;
                    Ok(StoredMessage {
                        message_id: row.get(0)?,
                        sequence: row.get(1)?,
                        published_at: row.get(2)?,
                        payload: serde_json::from_str(&payload).unwrap_or(Value::Null),
                        publisher: row.get(4)?,
                        direction: row.get(5)?,
                    })
                })?
                .collect()
        })
    }
}

fn insert_message(
    db: &Connection,
    profile_id: &str,
    stream: &str,
    message: &StoredMessage,
) -> rusqlite::Result<bool> {
    let inserted = db.execute(
        "INSERT OR IGNORE INTO messages
             (profile_id, stream, message_id, sequence, published_at, payload, publisher, direction)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            profile_id,
            stream,
            message.message_id,
            message.sequence,
            message.published_at,
            message.payload.to_string(),
            message.publisher,
            message.direction,
        ],
    )?;
    Ok(inserted > 0)
}
