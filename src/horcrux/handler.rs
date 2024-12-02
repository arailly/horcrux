use std::sync::Arc;

use super::db::{Value, DB};
use super::snapshot::take_snapshot;
use super::types::HorcruxError;

pub trait Handler: SetHandler + GetHandler + SnapshotHandler {}

pub trait SetHandler {
    async fn set(
        &self,
        key: String,
        flags: u32,
        exptime: u32,
        data: String,
    ) -> Result<(), HorcruxError>;
}

pub trait GetHandler {
    async fn get(&self, key: &str) -> Option<Value>;
}

pub trait SnapshotHandler {
    async fn snapshot(&self);
}

pub struct BaseHandler {
    db: Arc<DB>,
    snapshot_dir: String,
}

impl BaseHandler {
    pub fn new(db: Arc<DB>, snapshot_dir: String) -> Self {
        BaseHandler { db, snapshot_dir }
    }

    pub fn clone(&self) -> Self {
        BaseHandler {
            db: self.db.clone(),
            snapshot_dir: self.snapshot_dir.clone(),
        }
    }
}

impl SetHandler for BaseHandler {
    async fn set(
        &self,
        key: String,
        flags: u32,
        _exptime: u32,
        data: String,
    ) -> Result<(), HorcruxError> {
        let value = Value {
            flags: flags,
            data: data,
        };
        {
            self.db.write().await.insert(key, value);
        }
        Ok(())
    }
}

impl GetHandler for BaseHandler {
    async fn get(&self, key: &str) -> Option<Value> {
        {
            let db = self.db.read().await;
            return db.get(key).cloned();
        }
    }
}

impl SnapshotHandler for BaseHandler {
    async fn snapshot(&self) {
        take_snapshot(&self.db, &self.snapshot_dir).await;
    }
}

impl Handler for BaseHandler {}
