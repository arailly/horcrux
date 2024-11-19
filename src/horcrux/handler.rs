use std::sync::Arc;

use super::db::{Value, DB};
use super::types::HorcruxError;

pub async fn handle_set(
    db: &Arc<DB>,
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
        db.write().await.insert(key, value);
    }
    Ok(())
}

pub async fn handle_get(db: &Arc<DB>, key: &str) -> Option<Value> {
    {
        let db = db.read().await;
        return db.get(key).cloned();
    }
}
