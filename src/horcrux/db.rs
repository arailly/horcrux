use std::collections::HashMap;
use tokio::sync::RwLock;

pub type DB = RwLock<HashMap<String, Value>>;

#[derive(Debug, Clone)]
pub struct Value {
    pub flags: u32,
    // exptime: u32,
    pub data: String,
}
