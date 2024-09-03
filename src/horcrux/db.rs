use std::collections::HashMap;
use tokio::sync::Mutex;

pub type DB = Mutex<HashMap<String, Value>>;

pub struct Value {
    pub flags: u32,
    // exptime: u32,
    pub data: String,
}
