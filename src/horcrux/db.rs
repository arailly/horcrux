use std::collections::HashMap;

pub type DB = HashMap<String, Value>;

#[derive(Debug, Clone)]
pub struct Value {
    pub flags: u32,
    // exptime: u32,
    pub data: String,
}
