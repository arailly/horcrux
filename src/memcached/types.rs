pub enum Response {
    Stored,
    Value(String),
    Error,
    Accepted,
}

impl Response {
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Response::Stored => "STORED\r\n".as_bytes(),
            Response::Value(response) => response.as_bytes(),
            Response::Error => "ERROR\r\n".as_bytes(),
            Response::Accepted => "ACCEPTED\r\n".as_bytes(),
        }
    }
}

#[derive(Debug)]
pub enum MemcachedError {
    RestoreDB(String),
    Connection(String),
}

impl std::fmt::Display for MemcachedError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            MemcachedError::RestoreDB(msg) => write!(f, "Failed to restore DB: {}", msg),
            MemcachedError::Connection(msg) => write!(f, "Connection error: {}", msg),
        }
    }
}

impl std::error::Error for MemcachedError {}
