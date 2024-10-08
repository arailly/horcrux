pub enum Response {
    Stored,
    Value(String),
    Error,
    SnapshotFinished,
}

impl Response {
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Response::Stored => "STORED\r\n".as_bytes(),
            Response::Value(response) => response.as_bytes(),
            Response::Error => "ERROR\r\n".as_bytes(),
            Response::SnapshotFinished => "SNAPSHOT FINISHED\r\n".as_bytes(),
        }
    }
}

#[derive(Debug)]
pub enum HorcruxError {
    RestoreDB(String),
    Connection(String),
}

impl std::fmt::Display for HorcruxError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            HorcruxError::RestoreDB(msg) => write!(f, "Failed to restore DB: {}", msg),
            HorcruxError::Connection(msg) => write!(f, "Connection error: {}", msg),
        }
    }
}

impl std::error::Error for HorcruxError {}
