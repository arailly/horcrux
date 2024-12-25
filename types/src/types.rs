#[derive(Debug)]
pub enum HorcruxError {
    ParseRequest(String),
    RestoreDB(String),
    Connection(String),
    Ignorable,
    Internal,
}

impl std::fmt::Display for HorcruxError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            HorcruxError::ParseRequest(msg) => write!(f, "Failed to parse request: {}", msg),
            HorcruxError::RestoreDB(msg) => write!(f, "Failed to restore DB: {}", msg),
            HorcruxError::Connection(msg) => write!(f, "Connection error: {}", msg),
            HorcruxError::Ignorable => write!(f, "Ignorable error"),
            HorcruxError::Internal => write!(f, "Internal error"),
        }
    }
}

impl std::error::Error for HorcruxError {}
