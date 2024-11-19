use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::db::Value;
use super::types::HorcruxError;

pub enum Request {
    Set {
        key: String,
        flags: u32,
        _exptime: u32,
        data: String,
    },
    Get {
        key: String,
    },
    Snapshot,
}

pub async fn read_request(socket: &mut tokio::net::TcpStream) -> Result<Request, HorcruxError> {
    // read request line
    let mut buf = vec![0; 4096];
    let request: String;
    match socket.read(&mut buf).await {
        Ok(n) if n == 0 => return Err(HorcruxError::ParseRequest("Empty request".to_string())),
        Ok(n) => {
            request = String::from_utf8_lossy(&buf[..n]).to_string();
        }
        Err(_) => {
            println!("Failed to read from socket");
            return Err(HorcruxError::Connection(
                "Failed to read from socket".to_string(),
            ));
        }
    }

    // switch by command
    let parts: Vec<&str> = request.trim().split_whitespace().collect();
    if parts.is_empty() {
        return Err(HorcruxError::Ignorable);
    }
    let command = parts[0].to_lowercase();

    match command.as_str() {
        "set" => {
            // validate request
            if parts.len() != 5 && parts.len() != 6 {
                return Err(HorcruxError::ParseRequest("Invalid request".to_string()));
            }

            let flags = match parts[2].parse::<u32>() {
                Ok(flags) => flags,
                Err(_) => {
                    return Err(HorcruxError::ParseRequest("Invalid flags".to_string()));
                }
            };

            // validate exptime
            let exptime = match parts[3].parse::<u32>() {
                Ok(exptime) => exptime,
                Err(_) => {
                    return Err(HorcruxError::ParseRequest("Invalid exptime".to_string()));
                }
            };

            // read data
            let len = match parts[4].parse::<usize>() {
                Ok(len) => len,
                Err(_) => {
                    return Err(HorcruxError::ParseRequest(
                        "Invalid data length".to_string(),
                    ));
                }
            };
            let data: String;
            if parts.len() == 6 {
                data = parts[5].to_string();
            } else {
                let mut buf = vec![0; len];
                match socket.read(&mut buf).await {
                    Ok(n) if n != len => {
                        return Err(HorcruxError::Connection("Failed to read data".to_string()));
                    }
                    Ok(_) => {
                        data = String::from_utf8_lossy(&buf).to_string();
                    }
                    Err(_) => {
                        return Err(HorcruxError::Connection("Failed to read data".to_string()));
                    }
                }
            }

            Ok(Request::Set {
                key: parts[1].to_string(),
                flags,
                _exptime: exptime,
                data,
            })
        }
        "get" => {
            // validate request
            if parts.len() != 2 {
                return Err(HorcruxError::ParseRequest("Invalid request".to_string()));
            }

            let key = parts[1].to_string();
            Ok(Request::Get { key })
        }
        "snapshot" => Ok(Request::Snapshot),
        "quit" => {
            return Err(HorcruxError::Connection("Client quit".to_string()));
        }
        _ => {
            return Err(HorcruxError::ParseRequest("Invalid command".to_string()));
        }
    }
}

pub enum Response {
    Stored,
    Value(String, Option<Value>),
    Error,
    SnapshotFinished,
}

impl Response {
    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            Response::Stored => "STORED\r\n".as_bytes().to_vec(),
            Response::Value(key, response) => {
                if let Some(value) = response {
                    format!(
                        "VALUE {} {} {}\r\n{}\r\nEND\r\n",
                        key,
                        value.flags,
                        value.data.len(),
                        value.data
                    )
                    .as_bytes()
                    .to_vec()
                } else {
                    "END\r\n".as_bytes().to_vec()
                }
            }
            Response::Error => "ERROR\r\n".as_bytes().to_vec(),
            Response::SnapshotFinished => "SNAPSHOT FINISHED\r\n".as_bytes().to_vec(),
        }
    }
}

pub async fn send_response(
    socket: &mut tokio::net::TcpStream,
    response: Response,
) -> Result<(), HorcruxError> {
    if socket.write_all(&response.as_bytes()).await.is_err() {
        println!("Failed to send response");
        return Err(HorcruxError::Connection(
            "Failed to send response".to_string(),
        ));
    }
    Ok(())
}
