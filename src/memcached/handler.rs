use std::sync::Arc;
use tokio::io::AsyncReadExt;

use super::db::{Value, DB};
use super::types::{MemcachedError, Response};

pub async fn handle_set(
    socket: &mut tokio::net::TcpStream,
    db: &Arc<DB>,
    parts: Vec<&str>,
) -> Result<Response, MemcachedError> {
    // validate request
    if parts.len() != 5 {
        return Ok(Response::Error);
    }

    let flags = match parts[2].parse::<u32>() {
        Ok(flags) => flags,
        Err(_) => {
            return Ok(Response::Error);
        }
    };

    // validate exptime
    if parts[3].parse::<u32>().is_err() {
        return Ok(Response::Error);
    }

    // read data
    let len = match parts[4].parse::<usize>() {
        Ok(len) => len,
        Err(_) => {
            return Ok(Response::Error);
        }
    };
    let mut buf = vec![0; len];
    match socket.read(&mut buf).await {
        Ok(n) if n != len => {
            return Err(MemcachedError::Connection(
                "Failed to read data".to_string(),
            ));
        }
        Ok(_) => {}
        Err(_) => {
            return Err(MemcachedError::Connection(
                "Failed to read data".to_string(),
            ));
        }
    }

    // store data
    let key = parts[1].to_string();
    let value = Value {
        flags,
        data: String::from_utf8_lossy(&buf).to_string(),
    };
    {
        db.lock().await.insert(key, value);
    }

    // send response
    Ok(Response::Stored)
}

pub async fn handle_get(db: &Arc<DB>, parts: Vec<&str>) -> Response {
    // validate request
    if parts.len() != 2 {
        return Response::Error;
    }

    // get data
    let key = parts[1];
    let response: String;
    {
        let db = db.lock().await;
        if let Some(value) = db.get(key) {
            response = format!(
                "VALUE {} {} {}\r\n{}\r\nEND\r\n",
                key,
                value.flags,
                value.data.len(),
                value.data,
            );
        } else {
            response = "END\r\n".to_string();
        }
    }

    return Response::Value(response);
}
