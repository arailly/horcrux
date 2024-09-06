use std::sync::Arc;
use tokio::io::AsyncReadExt;

use super::db::{Value, DB};
use super::types::{HorcruxError, Response};

pub async fn handle_set(
    socket: &mut tokio::net::TcpStream,
    db: &Arc<DB>,
    parts: Vec<&str>,
) -> Result<Response, HorcruxError> {
    // validate request
    if parts.len() != 5 && parts.len() != 6 {
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

    // store data
    let key = parts[1].to_string();
    let value = Value { flags, data: data };
    {
        db.write().await.insert(key, value);
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
        let db = db.read().await;
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
