use bytes::{Buf, Bytes};
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use super::db::{Value, DB};
use super::handler::{handle_get, handle_set};
use super::snapshot_handler::handle_snapshot;
use super::types::{MemcachedError, Response};

pub async fn serve(addr: &str, snapshot_dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(addr).await?;
    println!("Server running on {}", addr);

    let db = restore_db(snapshot_dir).await?;

    loop {
        let (socket, _) = listener.accept().await?;
        let db = db.clone();
        let snapshot_dir = snapshot_dir.to_string();

        tokio::spawn(async move {
            process(socket, db, &snapshot_dir).await;
        });
    }
}

async fn restore_db(snapshot_dir: &str) -> Result<Arc<DB>, Box<dyn Error>> {
    let db = Arc::new(Mutex::new(HashMap::new()));
    let snapshot_file = format!("{}/snapshot", snapshot_dir);

    match tokio::fs::read(snapshot_file).await {
        Ok(data) => {
            println!("Restoring DB from snapshot");

            let mut db = db.lock().await;
            let mut mem = Bytes::from(data);
            while !mem.is_empty() {
                let key_len = mem.get_u8() as usize;
                let key = String::from_utf8(mem.split_to(key_len).to_vec())?;
                let flags = mem.get_u32();
                let data_len = mem.get_u32() as usize;
                let data = String::from_utf8(mem.split_to(data_len).to_vec())?;
                db.insert(key, Value { flags, data });
            }
        }
        Err(err) => match err.kind() {
            std::io::ErrorKind::NotFound => {
                println!("Snapshot file not found. Starting with empty DB.");
            }
            _ => {
                return Err(
                    MemcachedError::RestoreDB("Failed to read snapshot file".to_string()).into(),
                );
            }
        },
    }

    println!("DB restored");
    Ok(db)
}

pub async fn process(mut socket: tokio::net::TcpStream, db: Arc<DB>, snapshot_dir: &str) {
    loop {
        // read request line
        let mut buf = vec![0; 1024];
        let request: String;
        match socket.read(&mut buf).await {
            Ok(n) if n == 0 => {
                println!("Connection closed");
                return;
            }
            Ok(n) => {
                request = String::from_utf8_lossy(&buf[..n]).to_string();
            }
            Err(_) => {
                println!("Failed to read from socket");
                return;
            }
        }

        // process request
        let parts: Vec<&str> = request.trim().split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }
        let command = parts[0].to_lowercase();
        match command.as_str() {
            "set" => match handle_set(&mut socket, &db, parts).await {
                Ok(response) => {
                    if send_response(&mut socket, response).await.is_err() {
                        println!("Failed to send response");
                        return;
                    }
                }
                Err(_) => {
                    println!("Failed to handle set request");
                    return;
                }
            },
            "get" => {
                let response = handle_get(&db, parts).await;
                if send_response(&mut socket, response).await.is_err() {
                    println!("Failed to send response");
                    return;
                }
            }
            "snapshot" => {
                handle_snapshot(&db, snapshot_dir).await;
                if send_response(&mut socket, Response::Accepted)
                    .await
                    .is_err()
                {
                    println!("Failed to send response");
                    return;
                }
            }
            "quit" => {
                return;
            }
            _ => {
                if send_response(&mut socket, Response::Error).await.is_err() {
                    println!("Failed to send response");
                    return;
                }
            }
        }
    }
}

async fn send_response(
    socket: &mut tokio::net::TcpStream,
    response: Response,
) -> Result<(), MemcachedError> {
    if socket.write_all(response.as_bytes()).await.is_err() {
        println!("Failed to send response");
        return Err(MemcachedError::Connection(
            "Failed to send response".to_string(),
        ));
    }
    Ok(())
}
