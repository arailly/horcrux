use bytes::{Buf, Bytes};
use chrono::Utc;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::Mutex;
use tokio::{time, time::Duration};

use super::db::{Value, DB};
use super::handler::{handle_get, handle_set};
use super::snapshot_handler::handle_snapshot;
use super::types::{HorcruxError, Response};

pub struct Config {
    addr: String,
    snapshot_dir: String,
    snapshot_interval_secs: u64,
}

impl Config {
    pub fn new(
        addr: String,
        snapshot_dir: String,
        snapshot_interval_secs: u64,
    ) -> Result<Self, String> {
        if snapshot_dir == "" {
            return Err("Snapshot directory cannot be empty".to_string());
        }
        if snapshot_interval_secs == 0 {
            return Err("Snapshot interval cannot be 0".to_string());
        }

        Ok(Config {
            addr,
            snapshot_dir,
            snapshot_interval_secs,
        })
    }
}

pub async fn serve(config: &Config) -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind(&config.addr).await?;
    println!("Server running on {}", &config.addr);

    let db = restore_db(&config.snapshot_dir).await?;

    // SIGINT handler
    let mut sigint_task = tokio::spawn(async {
        let mut sigint = signal(SignalKind::interrupt()).unwrap();
        match sigint.recv().await {
            Some(_) => {
                println!("Shutting down...");
            }
            None => {
                println!("Failed to listen for SIGINT");
            }
        }
    });

    // SIGTERM handler
    let snapshot_dir_for_signal = config.snapshot_dir.clone();
    let db_for_signal = db.clone();
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigterm_task = tokio::spawn(async move {
        match sigterm.recv().await {
            Some(_) => {
                println!("Taking snapshot before shutting down");
                handle_snapshot(&db_for_signal, &snapshot_dir_for_signal).await;
                println!("Shutting down...");
            }
            None => {
                println!("Failed to listen for SIGTERM");
            }
        }
    });

    // take snapshot at regular intervals
    let interval = config.snapshot_interval_secs;
    let snapshot_dir_for_interval = config.snapshot_dir.clone();
    let db_for_interval = db.clone();
    let interval_task = tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(interval));
        // ignore the first tick, which is triggered immediately
        interval.tick().await;

        loop {
            interval.tick().await;
            println!("Start taking snapshot");
            handle_snapshot(&db_for_interval, &snapshot_dir_for_interval).await;
            println!("Finished taking snapshot");
        }
    });

    // main loop
    loop {
        tokio::select! {
            Ok((socket, _)) = listener.accept() => {
                let db = db.clone();
                let snapshot_dir = config.snapshot_dir.clone();

                tokio::spawn(async move {
                    process(socket, db, &snapshot_dir).await;
                });
            }
            _ = &mut sigterm_task => {
                break;
            }
            _ = &mut sigint_task => {
                break;
            }
        }
    }

    interval_task.abort();
    Ok(())
}

async fn restore_db(snapshot_dir: &str) -> Result<Arc<DB>, Box<dyn Error>> {
    let db = Arc::new(Mutex::new(HashMap::with_capacity(1000)));
    let snapshot_file = format!("{}/snapshot", snapshot_dir);

    match tokio::fs::read(snapshot_file).await {
        Ok(data) => {
            println!(
                "{:?}: Restoring DB from snapshot",
                Utc::now().format("%+").to_string()
            );

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
                    HorcruxError::RestoreDB("Failed to read snapshot file".to_string()).into(),
                );
            }
        },
    }

    println!("{:?}: DB restored", Utc::now().format("%+").to_string());
    Ok(db)
}

pub async fn process(mut socket: tokio::net::TcpStream, db: Arc<DB>, snapshot_dir: &str) {
    loop {
        // read request line
        let mut buf = vec![0; 4096];
        let request: String;
        match socket.read(&mut buf).await {
            Ok(n) if n == 0 => {
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
                if send_response(&mut socket, Response::SnapshotFinished)
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
) -> Result<(), HorcruxError> {
    if socket.write_all(response.as_bytes()).await.is_err() {
        println!("Failed to send response");
        return Err(HorcruxError::Connection(
            "Failed to send response".to_string(),
        ));
    }
    Ok(())
}
