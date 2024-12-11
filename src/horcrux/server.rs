use bytes::{Buf, Bytes};
use chrono::Utc;
use std::collections::HashMap;
use std::error::Error;
use std::thread;
use tokio::net::TcpListener;
use tokio::signal::unix::{signal, SignalKind};
use tokio::{time, time::Duration};

use crate::horcrux::handler::ShardHandler;
use crate::horcrux::worker;
use crate::horcrux::worker::{JobQueue, Worker};

use super::db::{Value, DB};
use super::handler::Handler;
use super::memcache::{read_request, send_response, Request, Response};
use super::types::HorcruxError;

pub struct Config {
    addr: String,
    snapshot_dir: String,
    shards: usize,
    snapshot_interval_secs: u64,
}

impl Config {
    pub fn new(
        addr: String,
        snapshot_dir: String,
        shards: usize,
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
            shards,
            snapshot_interval_secs,
        })
    }
}

pub async fn serve(config: &Config) -> Result<(), Box<dyn Error>> {
    let mut job_queues = Vec::new();

    for i in 0..config.shards {
        let db = restore_db(
            config.snapshot_dir.as_str(),
            i.to_string().as_str()
        ).await?;
        let job_queue = JobQueue::new();
        let mut worker = Worker::new(
            i,
            job_queue.clone(),
            db,
            config.snapshot_dir.clone(),
        );
        thread::spawn(move || worker.run());

        job_queues.push(job_queue);
    }

    let handler = ShardHandler::new(job_queues.clone());

    let listener = TcpListener::bind(&config.addr).await?;
    println!("Server running on {}", &config.addr);

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
    let job_queues_for_sigterm = job_queues.clone();
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigterm_task = tokio::spawn(async move {
        match sigterm.recv().await {
            Some(_) => {
                println!("Taking snapshot before shutting down");

                // take snapshot for each shard parallelly
                let receivers = job_queues_for_sigterm
                    .iter()
                    .map(|job_queue| job_queue.send_request(worker::Request::Snapshot { wait: true }))
                    .collect::<Vec<_>>();

                // wait for all snapshots to finish
                for receiver in receivers {
                    let result = receiver.recv();
                    match result {
                        Ok(worker::Response::SnapshotFinished) => {
                            println!("Snapshot taken successfully");
                        }
                        _ => {
                            println!("Failed to take snapshot");
                        }
                    }
                }
                println!("Shutting down...");
            }
            None => {
                println!("Failed to listen for SIGTERM");
            }
        }
    });

    // take snapshot at regular intervals
    let job_queues_for_interval = job_queues.clone();
    let interval = config.snapshot_interval_secs;
    let interval_task = tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(interval));
        // ignore the first tick, which is triggered immediately
        interval.tick().await;

        loop {
            interval.tick().await;
            println!("Start taking snapshot");

            // take snapshot for each shard parallelly
            let receivers = job_queues_for_interval
                .iter()
                .map(|job_queue| job_queue.send_request(worker::Request::Snapshot { wait: true }))
                .collect::<Vec<_>>();

            // wait for all snapshot requests to accept
            for receiver in receivers {
                let result = receiver.recv();
                match result {
                    Ok(worker::Response::SnapshotAccepted) => {
                        println!("Snapshot request has been sent successfully");
                    }
                    _ => {
                        println!("Failed to take snapshot");
                    }
                }
            }
            println!("Finished to send snapshot request");
        }
    });

    // main loop
    loop {
        tokio::select! {
            Ok((socket, _)) = listener.accept() => {
                let h = handler.clone();

                tokio::spawn(async move {
                    process(socket, h).await;
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

async fn restore_db(snapshot_dir: &str, suffix: &str) -> Result<DB, Box<dyn Error>> {
    let mut db = HashMap::with_capacity(1000);
    let snapshot_file = format!("{}/snapshot-{}", snapshot_dir, suffix);

    match tokio::fs::read(snapshot_file).await {
        Ok(data) => {
            println!(
                "{:?}: Restoring DB from snapshot",
                Utc::now().format("%+").to_string()
            );

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

pub async fn process<T: Handler>(mut socket: tokio::net::TcpStream, handler: T) {
    loop {
        let req = match read_request(&mut socket).await {
            Ok(req) => req,
            Err(err) => match err {
                HorcruxError::Connection(s) => {
                    println!("Connection error: {}", s);
                    return;
                }
                HorcruxError::ParseRequest(s) => {
                    println!("Failed to parse request: {}", s);
                    if send_response(&mut socket, Response::Error).await.is_err() {
                        println!("Failed to send response");
                        return;
                    }
                    continue;
                }
                HorcruxError::Ignorable => {
                    continue;
                }
                _ => {
                    return;
                }
            },
        };

        match req {
            Request::Set {
                key,
                flags,
                _exptime,
                data,
            } => match handler.set(key, flags, _exptime, data) {
                Ok(_) => {
                    if send_response(&mut socket, Response::Stored).await.is_err() {
                        println!("Failed to send response");
                        return;
                    }
                }
                Err(_) => {
                    println!("Failed to handle set request");
                    return;
                }
            },
            Request::Get { key } => {
                let val = handler.get(&key);
                if send_response(&mut socket, Response::Value(key, val))
                    .await
                    .is_err()
                {
                    println!("Failed to send response");
                    return;
                }
            }
            Request::Snapshot => match handler.snapshot() {
                Ok(_) => {
                    if send_response(&mut socket, Response::SnapshotFinished)
                        .await
                        .is_err()
                    {
                        println!("Failed to send response");
                        return;
                    }
                }
                Err(_) => {
                    if send_response(&mut socket, Response::Error).await.is_err() {
                        println!("Failed to send response");
                        return;
                    }
                }
            },
        }
    }
}
