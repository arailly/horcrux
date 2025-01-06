use db::db::DB;
use std::error::Error;
use std::thread;
use tokio::net::TcpListener;
use tokio::signal::unix::{signal, SignalKind};
use tokio::{time, time::Duration};

use super::handler::{BaseHandler, Handler, SnapshotHandler};
use super::memcache::{read_request, send_response, Request, Response};
use super::worker::{JobQueue, Worker};
use types::types::HorcruxError;

#[derive(Clone)]
pub struct Config {
    addr: String,
    snapshot_path: String,
    snapshot_interval_secs: u64,
}

impl Config {
    pub fn new(
        addr: String,
        snapshot_path: String,
        snapshot_interval_secs: u64,
    ) -> Result<Self, String> {
        if snapshot_path == "" {
            return Err("Snapshot directory cannot be empty".to_string());
        }
        if snapshot_interval_secs == 0 {
            return Err("Snapshot interval cannot be 0".to_string());
        }

        Ok(Config {
            addr,
            snapshot_path,
            snapshot_interval_secs,
        })
    }
}

pub async fn serve(config: &Config) -> Result<(), Box<dyn Error>> {
    let job_queue = JobQueue::new();
    let job_queue_for_worker = job_queue.clone();
    let config_for_worker = config.clone();

    thread::spawn(move || {
        let mut db = DB::new(config_for_worker.snapshot_path.clone());
        db.restore();

        let mut worker = Worker::new(job_queue_for_worker.clone(), db);
        worker.run();
    });

    let handler = BaseHandler::new(job_queue.clone());

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
    let handler_for_sigterm = handler.clone();
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigterm_task = tokio::spawn(async move {
        match sigterm.recv().await {
            Some(_) => {
                println!("Taking snapshot before shutting down");
                match handler_for_sigterm.snapshot(true) {
                    Ok(_) => {
                        println!("Snapshot taken successfully");
                    }
                    Err(_) => {
                        println!("Failed to take snapshot");
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
    let handler_for_interval = handler.clone();
    let interval = config.snapshot_interval_secs;
    let interval_task = tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(interval));
        // ignore the first tick, which is triggered immediately
        interval.tick().await;

        loop {
            interval.tick().await;
            println!("Start taking snapshot");
            match handler_for_interval.snapshot(false) {
                Ok(_) => {
                    println!("Snapshot taken successfully");
                }
                Err(_) => {
                    println!("Failed to take snapshot");
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
            Request::Snapshot => match handler.snapshot(false) {
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
