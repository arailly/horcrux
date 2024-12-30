use std::error::Error;
use std::thread;
use tokio::net::TcpListener;
use tokio::signal::unix::{signal, SignalKind};
use tokio::{time, time::Duration};

use db::db::DB;
use types::types::HorcruxError;

use super::handler::{Handler, ShardHandler, SnapshotHandler};
use super::memcache::{read_request, send_response, Request, Response};
use super::worker::{JobQueue, Worker};

#[derive(Clone)]
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
        let job_queue = JobQueue::new();
        job_queues.push(job_queue.clone());
        let config_for_worker = config.clone();

        thread::spawn(move || {
            let mut db = DB::new();
            let path = format!("{}/snapshot-{}", config_for_worker.snapshot_dir, i);
            match db.restore(&path) {
                Ok(_) => {
                    println!("DB restored from snapshot");
                }
                Err(err) => {
                    println!("Failed to restore DB: {}", err);
                }
            }

            let mut worker = Worker::new(job_queue.clone(), db, path);
            worker.run();
        });
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

// fn restore_db(snapshot_dir: &str, suffix: &str) -> DB {
//     let mut db = DB::new();
//     let snapshot_file = format!("{}/snapshot-{}", snapshot_dir, suffix);

//     match std::fs::read(snapshot_file) {
//         Ok(data) => {
//             println!(
//                 "{:?}: Restoring DB from snapshot",
//                 Utc::now().format("%+").to_string()
//             );

//             let mut mem = Bytes::from(data);
//             while !mem.is_empty() {
//                 let (key, value) = match get_key_value_from_bytes(&mut mem) {
//                     Ok((key, value)) => (key, value),
//                     Err(err) => {
//                         println!("{}", err);
//                         return db;
//                     }
//                 };
//                 db.insert(key, value);
//             }
//         }
//         Err(err) => match err.kind() {
//             std::io::ErrorKind::NotFound => {
//                 println!("Snapshot file not found. Starting with empty DB.");
//             }
//             _ => {
//                 println!("Failed to read snapshot file: {}", err);
//                 return db;
//             }
//         },
//     }

//     println!("{:?}: DB restored", Utc::now().format("%+").to_string());
//     db
// }

// fn get_key_value_from_bytes(mem: &mut Bytes) -> Result<(String, Value), HorcruxError> {
//     let key_len = mem.get_u8() as usize;
//     let key = String::from_utf8(mem.split_to(key_len).to_vec())
//         .map_err(|_| HorcruxError::RestoreDB("Failed to parse key from snapshot".to_string()))?;
//     let flags = mem.get_u32();
//     let data_len = mem.get_u32() as usize;
//     let data = String::from_utf8(mem.split_to(data_len).to_vec())
//         .map_err(|_| HorcruxError::RestoreDB("Failed to parse data from snapshot".to_string()))?;
//     Ok((key, Value { flags, data }))
// }

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
