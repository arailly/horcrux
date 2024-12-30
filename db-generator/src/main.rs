use std::thread;

use clap::Parser;
use rand::distributions::{Alphanumeric, DistString};

use db::db::DB;
use server::handler::{SetHandler, ShardHandler, SnapshotHandler};
use server::worker::{JobQueue, Worker};

#[derive(Debug, Parser)]
struct Args {
    #[clap(long, default_value = "/tmp/horcrux")]
    snapshot_dir: String,

    #[clap(long, default_value = "1")]
    shards: usize,

    #[clap(long, default_value = "1000000")]
    db_len: usize,

    #[clap(long, default_value = "36")]
    key_len: usize,

    #[clap(long, default_value = "450")]
    data_len: usize,
}

fn main() {
    let args = Args::parse();

    // setup job queues and handler
    let mut job_queues = Vec::new();

    for i in 0..args.shards {
        let job_queue = JobQueue::new();
        job_queues.push(job_queue.clone());
        let snapshot_path = format!("{}/snapshot-{}", args.snapshot_dir.clone(), i);

        thread::spawn(move || {
            let db = DB::new();
            let mut worker = Worker::new(job_queue.clone(), db, snapshot_path);
            worker.run();
        });
    }

    let handler = ShardHandler::new(job_queues.clone());

    // initialize db with random keys and values
    let mut rng = rand::thread_rng();
    for _ in 0..args.db_len {
        let key = Alphanumeric.sample_string(&mut rng, args.key_len);
        let data = Alphanumeric.sample_string(&mut rng, args.data_len);
        match handler.set(key, 0, 0, data) {
            Ok(_) => {}
            Err(_) => {
                println!("Failed to set key");
            }
        }
    }
    println!("DB initialized with random keys and values");

    // take snapshot
    match handler.snapshot(true) {
        Ok(_) => {}
        Err(_) => {
            println!("Failed to take snapshot");
        }
    }
}
