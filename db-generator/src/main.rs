use std::thread;

use clap::Parser;
use rand::distributions::{Alphanumeric, DistString};

use db::db::{Value, DB};
use db::snapshot::take_snapshot;
use server::handler::{SetHandler, ShardHandler, SnapshotHandler};
use server::worker::{JobQueue, Worker};

#[derive(Debug, Parser)]
struct Args {
    #[clap(long, default_value = "/var/horcrux")]
    snapshot_dir: String,

    #[clap(long, default_value = "1")]
    shards: usize,
}

fn main() {
    let args = Args::parse();

    // setup job queues and handler
    let mut job_queues = Vec::new();

    for i in 0..args.shards {
        let job_queue = JobQueue::new();
        job_queues.push(job_queue.clone());
        let snapshot_dir = args.snapshot_dir.clone();

        thread::spawn(move || {
            let db = DB::new();
            let mut worker = Worker::new(i, job_queue.clone(), db, snapshot_dir.clone());
            worker.run();
        });
    }

    let handler = ShardHandler::new(job_queues.clone());

    // initialize db with random keys and values
    let mut rng = rand::thread_rng();
    for _ in 0..1000 * 1000 * 7 {
        let key = Alphanumeric.sample_string(&mut rng, 36);
        let data = Alphanumeric.sample_string(&mut rng, 450);
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
