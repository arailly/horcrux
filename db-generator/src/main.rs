use std::thread;

use clap::Parser;
use rand::distributions::{Alphanumeric, DistString};

use db::db::DB;
use server::handler::{BaseHandler, SetHandler, SnapshotHandler};
use server::worker::{JobQueue, Worker};

#[derive(Debug, Parser)]
struct Args {
    #[clap(long, default_value = "/tmp/snapshot")]
    snapshot_path: String,

    #[clap(long, default_value = "1000000")]
    db_len: usize,

    #[clap(long, default_value = "36")]
    key_len: usize,

    #[clap(long, default_value = "450")]
    data_len: usize,
}

fn main() {
    let args = Args::parse();

    // setup job queue and handler
    let job_queue = JobQueue::new();
    let db = DB::new(args.snapshot_path.clone());

    let job_queue_for_worker = job_queue.clone();
    thread::spawn(move || {
        let mut worker = Worker::new(job_queue_for_worker, db);
        worker.run();
    });

    let handler = BaseHandler::new(job_queue.clone());

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
