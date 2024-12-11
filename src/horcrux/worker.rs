use crossbeam_channel::{bounded, unbounded, Receiver, Sender};

use super::{
    db::{Value, DB},
    snapshot::take_snapshot,
};

#[derive(Debug)]
pub enum Request {
    Set { key: String, value: Value },
    Get { key: String },
    Snapshot { wait: bool },
}

#[derive(Debug)]
pub enum Response {
    Stored,
    Value(Option<Value>),
    SnapshotAccepted,
    SnapshotFinished,
}

pub struct JobQueue {
    request_sender: Sender<(Request, Sender<Response>)>,
    request_receiver: Receiver<(Request, Sender<Response>)>,
}

impl JobQueue {
    pub fn new() -> Self {
        let (req_tx, req_rx) = unbounded();
        JobQueue {
            request_sender: req_tx,
            request_receiver: req_rx,
        }
    }

    pub fn send_request(&self, req: Request) -> Receiver<Response> {
        let (res_tx, res_rx) = bounded(1);
        self.request_sender.send((req, res_tx)).unwrap();
        res_rx
    }
}

impl Clone for JobQueue {
    fn clone(&self) -> Self {
        JobQueue {
            request_sender: self.request_sender.clone(),
            request_receiver: self.request_receiver.clone(),
        }
    }
}

pub struct Worker {
    id: usize,
    job_queue: JobQueue,
    db: DB,
    snapshot_dir: String,
}

impl Worker {
    pub fn new(id: usize, job_queue: JobQueue, db: DB, snapshot_dir: String) -> Self {
        Worker {
            id: id,
            job_queue,
            db: db,
            snapshot_dir: snapshot_dir,
        }
    }

    pub fn run(&mut self) {
        loop {
            let (req, res_tx) = self.job_queue.request_receiver.recv().unwrap();
            match req {
                Request::Set { key, value } => {
                    self.db.insert(key, value);
                    res_tx.send(Response::Stored).unwrap();
                }
                Request::Get { key } => {
                    let res = self.db.get(&key).cloned();
                    res_tx.send(Response::Value(res)).unwrap();
                }
                Request::Snapshot { wait } => {
                    let res = if wait {
                        Response::SnapshotFinished
                    } else {
                        Response::SnapshotAccepted
                    };
                    take_snapshot(&self.db, &self.snapshot_dir, &self.id.to_string(), wait);
                    res_tx.send(res).unwrap();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::thread;

    #[tokio::test]
    async fn test_worker_set_and_get() {
        let job_queue = JobQueue::new();
        let mut worker = Worker::new(0, job_queue.clone(), HashMap::new(), "/tmp".to_string());

        // Start the worker in a separate task
        thread::spawn(move || {
            worker.run();
        });

        // Test set method
        let key = "key1".to_string();
        let value = Value {
            flags: 0,
            data: "value1".to_string(),
        };
        let _ = job_queue
            .send_request(Request::Set {
                key: key.clone(),
                value: value.clone(),
            })
            .recv()
            .unwrap();

        // Test get method
        let get_res = job_queue
            .send_request(Request::Get { key: key.clone() })
            .recv()
            .unwrap();
        let actual = match get_res {
            Response::Value(Some(v)) => v,
            _ => panic!("Unexpected response"),
        };
        assert_eq!(actual.data, value.data);
    }
}
