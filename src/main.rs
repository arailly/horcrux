use clap::Parser;

mod memcached;

#[derive(Debug, Parser)]
struct Args {
    #[clap(default_value = "/var/horcrux")]
    snapshot_dir: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if args.snapshot_dir.is_empty() {
        eprintln!("Snapshot directory cannot be empty");
        std::process::exit(1);
    }
    memcached::serve("0.0.0.0:11211", &args.snapshot_dir).await
}
