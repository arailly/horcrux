use crate::memcached::server::ServerConfig;
use clap::Parser;

mod memcached;

#[derive(Debug, Parser)]
struct Args {
    #[clap(default_value = "/var/horcrux")]
    snapshot_dir: String,

    #[clap(default_value = "180")]
    snapshot_interval_secs: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let config = ServerConfig::new(
        "0.0.0.0:11211".to_string(),
        args.snapshot_dir.clone(),
        args.snapshot_interval_secs,
    )?;
    memcached::serve(&config).await
}
