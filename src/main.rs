use crate::horcrux::server::Config;
use clap::Parser;

mod horcrux;

#[derive(Debug, Parser)]
struct Args {
    #[clap(long, default_value = "/var/horcrux")]
    snapshot_dir: String,

    #[clap(long, default_value = "180")]
    snapshot_interval_secs: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let config = Config::new(
        "0.0.0.0:11211".to_string(),
        args.snapshot_dir.clone(),
        args.snapshot_interval_secs,
    )?;
    horcrux::serve(&config).await
}
