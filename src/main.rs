use crate::horcrux::server::Config;
use clap::Parser;

mod horcrux;

#[derive(Debug, Parser)]
struct Args {
    #[clap(long, default_value = "/var/horcrux")]
    snapshot_dir: String,

    #[clap(long, default_value = "1")]
    shards: usize,

    #[clap(long, default_value = "180")]
    snapshot_interval_secs: u64,

    #[clap(long, default_value = "11211")]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let address = format!("0.0.0.0:{}", args.port);
    let config = Config::new(
        address,
        args.snapshot_dir.clone(),
        args.shards,
        args.snapshot_interval_secs,
    )?;
    horcrux::serve(&config).await
}
