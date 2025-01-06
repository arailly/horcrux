use clap::Parser;
use server::server::Config;

#[derive(Debug, Parser)]
struct Args {
    #[clap(long, default_value = "/var/horcrux/snapshot")]
    snapshot_path: String,

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
        args.snapshot_path.clone(),
        args.snapshot_interval_secs,
    )?;
    server::server::serve(&config).await
}
