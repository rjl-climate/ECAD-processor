use clap::Parser;
use ecad_processor::cli::{run, Cli};
use ecad_processor::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    run(cli).await
}
