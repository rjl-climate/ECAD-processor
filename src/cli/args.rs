use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "uk-temp-processor")]
#[command(about = "High-performance UK temperature data processor")]
#[command(version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    #[arg(short, long, global = true, help = "Enable verbose logging")]
    pub verbose: bool,

    #[arg(long, global = true, help = "Log file path")]
    pub log_file: Option<PathBuf>,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Process temperature data and generate Parquet output
    Process {
        #[arg(short, long, default_value = "data")]
        input_dir: PathBuf,

        #[arg(short, long, default_value = "output/temperatures.parquet")]
        output_file: PathBuf,

        #[arg(short, long, default_value = "snappy")]
        compression: String,

        #[arg(short, long)]
        station_id: Option<u32>,

        #[arg(long, default_value = "false")]
        validate_only: bool,

        #[arg(long, default_value_t = num_cpus::get())]
        max_workers: usize,

        #[arg(long, default_value = "1000")]
        chunk_size: usize,
    },

    /// Validate temperature data without processing
    Validate {
        #[arg(short, long, default_value = "data")]
        input_dir: PathBuf,

        #[arg(long, default_value_t = num_cpus::get())]
        max_workers: usize,
    },

    /// Display information about a Parquet file
    Info {
        #[arg(short, long)]
        file: PathBuf,

        #[arg(short, long, default_value = "10")]
        sample: usize,

        #[arg(
            long,
            default_value = "0",
            help = "Maximum records to analyze (0 = all records)"
        )]
        analysis_limit: usize,
    },
}
