use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "ecad-processor")]
#[command(about = "High-performance ECAD weather data processor")]
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
    /// Process weather data from zip archive
    Process {
        #[arg(short, long, help = "Input zip archive file")]
        input_archive: PathBuf,

        #[arg(
            short,
            long,
            help = "Output Parquet file path [default: ecad-weather-{YYMMDD}.parquet]"
        )]
        output_file: Option<PathBuf>,

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

    /// Process all zip files in directory and combine into unified dataset
    ProcessDirectory {
        #[arg(short, long, help = "Input directory containing zip files")]
        input_dir: PathBuf,

        #[arg(
            short,
            long,
            help = "Output unified Parquet file path [default: ecad-weather-unified-{YYMMDD}.parquet]"
        )]
        output_file: Option<PathBuf>,

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

        #[arg(
            long,
            help = "Filter to specific file pattern (e.g., 'UK_ALL_')",
            default_value = ""
        )]
        file_pattern: String,
    },

    /// Validate archive data without processing
    Validate {
        #[arg(short, long, help = "Input zip archive file")]
        input_archive: PathBuf,

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
