use crate::cli::args::{Cli, Commands};
use crate::error::Result;
use crate::processors::{ParallelProcessor, IntegrityChecker};
use crate::utils::progress::ProgressReporter;
use crate::writers::ParquetWriter;

pub async fn run(cli: Cli) -> Result<()> {
    // Initialize logging if verbose
    if cli.verbose {
        println!("Verbose logging enabled");
    }
    
    match cli.command {
        Commands::Process { 
            input_dir, 
            output_file, 
            compression,
            station_id,
            validate_only,
            max_workers,
            chunk_size,
        } => {
            println!("Processing temperature data...");
            println!("Input directory: {}", input_dir.display());
            println!("Output file: {}", output_file.display());
            println!("Workers: {}, Chunk size: {}", max_workers, chunk_size);
            
            let progress = ProgressReporter::new_spinner("Processing data...", false);
            
            // Configure processor
            let processor = ParallelProcessor::new(max_workers)
                .with_chunk_size(chunk_size);
            
            // Process data
            let (records, integrity_report) = processor.process_all_data(&input_dir, Some(&progress)).await?;
            
            progress.finish_with_message(&format!("Processed {} records", records.len()));
            
            // Print integrity report
            let checker = IntegrityChecker::new();
            println!("\n{}", checker.generate_summary(&integrity_report));
            
            if validate_only {
                println!("Validation complete - no output file written");
                return Ok(());
            }
            
            // Filter by station if specified
            let filtered_records = if let Some(id) = station_id {
                records.into_iter().filter(|r| r.station_id == id).collect()
            } else {
                records
            };
            
            if filtered_records.is_empty() {
                println!("No records to write");
                return Ok(());
            }
            
            // Write to Parquet
            println!("Writing {} records to Parquet file...", filtered_records.len());
            let writer = ParquetWriter::new().with_compression(&compression)?;
            
            // Create output directory if it doesn't exist
            if let Some(parent) = output_file.parent() {
                std::fs::create_dir_all(parent)?;
            }
            
            writer.write_records_batched(&filtered_records, &output_file, chunk_size)?;
            
            // Get file info
            let file_info = writer.get_file_info(&output_file)?;
            println!("\n{}", file_info.summary());
            
            println!("Processing complete!");
        }
        
        Commands::Validate { input_dir, max_workers } => {
            println!("Validating temperature data...");
            println!("Input directory: {}", input_dir.display());
            
            let progress = ProgressReporter::new_spinner("Validating data...", false);
            
            let processor = ParallelProcessor::new(max_workers)
                .with_strict_validation(true);
            
            let (_records, integrity_report) = processor.process_all_data(&input_dir, Some(&progress)).await?;
            
            progress.finish_with_message("Validation complete");
            
            let checker = IntegrityChecker::new();
            println!("\n{}", checker.generate_summary(&integrity_report));
            
            if integrity_report.temperature_violations.is_empty() {
                println!("✅ All data passed validation checks");
            } else {
                println!("⚠️  Found {} validation issues", integrity_report.temperature_violations.len());
            }
        }
        
        Commands::Info { file, sample: _ } => {
            println!("Analyzing Parquet file: {}", file.display());
            
            let writer = ParquetWriter::new();
            let file_info = writer.get_file_info(&file)?;
            
            println!("\n{}", file_info.summary());
            
            // TODO: Add sample data reading
            println!("\nSample functionality not yet implemented");
        }
    }
    
    Ok(())
}