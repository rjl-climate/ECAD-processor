use crate::analyzers::WeatherAnalyzer;
use crate::archive::{ArchiveProcessor, MultiArchiveProcessor};
use crate::cli::args::{Cli, Commands};
use crate::error::Result;
use crate::processors::IntegrityChecker;
use crate::utils::progress::ProgressReporter;
use crate::utils::{generate_default_parquet_filename, generate_default_unified_parquet_filename};
use crate::writers::{ParquetWriter, SchemaType};

pub async fn run(cli: Cli) -> Result<()> {
    // Initialize logging if verbose
    if cli.verbose {
        println!("Verbose logging enabled");
    }

    match cli.command {
        Commands::Process {
            input_archive,
            output_file,
            compression: _,
            station_id,
            validate_only,
            max_workers,
            chunk_size,
        } => {
            println!("Processing weather data from archive...");
            println!("Input archive: {}", input_archive.display());

            // Use default filename if not specified
            let output_file = output_file.unwrap_or_else(generate_default_parquet_filename);

            println!("Output file: {}", output_file.display());
            println!("Workers: {}, Chunk size: {}", max_workers, chunk_size);

            let progress = ProgressReporter::new_spinner("Inspecting archive...", false);

            // Create archive processor
            let processor = ArchiveProcessor::from_zip(&input_archive).await?;

            // Display archive metadata
            println!("\n{}", processor.metadata().display_summary());

            progress.set_message("Processing data...");

            // Process data
            let (records, integrity_report) = processor.process_data(&input_archive).await?;

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
            println!(
                "Writing {} records to Parquet file...",
                filtered_records.len()
            );

            // Create parent directory if it doesn't exist
            if let Some(parent) = output_file.parent() {
                std::fs::create_dir_all(parent)?;
            }

            let writer = ParquetWriter::new();
            writer.write_weather_records_batched(&filtered_records, &output_file, 10000)?;

            println!(
                "Successfully wrote {} weather records to {}",
                filtered_records.len(),
                output_file.display()
            );

            if !filtered_records.is_empty() {
                let sample_record = &filtered_records[0];
                println!(
                    "Sample record: Station {} on {}",
                    sample_record.station_id, sample_record.date
                );
                println!("Available metrics: {:?}", sample_record.available_metrics());
            }

            println!("Processing complete!");
        }

        Commands::ProcessDirectory {
            input_dir,
            output_file,
            compression: _compression,
            station_id,
            validate_only,
            max_workers: _max_workers,
            chunk_size: _chunk_size,
            file_pattern,
        } => {
            println!("Processing weather data from directory...");
            println!("Input directory: {}", input_dir.display());

            // Use default unified filename if not specified
            let output_file = output_file.unwrap_or_else(generate_default_unified_parquet_filename);

            println!("Output file: {}", output_file.display());

            if !file_pattern.is_empty() {
                println!("File pattern filter: '{}'", file_pattern);
            }

            let progress =
                ProgressReporter::new_spinner("Scanning directory for archives...", false);

            // Create multi-archive processor
            let pattern = if file_pattern.is_empty() {
                None
            } else {
                Some(file_pattern.as_str())
            };
            let processor = MultiArchiveProcessor::from_directory(&input_dir, pattern, 4).await?;

            // Display archive summary
            println!("\n{}", processor.get_summary());

            progress.set_message("Processing all archives...");

            // Process unified data
            let (records, integrity_report, composition) =
                processor.process_unified_data(station_id).await?;

            progress.finish_with_message(&format!("Processed {} unified records", records.len()));

            // Print integrity report
            let checker = IntegrityChecker::new();
            println!("\n{}", checker.generate_summary(&integrity_report));

            if validate_only {
                println!("Validation complete - no output file written");
                return Ok(());
            }

            // Filter records if needed (already done in processor, but for consistency)
            let filtered_records = records;

            if filtered_records.is_empty() {
                println!("No records to write");
                return Ok(());
            }

            // Write to Parquet
            println!(
                "Writing {} unified records to Parquet file...",
                filtered_records.len()
            );

            // Create parent directory if it doesn't exist
            if let Some(parent) = output_file.parent() {
                std::fs::create_dir_all(parent)?;
            }

            let writer = ParquetWriter::new();
            writer.write_weather_records_batched(&filtered_records, &output_file, 10000)?;

            println!(
                "Successfully wrote {} unified weather records to {}",
                filtered_records.len(),
                output_file.display()
            );

            // Display dataset composition based on actual data
            println!("Dataset Composition:");
            println!("  Metrics in Parquet: {:?}", composition.available_metrics);
            println!("  Total records: {}", composition.total_records);

            println!("Metric Coverage:");
            if composition.records_with_temperature > 0 {
                println!(
                    "  Temperature: {}/{} ({:.1}%)",
                    composition.records_with_temperature,
                    composition.total_records,
                    composition.records_with_temperature as f32 / composition.total_records as f32
                        * 100.0
                );
            }
            if composition.records_with_precipitation > 0 {
                println!(
                    "  Precipitation: {}/{} ({:.1}%)",
                    composition.records_with_precipitation,
                    composition.total_records,
                    composition.records_with_precipitation as f32
                        / composition.total_records as f32
                        * 100.0
                );
            }
            if composition.records_with_wind_speed > 0 {
                println!(
                    "  Wind Speed: {}/{} ({:.1}%)",
                    composition.records_with_wind_speed,
                    composition.total_records,
                    composition.records_with_wind_speed as f32 / composition.total_records as f32
                        * 100.0
                );
            }

            println!("Unified processing complete!");
        }

        Commands::Validate {
            input_archive,
            max_workers: _,
        } => {
            println!("Validating weather data from archive...");
            println!("Input archive: {}", input_archive.display());

            let progress = ProgressReporter::new_spinner("Inspecting archive...", false);

            // Create archive processor
            let processor = ArchiveProcessor::from_zip(&input_archive).await?;

            // Display archive metadata
            println!("\n{}", processor.metadata().display_summary());

            progress.set_message("Validating data...");

            let (_records, integrity_report) = processor.process_data(&input_archive).await?;

            progress.finish_with_message("Validation complete");

            let checker = IntegrityChecker::new();
            println!("\n{}", checker.generate_summary(&integrity_report));

            if integrity_report.temperature_violations.is_empty() {
                println!("✅ All data passed validation checks");
            } else {
                println!(
                    "⚠️  Found {} validation issues",
                    integrity_report.temperature_violations.len()
                );
            }
        }

        Commands::Info {
            file,
            sample,
            analysis_limit,
        } => {
            println!("Analyzing Parquet file: {}", file.display());

            // Get basic file info
            let writer = ParquetWriter::new();
            let file_info = writer.get_file_info(&file)?;

            // Detect schema type
            let schema_type = writer.detect_schema_type(&file)?;
            println!("Schema Type: {:?}", schema_type);

            // Show file info
            println!("\nFile Details:");
            println!("{}", file_info.summary());

            // Handle analysis based on schema type
            match schema_type {
                SchemaType::ConsolidatedRecord => {
                    // Use old analyzer for consolidated records
                    let analyzer = WeatherAnalyzer::new();
                    let weather_stats =
                        analyzer.analyze_parquet_with_limit(&file, analysis_limit)?;
                    println!("\n{}", weather_stats.detailed_summary());

                    // Show sample data if requested
                    if sample > 0 {
                        println!("\nSample Records (showing {} records):", sample);
                        match writer.read_sample_records(&file, sample) {
                            Ok(records) => {
                                for (i, record) in records.iter().take(sample).enumerate() {
                                    println!(
                                        "{}. {} on {}: min={:.1}°C, avg={:.1}°C, max={:.1}°C ({})",
                                        i + 1,
                                        record.station_name,
                                        record.date,
                                        record.min_temp,
                                        record.avg_temp,
                                        record.max_temp,
                                        record.quality_flags
                                    );
                                }
                            }
                            Err(e) => println!("Error reading sample data: {}", e),
                        }
                    }
                }
                SchemaType::WeatherRecord => {
                    // Use comprehensive weather dataset analysis
                    match writer.analyze_weather_dataset(&file, sample) {
                        Ok(dataset_summary) => {
                            println!("{}", dataset_summary.display_comprehensive_summary());
                        }
                        Err(e) => {
                            println!("Error analyzing weather dataset: {}", e);

                            // Fallback to basic sample display
                            if sample > 0 {
                                println!(
                                    "\nFallback: Sample Weather Records (showing {} records):",
                                    sample
                                );
                                match writer.read_sample_weather_records(&file, sample) {
                                    Ok(records) => {
                                        for (i, record) in records.iter().take(sample).enumerate() {
                                            let mut metrics = Vec::new();

                                            // Build temperature display
                                            let temp_parts: Vec<String> = [
                                                record.temp_min.map(|t| format!("min={:.1}°C", t)),
                                                record.temp_avg.map(|t| format!("avg={:.1}°C", t)),
                                                record.temp_max.map(|t| format!("max={:.1}°C", t)),
                                            ]
                                            .into_iter()
                                            .flatten()
                                            .collect();

                                            if !temp_parts.is_empty() {
                                                metrics.push(format!(
                                                    "temp({})",
                                                    temp_parts.join(", ")
                                                ));
                                            }

                                            if let Some(precip) = record.precipitation {
                                                metrics.push(format!("precip={:.1}mm", precip));
                                            }

                                            if let Some(wind) = record.wind_speed {
                                                metrics.push(format!("wind={:.1}m/s", wind));
                                            }

                                            let metrics_str = if metrics.is_empty() {
                                                "no data".to_string()
                                            } else {
                                                metrics.join(", ")
                                            };

                                            println!(
                                                "{}. {} on {}: {}",
                                                i + 1,
                                                record.station_name,
                                                record.date,
                                                metrics_str
                                            );
                                        }
                                    }
                                    Err(e) => println!("Error reading sample data: {}", e),
                                }
                            }
                        }
                    }
                }
                SchemaType::Unknown => {
                    println!("\nUnknown schema type. Cannot analyze this Parquet file format.");
                }
            }
        }
    }

    Ok(())
}
