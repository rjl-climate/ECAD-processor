use crate::archive::{ArchiveInspector, ArchiveProcessor, WeatherMetric};
use crate::error::{ProcessingError, Result};
use crate::models::WeatherRecord;
use crate::processors::IntegrityReport;
use chrono::NaiveDate;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use tokio::task::JoinSet;

#[derive(Debug, Clone)]
pub struct ArchiveInfo {
    pub path: PathBuf,
    pub metrics: Vec<WeatherMetric>,
    pub station_count: usize,
    pub file_count: usize,
}

#[derive(Debug, Clone)]
pub struct DatasetComposition {
    pub total_records: usize,
    pub records_with_temperature: usize,
    pub records_with_precipitation: usize,
    pub records_with_wind_speed: usize,
    pub available_metrics: Vec<String>,
}

pub struct MultiArchiveProcessor {
    archives: Vec<ArchiveInfo>,
    max_workers: usize,
}

impl MultiArchiveProcessor {
    /// Create a new processor by scanning a directory for zip files
    pub async fn from_directory(
        dir_path: &Path,
        file_pattern: Option<&str>,
        max_workers: usize,
    ) -> Result<Self> {
        if !dir_path.is_dir() {
            return Err(ProcessingError::InvalidFormat(format!(
                "Path is not a directory: {}",
                dir_path.display()
            )));
        }

        let mut archives = Vec::new();

        // Read directory entries
        let entries = fs::read_dir(dir_path)?;

        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            // Filter for zip files
            if !path.is_file() || path.extension().map_or(true, |ext| ext != "zip") {
                continue;
            }

            // Apply file pattern filter if specified
            if let Some(pattern) = file_pattern {
                if !pattern.is_empty() {
                    if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                        if !filename.contains(pattern) {
                            continue;
                        }
                    } else {
                        continue;
                    }
                }
            }

            println!("Inspecting archive: {}", path.display());

            // Inspect the archive to get metadata
            match ArchiveInspector::inspect_zip(&path) {
                Ok(metadata) => {
                    let archive_info = ArchiveInfo {
                        path: path.clone(),
                        metrics: metadata.metrics,
                        station_count: metadata.station_count,
                        file_count: metadata.total_files,
                    };

                    println!(
                        "  → Found {} metrics across {} stations in {} files",
                        archive_info.metrics.len(),
                        archive_info.station_count,
                        archive_info.file_count
                    );

                    archives.push(archive_info);
                }
                Err(e) => {
                    println!("  → Warning: Failed to inspect {}: {}", path.display(), e);
                    continue;
                }
            }
        }

        if archives.is_empty() {
            return Err(ProcessingError::InvalidFormat(format!(
                "No valid zip files found in directory: {}",
                dir_path.display()
            )));
        }

        // Sort archives by filename for consistent processing order
        archives.sort_by(|a, b| a.path.file_name().cmp(&b.path.file_name()));

        println!("\nFound {} archives to process:", archives.len());
        for archive in &archives {
            println!(
                "  • {} ({} metrics)",
                archive.path.file_name().unwrap().to_string_lossy(),
                archive.metrics.len()
            );
        }

        Ok(Self {
            archives,
            max_workers,
        })
    }

    /// Get summary of all discovered archives
    pub fn get_summary(&self) -> String {
        let total_files = self.archives.iter().map(|a| a.file_count).sum::<usize>();
        let total_metrics: Vec<_> = self.archives.iter().flat_map(|a| &a.metrics).collect();
        let unique_metrics: std::collections::HashSet<_> = total_metrics.iter().collect();

        let mut summary = format!(
            "Multi-Archive Summary:\n  Archives: {}\n  Total Files: {}\n  Unique Metrics: {}\n",
            self.archives.len(),
            total_files,
            unique_metrics.len()
        );

        summary.push_str("  Available Metrics:\n");
        for metric in unique_metrics {
            let archive_count = self
                .archives
                .iter()
                .filter(|a| a.metrics.contains(metric))
                .count();
            summary.push_str(&format!("    {}: {} archives\n", metric, archive_count));
        }

        summary
    }

    /// Process all archives and merge data into unified records
    pub async fn process_unified_data(
        mut self,
        station_filter: Option<u32>,
    ) -> Result<(Vec<WeatherRecord>, IntegrityReport, DatasetComposition)> {
        println!(
            "Processing {} archives with up to {} workers...",
            self.archives.len(),
            self.max_workers
        );

        // Process archives concurrently
        let mut join_set = JoinSet::new();
        let archives = std::mem::take(&mut self.archives);

        for archive_info in archives {
            join_set.spawn(async move {
                println!("Starting processing: {}", archive_info.path.display());

                let processor = ArchiveProcessor::from_zip(&archive_info.path).await?;
                let (records, report) = processor.process_data(&archive_info.path).await?;

                // Filter by station if specified
                let filtered_records = if let Some(station_id) = station_filter {
                    records
                        .into_iter()
                        .filter(|r| r.station_id == station_id)
                        .collect()
                } else {
                    records
                };

                println!(
                    "Completed processing: {} ({} records)",
                    archive_info.path.file_name().unwrap().to_string_lossy(),
                    filtered_records.len()
                );

                Ok::<(Vec<WeatherRecord>, IntegrityReport), ProcessingError>((
                    filtered_records,
                    report,
                ))
            });
        }

        // Collect all results
        let mut all_records_by_archive = Vec::new();
        let mut all_reports = Vec::new();

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok((records, report))) => {
                    all_records_by_archive.push(records);
                    all_reports.push(report);
                }
                Ok(Err(e)) => return Err(e),
                Err(e) => return Err(ProcessingError::TaskJoin(e)),
            }
        }

        println!("All archives processed. Merging unified records...");

        // Merge records by station and date
        let (unified_records, composition) = self.merge_records_by_key(all_records_by_archive)?;

        println!("Created {} unified weather records", unified_records.len());

        // Combine integrity reports
        let combined_report = self.combine_integrity_reports(all_reports);

        Ok((unified_records, combined_report, composition))
    }

    /// Merge records from multiple archives by (station_id, date) key
    fn merge_records_by_key(
        &self,
        records_by_archive: Vec<Vec<WeatherRecord>>,
    ) -> Result<(Vec<WeatherRecord>, DatasetComposition)> {
        let mut record_map: HashMap<(u32, NaiveDate), WeatherRecord> = HashMap::new();

        for archive_records in records_by_archive {
            for record in archive_records {
                let key = (record.station_id, record.date);

                match record_map.get_mut(&key) {
                    Some(existing) => {
                        // Merge with existing record
                        Self::merge_weather_records(existing, record)?;
                    }
                    None => {
                        // Add new record
                        record_map.insert(key, record);
                    }
                }
            }
        }

        // Convert to vector and ensure all records have physical validation
        let mut unified_records: Vec<_> = record_map.into_values().collect();

        // Ensure all records have physical validation performed
        for record in &mut unified_records {
            record.perform_physical_validation();
        }

        unified_records.sort_by(|a, b| {
            a.station_id
                .cmp(&b.station_id)
                .then_with(|| a.date.cmp(&b.date))
        });

        // Calculate dataset composition
        let total_records = unified_records.len();
        let records_with_temperature = unified_records
            .iter()
            .filter(|r| r.has_temperature_data())
            .count();
        let records_with_precipitation = unified_records
            .iter()
            .filter(|r| r.has_precipitation())
            .count();
        let records_with_wind_speed = unified_records
            .iter()
            .filter(|r| r.has_wind_speed())
            .count();

        let mut available_metrics = Vec::new();
        if records_with_temperature > 0 {
            available_metrics.push("temperature".to_string());
        }
        if records_with_precipitation > 0 {
            available_metrics.push("precipitation".to_string());
        }
        if records_with_wind_speed > 0 {
            available_metrics.push("wind_speed".to_string());
        }

        let composition = DatasetComposition {
            total_records,
            records_with_temperature,
            records_with_precipitation,
            records_with_wind_speed,
            available_metrics,
        };

        Ok((unified_records, composition))
    }

    /// Merge data from one weather record into another
    fn merge_weather_records(target: &mut WeatherRecord, source: WeatherRecord) -> Result<()> {
        // Verify records are for same station and date
        if target.station_id != source.station_id || target.date != source.date {
            return Err(ProcessingError::InvalidFormat(format!(
                "Cannot merge records: station/date mismatch ({}/{} vs {}/{})",
                target.station_id, target.date, source.station_id, source.date
            )));
        }

        // Merge temperature data (prefer non-null values)
        if source.temp_min.is_some() {
            target.temp_min = source.temp_min;
        }
        if source.temp_max.is_some() {
            target.temp_max = source.temp_max;
        }
        if source.temp_avg.is_some() {
            target.temp_avg = source.temp_avg;
        }

        // Merge precipitation data
        if source.precipitation.is_some() {
            target.precipitation = source.precipitation;
        }

        // Merge wind speed data
        if source.wind_speed.is_some() {
            target.wind_speed = source.wind_speed;
        }

        // Merge quality flags
        if source.temp_quality.is_some() {
            target.temp_quality = source.temp_quality;
        }
        if source.precip_quality.is_some() {
            target.precip_quality = source.precip_quality;
        }
        if source.wind_quality.is_some() {
            target.wind_quality = source.wind_quality;
        }

        // Re-run physical validation after merging data
        target.perform_physical_validation();

        Ok(())
    }

    /// Combine multiple integrity reports into one
    fn combine_integrity_reports(&self, reports: Vec<IntegrityReport>) -> IntegrityReport {
        let mut combined = IntegrityReport {
            total_records: 0,
            valid_records: 0,
            suspect_records: 0,
            invalid_records: 0,
            missing_data_records: 0,
            temperature_violations: Vec::new(),
            station_statistics: HashMap::new(),
        };

        for report in reports {
            combined.total_records += report.total_records;
            combined.valid_records += report.valid_records;
            combined.suspect_records += report.suspect_records;
            combined.invalid_records += report.invalid_records;
            combined.missing_data_records += report.missing_data_records;

            // Merge violations
            combined
                .temperature_violations
                .extend(report.temperature_violations);

            // Merge station statistics (basic approach - could be more sophisticated)
            for (station_id, stats) in report.station_statistics {
                combined.station_statistics.insert(station_id, stats);
            }
        }

        combined
    }

    /// Get list of archive paths
    pub fn archive_paths(&self) -> Vec<&Path> {
        self.archives.iter().map(|a| a.path.as_path()).collect()
    }

    /// Get total number of archives
    pub fn archive_count(&self) -> usize {
        self.archives.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::TempDir;

    fn create_test_directory() -> Result<TempDir> {
        let temp_dir = TempDir::new()?;

        // Create some test zip files
        File::create(temp_dir.path().join("UK_ALL_TEMP_MIN.zip"))?;
        File::create(temp_dir.path().join("UK_ALL_TEMP_MAX.zip"))?;
        File::create(temp_dir.path().join("UK_ALL_PRECIPITATION.zip"))?;
        File::create(temp_dir.path().join("OTHER_DATA.zip"))?;
        File::create(temp_dir.path().join("not_a_zip.txt"))?;

        Ok(temp_dir)
    }

    #[tokio::test]
    async fn test_directory_scanning() {
        let temp_dir = create_test_directory().unwrap();

        // This will fail because the zip files are empty, but we can test the scanning logic
        let result =
            MultiArchiveProcessor::from_directory(temp_dir.path(), Some("UK_ALL_"), 4).await;

        // Should find 3 UK_ALL_ files and fail on inspection
        assert!(result.is_err());
    }

    #[test]
    fn test_merge_weather_records() {
        use chrono::NaiveDate;

        let date = NaiveDate::from_ymd_opt(2023, 7, 15).unwrap();

        let mut target = WeatherRecord::new(
            123,
            "Test Station".to_string(),
            date,
            51.5,
            -0.1,
            Some(10.0), // min temp
            None,       // max temp
            None,       // avg temp
            None,       // precipitation
            None,       // wind speed
            Some("0".to_string()),
            None,
            None,
        );

        let source = WeatherRecord::new(
            123,
            "Test Station".to_string(),
            date,
            51.5,
            -0.1,
            None,       // min temp
            Some(20.0), // max temp
            Some(15.0), // avg temp
            Some(5.5),  // precipitation
            None,       // wind speed
            None,
            Some("0".to_string()),
            None,
        );

        MultiArchiveProcessor::merge_weather_records(&mut target, source).unwrap();

        assert_eq!(target.temp_min, Some(10.0));
        assert_eq!(target.temp_max, Some(20.0));
        assert_eq!(target.temp_avg, Some(15.0));
        assert_eq!(target.precipitation, Some(5.5));
        assert!(target.wind_speed.is_none());
    }
}
