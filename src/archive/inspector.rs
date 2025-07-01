use crate::archive::{TemperatureType, WeatherMetric};
use crate::error::{ProcessingError, Result};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use zip::ZipArchive;

type ScanResult = (
    Vec<WeatherMetric>,
    HashMap<WeatherMetric, usize>,
    HashSet<u32>,
);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveMetadata {
    pub country: String,
    pub metrics: Vec<WeatherMetric>,
    pub station_count: usize,
    pub date_range: Option<(NaiveDate, NaiveDate)>,
    pub file_counts: HashMap<WeatherMetric, usize>,
    pub total_files: usize,
}

impl ArchiveMetadata {
    pub fn display_summary(&self) -> String {
        let mut summary = format!(
            "Archive Metadata:\n  Country: {}\n  Total Stations: {}\n  Total Files: {}\n",
            self.country, self.station_count, self.total_files
        );

        if let Some((start, end)) = &self.date_range {
            summary.push_str(&format!("  Date Range: {} to {}\n", start, end));
        }

        summary.push_str("  Available Metrics:\n");
        for metric in &self.metrics {
            if let Some(count) = self.file_counts.get(metric) {
                summary.push_str(&format!(
                    "    {}: {} stations ({})\n",
                    metric.display_name(),
                    count,
                    metric.units()
                ));
            }
        }

        summary
    }

    pub fn has_temperature_data(&self) -> bool {
        self.metrics
            .iter()
            .any(|m| matches!(m, WeatherMetric::Temperature(_)))
    }

    pub fn has_complete_temperature(&self) -> bool {
        let temp_types: HashSet<_> = self
            .metrics
            .iter()
            .filter_map(|m| match m {
                WeatherMetric::Temperature(t) => Some(t),
                _ => None,
            })
            .collect();

        temp_types.contains(&TemperatureType::Minimum)
            && temp_types.contains(&TemperatureType::Maximum)
            && temp_types.contains(&TemperatureType::Average)
    }

    pub fn get_metric_coverage(&self, metric: &WeatherMetric) -> f64 {
        if let Some(count) = self.file_counts.get(metric) {
            (*count as f64) / (self.station_count as f64)
        } else {
            0.0
        }
    }
}

pub struct ArchiveInspector;

impl ArchiveInspector {
    pub fn inspect_zip(zip_path: &Path) -> Result<ArchiveMetadata> {
        let file = File::open(zip_path)?;
        let mut archive = ZipArchive::new(file)?;

        // Step 1: Scan all files to identify metrics and collect station IDs
        let (metrics, file_counts, all_station_ids) = Self::scan_data_files(&mut archive)?;

        if metrics.is_empty() {
            return Err(ProcessingError::InvalidFormat(
                "No recognized weather data files found in archive".to_string(),
            ));
        }

        // Step 2: Extract country from stations.txt
        let country = Self::extract_country(&mut archive)?;

        // Step 3: Validate metrics against elements.txt (optional)
        if let Ok(element_metrics) = Self::validate_with_elements(&mut archive) {
            // Cross-validate if elements.txt is available
            for metric in &metrics {
                if !element_metrics.contains(metric) {
                    println!(
                        "Warning: Metric {} found in files but not in elements.txt",
                        metric
                    );
                }
            }
        }

        // Step 4: Estimate date range (optional - requires parsing data files)
        let date_range = Self::estimate_date_range(&mut archive, &metrics).ok();

        Ok(ArchiveMetadata {
            country,
            metrics,
            station_count: all_station_ids.len(),
            date_range,
            file_counts,
            total_files: archive.len(),
        })
    }

    fn scan_data_files(archive: &mut ZipArchive<File>) -> Result<ScanResult> {
        let mut metrics = Vec::new();
        let mut file_counts: HashMap<WeatherMetric, usize> = HashMap::new();
        let mut all_station_ids = HashSet::new();

        for i in 0..archive.len() {
            let file = archive.by_index(i)?;
            let file_name = file.name();

            // Skip directories and metadata files
            if file_name.ends_with('/')
                || file_name == "stations.txt"
                || file_name == "elements.txt"
                || file_name == "metadata.txt"
                || file_name == "sources.txt"
            {
                continue;
            }

            // Parse weather data file pattern: {PREFIX}_STAID{ID}.txt
            if let Some(metric) = Self::parse_data_file_name(file_name) {
                // Add metric if not already present
                if !metrics.contains(&metric) {
                    metrics.push(metric.clone());
                }

                // Count files per metric
                *file_counts.entry(metric).or_insert(0) += 1;

                // Extract station ID
                if let Some(station_id) = Self::extract_station_id_from_filename(file_name) {
                    all_station_ids.insert(station_id);
                }
            }
        }

        Ok((metrics, file_counts, all_station_ids))
    }

    fn parse_data_file_name(file_name: &str) -> Option<WeatherMetric> {
        // Expected pattern: {PREFIX}_STAID{ID}.txt
        if !file_name.ends_with(".txt") {
            return None;
        }

        let name_without_ext = &file_name[..file_name.len() - 4];

        // Find the prefix before "_STAID"
        if let Some(pos) = name_without_ext.find("_STAID") {
            let prefix = &name_without_ext[..pos];
            WeatherMetric::from_file_prefix(prefix)
        } else {
            None
        }
    }

    fn extract_station_id_from_filename(file_name: &str) -> Option<u32> {
        // Extract station ID from patterns like TX_STAID000257.txt
        if let Some(start) = file_name.find("STAID") {
            let after_staid = &file_name[start + 5..];
            if let Some(end) = after_staid.find('.') {
                let id_str = &after_staid[..end];
                // Remove leading zeros and parse
                id_str.trim_start_matches('0').parse().ok()
            } else {
                None
            }
        } else {
            None
        }
    }

    fn extract_country(archive: &mut ZipArchive<File>) -> Result<String> {
        // Extract stations.txt to read country codes
        let mut stations_file = archive.by_name("stations.txt").map_err(|_| {
            ProcessingError::InvalidFormat("stations.txt not found in archive".to_string())
        })?;

        let reader = BufReader::new(&mut stations_file);
        let mut countries = HashSet::new();

        for line_result in reader.lines() {
            let line = line_result?;
            let trimmed = line.trim();

            // Skip empty lines and headers
            if trimmed.is_empty() || trimmed.starts_with("STAID") || trimmed.starts_with("---") {
                continue;
            }

            // Skip header content lines
            if trimmed.contains("EUROPEAN") || trimmed.contains("Klein Tank") {
                continue;
            }

            // Parse station line: STAID,STANAME,CN,LAT,LON,HGHT
            let parts: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
            if parts.len() >= 3 {
                // Country code is the 3rd field (index 2)
                let country_code = parts[2].trim();
                if !country_code.is_empty() && country_code.len() == 2 {
                    countries.insert(country_code.to_string());
                }
            }
        }

        if countries.is_empty() {
            return Err(ProcessingError::InvalidFormat(
                "No valid country codes found in stations.txt".to_string(),
            ));
        }

        if countries.len() > 1 {
            println!("Warning: Multiple countries found: {:?}", countries);
        }

        // Return the first (or most common) country
        Ok(countries.into_iter().next().unwrap())
    }

    fn validate_with_elements(archive: &mut ZipArchive<File>) -> Result<Vec<WeatherMetric>> {
        let mut elements_file = archive.by_name("elements.txt").map_err(|_| {
            ProcessingError::InvalidFormat("elements.txt not found in archive".to_string())
        })?;

        let reader = BufReader::new(&mut elements_file);
        let mut element_metrics = Vec::new();

        for line_result in reader.lines() {
            let line = line_result?;
            let trimmed = line.trim();

            // Skip empty lines and headers
            if trimmed.is_empty() || trimmed.starts_with("ELEID") || trimmed.starts_with("---") {
                continue;
            }

            // Skip header content lines
            if trimmed.contains("EUROPEAN") || trimmed.contains("Klein Tank") {
                continue;
            }

            // Parse element line: ELEID,DESC,UNIT
            let parts: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
            if let Some(element_id) = parts.first() {
                // Extract prefix from element ID (e.g., "TX1" -> "TX")
                let prefix = element_id
                    .chars()
                    .take_while(|c| c.is_alphabetic())
                    .collect::<String>();
                if let Some(metric) = WeatherMetric::from_file_prefix(&prefix) {
                    if !element_metrics.contains(&metric) {
                        element_metrics.push(metric);
                    }
                }
            }
        }

        Ok(element_metrics)
    }

    fn estimate_date_range(
        archive: &mut ZipArchive<File>,
        _metrics: &[WeatherMetric],
    ) -> Result<(NaiveDate, NaiveDate)> {
        // For performance, we'll just sample a few files to estimate date range
        let mut min_date: Option<NaiveDate> = None;
        let mut max_date: Option<NaiveDate> = None;
        let mut files_sampled = 0;
        const MAX_SAMPLE_FILES: usize = 5;

        for i in 0..archive.len() {
            if files_sampled >= MAX_SAMPLE_FILES {
                break;
            }

            let file = archive.by_index(i)?;
            let file_name = file.name();

            // Only sample data files
            if Self::parse_data_file_name(file_name).is_some() {
                if let Ok(dates) = Self::extract_date_range_from_file(file) {
                    min_date = Some(min_date.map_or(dates.0, |d| d.min(dates.0)));
                    max_date = Some(max_date.map_or(dates.1, |d| d.max(dates.1)));
                    files_sampled += 1;
                }
            }
        }

        match (min_date, max_date) {
            (Some(min), Some(max)) => Ok((min, max)),
            _ => Err(ProcessingError::InvalidFormat(
                "Could not determine date range from data files".to_string(),
            )),
        }
    }

    fn extract_date_range_from_file(
        mut file: zip::read::ZipFile,
    ) -> Result<(NaiveDate, NaiveDate)> {
        let reader = BufReader::new(&mut file);
        let mut min_date: Option<NaiveDate> = None;
        let mut max_date: Option<NaiveDate> = None;
        let mut lines_read = 0;
        const MAX_LINES_TO_READ: usize = 100; // Sample first 100 data lines

        for line_result in reader.lines() {
            if lines_read >= MAX_LINES_TO_READ {
                break;
            }

            let line = line_result?;
            let trimmed = line.trim();

            // Skip empty lines and headers
            if trimmed.is_empty() || lines_read < 20 {
                continue;
            }

            // Parse data line: SOUID, DATE, VALUE, Q_FLAG
            let parts: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
            if parts.len() >= 2 {
                if let Ok(date) = NaiveDate::parse_from_str(parts[1], "%Y%m%d") {
                    min_date = Some(min_date.map_or(date, |d| d.min(date)));
                    max_date = Some(max_date.map_or(date, |d| d.max(date)));
                    lines_read += 1;
                }
            }
        }

        match (min_date, max_date) {
            (Some(min), Some(max)) => Ok((min, max)),
            _ => Err(ProcessingError::InvalidFormat(
                "No valid dates found in data file".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;
    use zip::{CompressionMethod, ZipWriter};

    fn create_test_zip_with_multiple_metrics() -> Result<NamedTempFile> {
        let file = NamedTempFile::new()?;
        {
            let mut zip = ZipWriter::new(&file);

            // Add stations.txt with GB country code
            zip.start_file(
                "stations.txt",
                zip::write::FileOptions::default().compression_method(CompressionMethod::Stored),
            )?;
            zip.write_all(b"EUROPEAN CLIMATE ASSESSMENT & DATASET\n\nSTAID,STANAME,CN,LAT,LON,HGHT\n257,TEST STATION,GB,+51:30:00,-000:07:00,100\n258,ANOTHER STATION,GB,+52:30:00,-001:07:00,200\n")?;

            // Add elements.txt
            zip.start_file(
                "elements.txt",
                zip::write::FileOptions::default().compression_method(CompressionMethod::Stored),
            )?;
            zip.write_all(b"EUROPEAN CLIMATE ASSESSMENT & DATASET\n\nELEID,DESC,UNIT\nTX1,Maximum temperature,0.1 C\nTN1,Minimum temperature,0.1 C\nRR1,Precipitation,0.1 mm\n")?;

            // Add temperature data files
            zip.start_file(
                "TX_STAID000257.txt",
                zip::write::FileOptions::default().compression_method(CompressionMethod::Stored),
            )?;
            zip.write_all(b"Header\n101,20230101,125,0\n101,20230102,130,0\n")?;

            zip.start_file(
                "TN_STAID000257.txt",
                zip::write::FileOptions::default().compression_method(CompressionMethod::Stored),
            )?;
            zip.write_all(b"Header\n101,20230101,75,0\n101,20230102,80,0\n")?;

            // Add precipitation data file
            zip.start_file(
                "RR_STAID000258.txt",
                zip::write::FileOptions::default().compression_method(CompressionMethod::Stored),
            )?;
            zip.write_all(b"Header\n102,20230101,25,0\n102,20230102,30,0\n")?;

            zip.finish()?;
        } // zip goes out of scope here
        Ok(file)
    }

    #[test]
    fn test_parse_data_file_name() {
        assert_eq!(
            ArchiveInspector::parse_data_file_name("TX_STAID000257.txt"),
            Some(WeatherMetric::Temperature(TemperatureType::Maximum))
        );
        assert_eq!(
            ArchiveInspector::parse_data_file_name("RR_STAID000258.txt"),
            Some(WeatherMetric::Precipitation)
        );
        assert_eq!(
            ArchiveInspector::parse_data_file_name("FG_STAID000259.txt"),
            Some(WeatherMetric::WindSpeed)
        );
        assert_eq!(ArchiveInspector::parse_data_file_name("stations.txt"), None);
        assert_eq!(
            ArchiveInspector::parse_data_file_name("invalid_file.txt"),
            None
        );
    }

    #[test]
    fn test_extract_station_id_from_filename() {
        assert_eq!(
            ArchiveInspector::extract_station_id_from_filename("TX_STAID000257.txt"),
            Some(257)
        );
        assert_eq!(
            ArchiveInspector::extract_station_id_from_filename("RR_STAID001234.txt"),
            Some(1234)
        );
        assert_eq!(
            ArchiveInspector::extract_station_id_from_filename("invalid_file.txt"),
            None
        );
    }

    #[test]
    fn test_inspect_zip() -> Result<()> {
        let test_zip = create_test_zip_with_multiple_metrics()?;
        let metadata = ArchiveInspector::inspect_zip(test_zip.path())?;

        assert_eq!(metadata.country, "GB");
        assert_eq!(metadata.station_count, 2); // 257 and 258
        assert_eq!(metadata.metrics.len(), 3); // TX, TN, RR

        assert!(metadata.has_temperature_data());
        assert!(!metadata.has_complete_temperature()); // Missing TG

        // Check file counts
        assert_eq!(
            metadata
                .file_counts
                .get(&WeatherMetric::Temperature(TemperatureType::Maximum)),
            Some(&1)
        );
        assert_eq!(
            metadata.file_counts.get(&WeatherMetric::Precipitation),
            Some(&1)
        );

        Ok(())
    }

    #[test]
    fn test_archive_metadata_display() -> Result<()> {
        let test_zip = create_test_zip_with_multiple_metrics()?;
        let metadata = ArchiveInspector::inspect_zip(test_zip.path())?;

        let summary = metadata.display_summary();
        assert!(summary.contains("Country: GB"));
        assert!(summary.contains("Total Stations: 2"));
        assert!(summary.contains("Temperature (Max)"));
        assert!(summary.contains("Precipitation"));

        Ok(())
    }
}
