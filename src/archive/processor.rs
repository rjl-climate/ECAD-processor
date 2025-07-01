use crate::archive::{
    ArchiveInspector, ArchiveMetadata, TempFileManager, TemperatureType, WeatherMetric,
};
use crate::error::{ProcessingError, Result};
use crate::models::{StationMetadata, WeatherRecord};
use crate::processors::{IntegrityReport, StationStatistics, TemperatureViolation, ViolationType};
use crate::readers::{StationReader, TemperatureReader};
use chrono::NaiveDate;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

pub struct ArchiveProcessor {
    temp_manager: TempFileManager,
    archive_metadata: ArchiveMetadata,
}

impl ArchiveProcessor {
    pub async fn from_zip(zip_path: &Path) -> Result<Self> {
        // Inspect the archive to get metadata
        let archive_metadata = ArchiveInspector::inspect_zip(zip_path)?;

        // Create temporary file manager
        let temp_manager = TempFileManager::new()?;

        Ok(Self {
            temp_manager,
            archive_metadata,
        })
    }

    pub fn metadata(&self) -> &ArchiveMetadata {
        &self.archive_metadata
    }

    pub async fn process_data(
        mut self,
        zip_path: &Path,
    ) -> Result<(Vec<WeatherRecord>, IntegrityReport)> {
        // Extract metadata files
        let metadata_files = self.temp_manager.extract_metadata_files(zip_path)?;

        // Read station metadata
        let station_map = if let Some(stations_path) = metadata_files.get("stations.txt") {
            let reader = StationReader::new();
            reader.read_stations_map(stations_path)?
        } else {
            return Err(ProcessingError::InvalidFormat(
                "stations.txt not found in archive".to_string(),
            ));
        };

        println!("Loaded {} stations from metadata", station_map.len());

        // Group weather data by station and date
        let mut weather_data: HashMap<(u32, NaiveDate), WeatherRecord> = HashMap::new();

        // Process each metric type
        for metric in &self.archive_metadata.metrics {
            let pattern = format!("{}_STAID", metric.to_file_prefix());
            let data_files = self
                .temp_manager
                .extract_files_matching_pattern(zip_path, &pattern)?;

            println!(
                "Processing {} files for metric: {}",
                data_files.len(),
                metric
            );

            // Process files for this metric
            self.process_metric_files(&data_files, metric, &station_map, &mut weather_data)
                .await?;
        }

        // Convert to vector and ensure all records have physical validation
        let mut all_records: Vec<WeatherRecord> = weather_data.into_values().collect();

        // Ensure all records have physical validation performed after data population
        for record in &mut all_records {
            record.perform_physical_validation();
        }

        let integrity_report = self.calculate_integrity_report(&all_records);

        // Cleanup temporary files
        self.temp_manager.cleanup()?;

        Ok((all_records, integrity_report))
    }

    async fn process_metric_files(
        &self,
        file_paths: &[std::path::PathBuf],
        metric: &WeatherMetric,
        station_map: &HashMap<u32, StationMetadata>,
        weather_data: &mut HashMap<(u32, NaiveDate), WeatherRecord>,
    ) -> Result<()> {
        for file_path in file_paths {
            if let Some(file_name) = file_path.file_name().and_then(|n| n.to_str()) {
                if let Some(station_id) = extract_station_id_from_filename(file_name) {
                    // Get station metadata
                    let station_metadata = station_map.get(&station_id);
                    if station_metadata.is_none() {
                        println!("Warning: Station {} not found in metadata", station_id);
                        continue;
                    }
                    let station = station_metadata.unwrap();

                    // Parse weather data based on metric type
                    match metric {
                        WeatherMetric::Temperature(temp_type) => {
                            self.process_temperature_file(
                                file_path,
                                station,
                                temp_type,
                                weather_data,
                            )?;
                        }
                        WeatherMetric::Precipitation => {
                            self.process_precipitation_file(file_path, station, weather_data)?;
                        }
                        WeatherMetric::WindSpeed => {
                            self.process_wind_speed_file(file_path, station, weather_data)?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub fn temp_dir_path(&self) -> &Path {
        self.temp_manager.temp_dir_path()
    }

    pub fn cleanup(mut self) -> Result<()> {
        self.temp_manager.cleanup()
    }

    fn process_temperature_file(
        &self,
        file_path: &Path,
        station: &StationMetadata,
        temp_type: &TemperatureType,
        weather_data: &mut HashMap<(u32, NaiveDate), WeatherRecord>,
    ) -> Result<()> {
        let reader = TemperatureReader::new();
        let temp_records = reader.read_temperatures_with_station_id(file_path, station.staid)?;

        for temp_record in temp_records {
            let key = (temp_record.staid, temp_record.date);

            // Get or create weather record for this station/date
            let weather_record = weather_data.entry(key).or_insert_with(|| {
                WeatherRecord::builder()
                    .station_id(station.staid)
                    .station_name(station.name.clone())
                    .date(temp_record.date)
                    .coordinates(station.latitude, station.longitude)
                    .build()
                    .unwrap_or_else(|_| {
                        // Fallback record if builder fails
                        WeatherRecord::new(
                            station.staid,
                            station.name.clone(),
                            temp_record.date,
                            station.latitude,
                            station.longitude,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                        )
                    })
            });

            // Update temperature data based on type
            match temp_type {
                TemperatureType::Minimum => {
                    weather_record.temp_min = Some(temp_record.temperature);
                }
                TemperatureType::Maximum => {
                    weather_record.temp_max = Some(temp_record.temperature);
                }
                TemperatureType::Average => {
                    weather_record.temp_avg = Some(temp_record.temperature);
                }
            }

            // Update quality flag (combine multiple flags)
            let quality_str = temp_record.quality_flag.to_string();
            if let Some(ref existing) = weather_record.temp_quality {
                if !existing.contains(&quality_str) {
                    weather_record.temp_quality = Some(format!("{}{}", existing, quality_str));
                }
            } else {
                weather_record.temp_quality = Some(quality_str);
            }
        }

        Ok(())
    }

    fn process_precipitation_file(
        &self,
        file_path: &Path,
        station: &StationMetadata,
        weather_data: &mut HashMap<(u32, NaiveDate), WeatherRecord>,
    ) -> Result<()> {
        let precip_records = self.parse_weather_file(file_path, station.staid)?;

        for (date, value, quality) in precip_records {
            let key = (station.staid, date);

            let weather_record = weather_data.entry(key).or_insert_with(|| {
                WeatherRecord::new(
                    station.staid,
                    station.name.clone(),
                    date,
                    station.latitude,
                    station.longitude,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
            });

            weather_record.precipitation = Some(value / 10.0); // Convert from 0.1mm to mm
            weather_record.precip_quality = Some(quality.to_string());
        }

        Ok(())
    }

    fn process_wind_speed_file(
        &self,
        file_path: &Path,
        station: &StationMetadata,
        weather_data: &mut HashMap<(u32, NaiveDate), WeatherRecord>,
    ) -> Result<()> {
        let wind_records = self.parse_weather_file(file_path, station.staid)?;

        for (date, value, quality) in wind_records {
            let key = (station.staid, date);

            let weather_record = weather_data.entry(key).or_insert_with(|| {
                WeatherRecord::new(
                    station.staid,
                    station.name.clone(),
                    date,
                    station.latitude,
                    station.longitude,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
            });

            weather_record.wind_speed = Some(value / 10.0); // Convert from 0.1 m/s to m/s
            weather_record.wind_quality = Some(quality.to_string());
        }

        Ok(())
    }

    fn parse_weather_file(
        &self,
        file_path: &Path,
        _station_id: u32,
    ) -> Result<Vec<(NaiveDate, f32, u8)>> {
        let file = File::open(file_path)?;
        let reader = BufReader::new(file);
        let mut records = Vec::new();
        let mut line_count = 0;

        for line_result in reader.lines() {
            let line = line_result?;
            line_count += 1;

            // Skip empty lines
            if line.trim().is_empty() {
                continue;
            }

            // Skip header lines (first 20 lines typically contain headers)
            if line_count <= 20 {
                continue;
            }

            // Parse data line: SOUID, DATE, VALUE, Q_FLAG
            let parts: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
            if parts.len() < 4 {
                continue; // Skip malformed lines
            }

            // Parse date (YYYYMMDD format)
            if let Ok(date) = NaiveDate::parse_from_str(parts[1], "%Y%m%d") {
                // Parse value (skip missing values)
                if parts[2] != "-9999" {
                    if let (Ok(value), Ok(quality)) =
                        (parts[2].parse::<f32>(), parts[3].parse::<u8>())
                    {
                        records.push((date, value, quality));
                    }
                }
            }
        }

        Ok(records)
    }

    fn calculate_integrity_report(&self, records: &[WeatherRecord]) -> IntegrityReport {
        let mut valid_records = 0;
        let mut suspect_records = 0;
        let mut invalid_records = 0;
        let mut missing_data_records = 0;
        let mut temperature_violations = Vec::new();
        let mut station_statistics: HashMap<u32, StationStatistics> = HashMap::new();

        for record in records {
            // Check data quality
            if record.has_valid_temperature_data()
                && record.has_valid_precipitation_data()
                && record.has_valid_wind_data()
            {
                valid_records += 1;
            } else if record.has_suspect_data() {
                suspect_records += 1;
            } else if record.has_missing_data() {
                missing_data_records += 1;
            }

            // Check temperature relationships
            if let Err(e) = record.validate_relationships() {
                let violation_type = if e.to_string().contains("Min temperature") {
                    ViolationType::MinGreaterThanAvg
                } else if e.to_string().contains("Avg temperature") {
                    ViolationType::AvgGreaterThanMax
                } else {
                    ViolationType::OutOfRange
                };

                temperature_violations.push(TemperatureViolation {
                    station_id: record.station_id,
                    date: record.date,
                    violation_type,
                    details: e.to_string(),
                });
                invalid_records += 1;
            }

            // Update station statistics
            let station_stats = station_statistics.entry(record.station_id).or_default();

            station_stats.total_records += 1;

            if record.has_valid_temperature_data() {
                station_stats.valid_records += 1;
            } else if record.has_suspect_data() {
                station_stats.suspect_records += 1;
            } else if record.has_missing_data() {
                station_stats.missing_data_records += 1;
            }

            // Update temperature statistics
            if let Some(min_temp) = record.temp_min {
                station_stats.min_temp = Some(
                    station_stats
                        .min_temp
                        .map_or(min_temp, |curr| curr.min(min_temp)),
                );
            }
            if let Some(max_temp) = record.temp_max {
                station_stats.max_temp = Some(
                    station_stats
                        .max_temp
                        .map_or(max_temp, |curr| curr.max(max_temp)),
                );
            }
            if let Some(avg_temp) = record.temp_avg {
                station_stats.avg_temp = Some(
                    station_stats
                        .avg_temp
                        .map_or(avg_temp, |curr| (curr + avg_temp) / 2.0),
                );
            }
        }

        IntegrityReport {
            total_records: records.len(),
            valid_records,
            suspect_records,
            invalid_records,
            missing_data_records,
            temperature_violations,
            station_statistics,
        }
    }
}

impl Drop for ArchiveProcessor {
    fn drop(&mut self) {
        if let Err(e) = self.temp_manager.cleanup() {
            eprintln!("Warning: Failed to cleanup archive processor: {}", e);
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_station_id_from_filename() {
        assert_eq!(
            extract_station_id_from_filename("TX_STAID000257.txt"),
            Some(257)
        );
        assert_eq!(
            extract_station_id_from_filename("RR_STAID001234.txt"),
            Some(1234)
        );
        assert_eq!(extract_station_id_from_filename("invalid_file.txt"), None);
    }
}
