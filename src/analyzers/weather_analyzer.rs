use crate::error::Result;
use crate::models::ConsolidatedRecord;
use crate::utils::constants::{MAX_VALID_TEMP, MIN_VALID_TEMP};
use chrono::NaiveDate;
use std::collections::HashSet;
use std::path::Path;

/// Check if a temperature value is within the valid range and not a missing value
fn is_valid_temperature(temp: f32) -> bool {
    temp != -9999.0 && (MIN_VALID_TEMP..=MAX_VALID_TEMP).contains(&temp)
}

#[derive(Debug)]
pub struct WeatherStatistics {
    pub total_records: usize,
    pub unique_stations: usize,
    pub date_range: (NaiveDate, NaiveDate),
    pub temperature_stats: TemperatureStats,
    pub data_quality: DataQuality,
    pub geographic_bounds: GeographicBounds,
}

#[derive(Debug)]
pub struct TemperatureStats {
    pub min_temp: f32,
    pub max_temp: f32,
    pub avg_temp: f32,
    pub min_temp_location: String,
    pub max_temp_location: String,
}

#[derive(Debug)]
pub struct DataQuality {
    pub total_records: usize,
    pub valid_records: usize,
    pub suspect_records: usize,
    pub missing_records: usize,
    pub complete_records: usize,
}

impl DataQuality {
    pub fn valid_percentage(&self) -> f64 {
        (self.valid_records as f64 / self.total_records as f64) * 100.0
    }

    pub fn suspect_percentage(&self) -> f64 {
        (self.suspect_records as f64 / self.total_records as f64) * 100.0
    }

    pub fn missing_percentage(&self) -> f64 {
        (self.missing_records as f64 / self.total_records as f64) * 100.0
    }
}

#[derive(Debug)]
pub struct GeographicBounds {
    pub min_lat: f64,
    pub max_lat: f64,
    pub min_lon: f64,
    pub max_lon: f64,
}

pub struct WeatherAnalyzer;

impl WeatherAnalyzer {
    pub fn new() -> Self {
        Self
    }

    pub fn analyze_parquet(&self, path: &Path) -> Result<WeatherStatistics> {
        self.analyze_parquet_with_limit(path, 0) // Default to all records
    }

    pub fn analyze_parquet_with_limit(
        &self,
        path: &Path,
        limit: usize,
    ) -> Result<WeatherStatistics> {
        let writer = crate::writers::ParquetWriter::new();

        // Get file info to determine how many records to read
        let file_info = writer.get_file_info(path)?;
        let total_rows = file_info.total_rows as usize;

        // Determine how many records to read
        let records_to_read = if limit == 0 {
            total_rows // 0 means read all records
        } else {
            limit.min(total_rows) // Read up to limit, but not more than available
        };

        let records = writer.read_sample_records(path, records_to_read)?;

        if records.is_empty() {
            return Err(crate::error::ProcessingError::Config(
                "No records found in Parquet file".to_string(),
            ));
        }

        self.calculate_statistics(&records)
    }

    fn calculate_statistics(&self, records: &[ConsolidatedRecord]) -> Result<WeatherStatistics> {
        if records.is_empty() {
            return Err(crate::error::ProcessingError::Config(
                "No records to analyze".to_string(),
            ));
        }

        let mut unique_stations = HashSet::new();
        let mut min_date = records[0].date;
        let mut max_date = records[0].date;
        let mut min_temp = f32::INFINITY;
        let mut max_temp = f32::NEG_INFINITY;
        let mut temp_sum = 0.0f64;
        let mut temp_count = 0;
        let mut min_temp_location = String::new();
        let mut max_temp_location = String::new();

        let mut valid_count = 0;
        let mut suspect_count = 0;
        let mut missing_count = 0;
        let mut complete_count = 0;

        let mut min_lat = records[0].latitude;
        let mut max_lat = records[0].latitude;
        let mut min_lon = records[0].longitude;
        let mut max_lon = records[0].longitude;

        for record in records {
            unique_stations.insert(record.station_id);

            if record.date < min_date {
                min_date = record.date;
            }
            if record.date > max_date {
                max_date = record.date;
            }

            // Only include temperatures within valid range for statistics
            if is_valid_temperature(record.min_temp) && record.min_temp < min_temp {
                min_temp = record.min_temp;
                min_temp_location = format!("{} ({})", record.station_name, record.date);
            }

            if is_valid_temperature(record.max_temp) && record.max_temp > max_temp {
                max_temp = record.max_temp;
                max_temp_location = format!("{} ({})", record.station_name, record.date);
            }

            // Only include valid average temperatures in the overall average
            if is_valid_temperature(record.avg_temp) {
                temp_sum += record.avg_temp as f64;
                temp_count += 1;
            }

            if record.has_valid_data() {
                valid_count += 1;
            }
            if record.has_suspect_data() {
                suspect_count += 1;
            }
            if record.has_missing_data() {
                missing_count += 1;
            }
            if record.is_complete() {
                complete_count += 1;
            }

            if record.latitude < min_lat {
                min_lat = record.latitude;
            }
            if record.latitude > max_lat {
                max_lat = record.latitude;
            }
            if record.longitude < min_lon {
                min_lon = record.longitude;
            }
            if record.longitude > max_lon {
                max_lon = record.longitude;
            }
        }

        // Handle case where no valid temperatures were found
        let avg_temp = if temp_count > 0 {
            (temp_sum / temp_count as f64) as f32
        } else {
            f32::NAN
        };

        // Handle case where min/max are still infinity (no valid temperatures)
        if min_temp == f32::INFINITY {
            min_temp = f32::NAN;
            min_temp_location = "No valid measurements".to_string();
        }
        if max_temp == f32::NEG_INFINITY {
            max_temp = f32::NAN;
            max_temp_location = "No valid measurements".to_string();
        }

        Ok(WeatherStatistics {
            total_records: records.len(),
            unique_stations: unique_stations.len(),
            date_range: (min_date, max_date),
            temperature_stats: TemperatureStats {
                min_temp,
                max_temp,
                avg_temp,
                min_temp_location,
                max_temp_location,
            },
            data_quality: DataQuality {
                total_records: records.len(),
                valid_records: valid_count,
                suspect_records: suspect_count,
                missing_records: missing_count,
                complete_records: complete_count,
            },
            geographic_bounds: GeographicBounds {
                min_lat,
                max_lat,
                min_lon,
                max_lon,
            },
        })
    }
}

impl WeatherStatistics {
    pub fn summary(&self) -> String {
        let temp_range = if self.temperature_stats.min_temp.is_nan()
            || self.temperature_stats.max_temp.is_nan()
        {
            "No valid measurements".to_string()
        } else {
            format!(
                "{:.1}°C to {:.1}°C",
                self.temperature_stats.min_temp, self.temperature_stats.max_temp
            )
        };

        format!(
            "Weather Parameters: Temperature (min/max/avg)\n\
            Stations: {} stations\n\
            Date Range: {} to {} ({} years)\n\
            Records: {} total\n\
            Data Quality: {:.1}% valid, {:.1}% suspect, {:.1}% missing\n\
            Temperature Range: {}\n\
            Coverage: {:.1}°N-{:.1}°N, {:.1}°W-{:.1}°E",
            self.unique_stations,
            self.date_range.0,
            self.date_range.1,
            (self
                .date_range
                .1
                .signed_duration_since(self.date_range.0)
                .num_days()
                / 365),
            self.total_records,
            self.data_quality.valid_percentage(),
            self.data_quality.suspect_percentage(),
            self.data_quality.missing_percentage(),
            temp_range,
            self.geographic_bounds.min_lat,
            self.geographic_bounds.max_lat,
            self.geographic_bounds.min_lon.abs(),
            self.geographic_bounds.max_lon
        )
    }

    pub fn detailed_summary(&self) -> String {
        let coldest = if self.temperature_stats.min_temp.is_nan() {
            "No valid measurements".to_string()
        } else {
            format!(
                "{:.1}°C at {}",
                self.temperature_stats.min_temp, self.temperature_stats.min_temp_location
            )
        };

        let hottest = if self.temperature_stats.max_temp.is_nan() {
            "No valid measurements".to_string()
        } else {
            format!(
                "{:.1}°C at {}",
                self.temperature_stats.max_temp, self.temperature_stats.max_temp_location
            )
        };

        let average = if self.temperature_stats.avg_temp.is_nan() {
            "No valid measurements".to_string()
        } else {
            format!("{:.1}°C", self.temperature_stats.avg_temp)
        };

        format!(
            "{}\n\n\
            Extreme Temperatures (valid range only):\n\
            - Coldest: {}\n\
            - Hottest: {}\n\
            - Average: {}\n\n\
            Data Completeness:\n\
            - Complete records: {}/{} ({:.1}%)",
            self.summary(),
            coldest,
            hottest,
            average,
            self.data_quality.complete_records,
            self.data_quality.total_records,
            (self.data_quality.complete_records as f64 / self.data_quality.total_records as f64)
                * 100.0
        )
    }
}

impl Default for WeatherAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}
