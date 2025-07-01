use crate::error::{ProcessingError, Result};
use crate::models::{StationMetadata, TemperatureRecord};
use crate::readers::{StationReader, TemperatureReader};
use crate::utils::constants::{STATIONS_FILE, UK_TEMP_AVG_DIR, UK_TEMP_MAX_DIR, UK_TEMP_MIN_DIR};
use rayon::prelude::*;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::task::JoinHandle;

pub struct ConcurrentReader {
    max_workers: usize,
}

impl ConcurrentReader {
    pub fn new(max_workers: usize) -> Self {
        Self { max_workers }
    }

    /// Read all temperature data concurrently
    pub async fn read_all_temperature_data(&self, base_path: &Path) -> Result<TemperatureData> {
        // Read station metadata first
        let stations_path = base_path.join(UK_TEMP_MIN_DIR).join(STATIONS_FILE);
        let station_reader = StationReader::new();
        let stations = station_reader.read_stations_map(&stations_path)?;
        let stations = Arc::new(stations);

        // Define paths for each temperature type
        let min_path = base_path.join(UK_TEMP_MIN_DIR);
        let max_path = base_path.join(UK_TEMP_MAX_DIR);
        let avg_path = base_path.join(UK_TEMP_AVG_DIR);

        // Read temperature files concurrently
        let stations_clone1 = stations.clone();
        let stations_clone2 = stations.clone();
        let stations_clone3 = stations.clone();

        let max_workers = self.max_workers;

        let min_handle: JoinHandle<Result<HashMap<(u32, chrono::NaiveDate), TemperatureRecord>>> =
            tokio::spawn(async move {
                Self::read_temperature_type_parallel_static(&min_path, stations_clone1, max_workers)
                    .await
            });

        let max_handle: JoinHandle<Result<HashMap<(u32, chrono::NaiveDate), TemperatureRecord>>> =
            tokio::spawn(async move {
                Self::read_temperature_type_parallel_static(&max_path, stations_clone2, max_workers)
                    .await
            });

        let avg_handle: JoinHandle<Result<HashMap<(u32, chrono::NaiveDate), TemperatureRecord>>> =
            tokio::spawn(async move {
                Self::read_temperature_type_parallel_static(&avg_path, stations_clone3, max_workers)
                    .await
            });

        // Wait for all reads to complete
        let (min_temps, max_temps, avg_temps) =
            tokio::try_join!(min_handle, max_handle, avg_handle)?;

        Ok(TemperatureData {
            stations: Arc::try_unwrap(stations).unwrap_or_else(|arc| (*arc).clone()),
            min_temperatures: min_temps?,
            max_temperatures: max_temps?,
            avg_temperatures: avg_temps?,
        })
    }

    /// Read temperature files for a specific type using parallel processing
    async fn read_temperature_type_parallel_static(
        dir_path: &Path,
        stations: Arc<HashMap<u32, StationMetadata>>,
        _max_workers: usize,
    ) -> Result<HashMap<(u32, chrono::NaiveDate), TemperatureRecord>> {
        // Find all temperature files for UK stations
        // Determine file prefix based on directory name
        let file_prefix = match dir_path.file_name().and_then(|f| f.to_str()) {
            Some("uk_temp_min") => "TN",
            Some("uk_temp_max") => "TX",
            Some("uk_temp_avg") => "TG",
            _ => {
                return Err(ProcessingError::InvalidFormat(format!(
                    "Unknown temperature directory: {:?}",
                    dir_path
                )))
            }
        };

        let temperature_files: Vec<PathBuf> =
            Self::find_temperature_files_static(dir_path, &stations, file_prefix)?;

        // Process files in parallel using Rayon
        let all_records: Vec<Vec<TemperatureRecord>> = temperature_files
            .par_iter()
            .map(|path| {
                let reader = TemperatureReader::new();
                reader.read_temperatures(path)
            })
            .collect::<Result<Vec<_>>>()?;

        // Flatten and convert to HashMap
        let mut temperature_map = HashMap::new();
        for records in all_records {
            for record in records {
                temperature_map.insert((record.staid, record.date), record);
            }
        }

        Ok(temperature_map)
    }

    /// Find temperature files for UK stations with specific prefix
    fn find_temperature_files_static(
        dir_path: &Path,
        stations: &HashMap<u32, StationMetadata>,
        file_prefix: &str,
    ) -> Result<Vec<PathBuf>> {
        let mut files = Vec::new();

        // Look for files with pattern {prefix}_STAID*.txt
        for entry in std::fs::read_dir(dir_path)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() {
                if let Some(file_name) = path.file_name() {
                    let file_name_str = file_name.to_string_lossy();

                    // Check if it's a temperature data file with the right prefix
                    if file_name_str.starts_with(&format!("{}_STAID", file_prefix))
                        && file_name_str.ends_with(".txt")
                    {
                        // Extract station ID from filename
                        if let Some(staid_str) = file_name_str
                            .strip_prefix(&format!("{}_STAID", file_prefix))
                            .and_then(|s| s.strip_suffix(".txt"))
                        {
                            if let Ok(staid) = staid_str.trim_start_matches('0').parse::<u32>() {
                                // Only include UK stations
                                if let Some(station) = stations.get(&staid) {
                                    if station.is_uk_station() {
                                        files.push(path);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(files)
    }

    /// Process a single station's data across all temperature types
    pub fn process_station_data(
        &self,
        station_id: u32,
        base_path: &Path,
    ) -> Result<StationTemperatureData> {
        let min_path = base_path.join(UK_TEMP_MIN_DIR);
        let max_path = base_path.join(UK_TEMP_MAX_DIR);
        let avg_path = base_path.join(UK_TEMP_AVG_DIR);

        let reader = TemperatureReader::new();

        // Read temperature data for this station
        let min_file = min_path.join(format!("TG_STAID{:06}.txt", station_id));
        let max_file = max_path.join(format!("TG_STAID{:06}.txt", station_id));
        let avg_file = avg_path.join(format!("TG_STAID{:06}.txt", station_id));

        let min_temps = if min_file.exists() {
            reader.read_temperatures(&min_file)?
        } else {
            Vec::new()
        };

        let max_temps = if max_file.exists() {
            reader.read_temperatures(&max_file)?
        } else {
            Vec::new()
        };

        let avg_temps = if avg_file.exists() {
            reader.read_temperatures(&avg_file)?
        } else {
            Vec::new()
        };

        Ok(StationTemperatureData {
            station_id,
            min_temperatures: min_temps,
            max_temperatures: max_temps,
            avg_temperatures: avg_temps,
        })
    }
}

impl Default for ConcurrentReader {
    fn default() -> Self {
        Self::new(num_cpus::get())
    }
}

/// Container for all temperature data
#[derive(Debug)]
pub struct TemperatureData {
    pub stations: HashMap<u32, StationMetadata>,
    pub min_temperatures: HashMap<(u32, chrono::NaiveDate), TemperatureRecord>,
    pub max_temperatures: HashMap<(u32, chrono::NaiveDate), TemperatureRecord>,
    pub avg_temperatures: HashMap<(u32, chrono::NaiveDate), TemperatureRecord>,
}

/// Container for a single station's temperature data
#[derive(Debug)]
pub struct StationTemperatureData {
    pub station_id: u32,
    pub min_temperatures: Vec<TemperatureRecord>,
    pub max_temperatures: Vec<TemperatureRecord>,
    pub avg_temperatures: Vec<TemperatureRecord>,
}
