use crate::error::{ProcessingError, Result};
use crate::models::StationMetadata;
use crate::utils::coordinates::parse_coordinate;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

pub struct StationReader {
    skip_headers: bool,
}

impl StationReader {
    pub fn new() -> Self {
        Self { skip_headers: true }
    }

    pub fn with_skip_headers(skip_headers: bool) -> Self {
        Self { skip_headers }
    }

    /// Read station metadata from the stations.txt file
    pub fn read_stations(&self, path: &Path) -> Result<Vec<StationMetadata>> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let mut stations = Vec::new();
        for (_line_count, line_result) in reader.lines().enumerate() {
            let line = line_result?;
            let _line_count = _line_count + 1;

            // Skip empty lines
            if line.trim().is_empty() {
                continue;
            }

            // Skip header lines and detect data start automatically
            if self.skip_headers {
                // Skip lines that don't start with a number (station ID)
                if !line
                    .trim_start()
                    .chars()
                    .next()
                    .unwrap_or(' ')
                    .is_ascii_digit()
                {
                    continue;
                }
            }

            // Parse station data
            if let Some(station) = self.parse_station_line(&line)? {
                stations.push(station);
            }
        }

        Ok(stations)
    }

    /// Parse a single line from the stations file
    fn parse_station_line(&self, line: &str) -> Result<Option<StationMetadata>> {
        // Expected format: STAID, STANAME                                 , CN, LAT    , LON     , HGHT
        let parts: Vec<&str> = line.split(',').map(|s| s.trim()).collect();

        if parts.len() < 6 {
            return Ok(None); // Skip malformed lines
        }

        // Parse station ID
        let staid = parts[0].parse::<u32>().map_err(|_| {
            ProcessingError::InvalidFormat(format!("Invalid station ID: '{}'", parts[0]))
        })?;

        // Parse other fields
        let name = parts[1].to_string();
        let country = parts[2].to_string();
        let latitude = parse_coordinate(parts[3])?;
        let longitude = parse_coordinate(parts[4])?;

        // Parse elevation (can be negative or missing)
        let elevation = if parts[5].is_empty() || parts[5] == "-999" {
            None
        } else {
            Some(parts[5].parse::<i32>().map_err(|_| {
                ProcessingError::InvalidFormat(format!("Invalid elevation: '{}'", parts[5]))
            })?)
        };

        Ok(Some(StationMetadata::new(
            staid, name, country, latitude, longitude, elevation,
        )))
    }

    /// Read station metadata from a map of station IDs
    pub fn read_stations_map(&self, path: &Path) -> Result<HashMap<u32, StationMetadata>> {
        let stations = self.read_stations(path)?;
        let mut map = HashMap::with_capacity(stations.len());

        for station in stations {
            map.insert(station.staid, station);
        }

        Ok(map)
    }
}

impl Default for StationReader {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_parse_station_line() {
        let reader = StationReader::new();

        let line = "12345, London Weather Station        , GB, 51:30:26, -0:07:39,   35";
        let station = reader.parse_station_line(line).unwrap().unwrap();

        assert_eq!(station.staid, 12345);
        assert_eq!(station.name, "London Weather Station");
        assert_eq!(station.country, "GB");
        assert!((station.latitude - 51.507222).abs() < 0.00001);
        assert!((station.longitude - -0.1275).abs() < 0.00001);
        assert_eq!(station.elevation, Some(35));
    }

    #[test]
    fn test_read_stations_file() -> Result<()> {
        let mut temp_file = NamedTempFile::new()?;
        writeln!(
            temp_file,
            "STAID, STANAME                                 , CN, LAT    , LON     , HGHT"
        )?;
        writeln!(
            temp_file,
            "------,----------------------------------------,---,--------,--------,-----"
        )?;
        writeln!(temp_file, "")?;
        writeln!(
            temp_file,
            "    1, VAEXJOE                                 , SE, 56:52:00, 14:48:00,  166"
        )?;
        writeln!(
            temp_file,
            "    2, BRAGANCA                                , PT, 41:48:00, -6:44:00,  691"
        )?;

        let reader_test = StationReader::new();
        let stations = reader_test.read_stations(temp_file.path())?;

        assert_eq!(stations.len(), 2);
        assert_eq!(stations[0].staid, 1);
        assert_eq!(stations[0].name, "VAEXJOE");
        assert_eq!(stations[1].staid, 2);
        assert_eq!(stations[1].name, "BRAGANCA");

        Ok(())
    }

    #[test]
    fn test_read_real_stations_file() -> Result<()> {
        use std::path::Path;

        let stations_path = Path::new("data/uk_temp_min/stations.txt");
        if !stations_path.exists() {
            // Skip test if data file doesn't exist
            return Ok(());
        }

        let reader = StationReader::new();
        let stations = reader.read_stations(stations_path)?;

        println!("Found {} stations", stations.len());
        if !stations.is_empty() {
            println!(
                "First station: ID={}, Name={}",
                stations[0].staid, stations[0].name
            );
        }

        assert!(!stations.is_empty(), "Should find at least one station");

        Ok(())
    }
}
