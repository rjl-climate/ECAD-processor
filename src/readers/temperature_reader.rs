use crate::error::{ProcessingError, Result};
use crate::models::TemperatureRecord;
use crate::utils::constants::DEFAULT_BUFFER_SIZE;
use chrono::NaiveDate;
use memmap2::Mmap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

pub struct TemperatureReader {
    skip_headers: bool,
    use_mmap: bool,
}

impl TemperatureReader {
    pub fn new() -> Self {
        Self {
            skip_headers: true,
            use_mmap: false,
        }
    }
    
    pub fn with_skip_headers(skip_headers: bool) -> Self {
        Self {
            skip_headers,
            use_mmap: false,
        }
    }
    
    pub fn with_mmap(use_mmap: bool) -> Self {
        Self {
            skip_headers: true,
            use_mmap,
        }
    }
    
    /// Read temperature records from a file (extracts station ID from filename)
    pub fn read_temperatures(&self, path: &Path) -> Result<Vec<TemperatureRecord>> {
        let station_id = self.extract_station_id_from_path(path)?;
        self.read_temperatures_with_station_id(path, station_id)
    }
    
    /// Read temperature records from a file with explicit station ID
    pub fn read_temperatures_with_station_id(&self, path: &Path, station_id: u32) -> Result<Vec<TemperatureRecord>> {
        if self.use_mmap {
            self.read_temperatures_mmap(path, station_id)
        } else {
            self.read_temperatures_buffered(path, station_id)
        }
    }
    
    /// Extract station ID from filename (e.g., TN_STAID000257.txt -> 257)
    pub fn extract_station_id_from_path(&self, path: &Path) -> Result<u32> {
        let filename = path.file_name()
            .and_then(|f| f.to_str())
            .ok_or_else(|| ProcessingError::InvalidFormat("Invalid file path".to_string()))?;
        
        // Extract station ID from patterns like TN_STAID000257.txt, TX_STAID000257.txt, TG_STAID000257.txt
        if let Some(staid_part) = filename
            .strip_suffix(".txt")
            .and_then(|s| s.find("STAID").map(|pos| &s[pos + 5..]))
        {
            staid_part.trim_start_matches('0').parse::<u32>()
                .map_err(|_| ProcessingError::InvalidFormat(format!(
                    "Could not extract station ID from filename: {}", filename
                )))
        } else {
            Err(ProcessingError::InvalidFormat(format!(
                "Filename does not match expected pattern: {}", filename
            )))
        }
    }
    
    /// Read temperature records using buffered I/O
    fn read_temperatures_buffered(&self, path: &Path, station_id: u32) -> Result<Vec<TemperatureRecord>> {
        let file = File::open(path)?;
        let reader = BufReader::with_capacity(DEFAULT_BUFFER_SIZE, file);
        let mut records = Vec::new();
        let mut line_count = 0;
        
        for line_result in reader.lines() {
            let line = line_result?;
            line_count += 1;
            
            // Skip empty lines
            if line.trim().is_empty() {
                continue;
            }
            
            // Skip header lines
            if self.skip_headers && line_count <= 20 {
                continue;
            }
            
            // Parse temperature data
            if let Some(record) = self.parse_temperature_line(&line, station_id)? {
                records.push(record);
            }
        }
        
        Ok(records)
    }
    
    /// Read temperature records using memory-mapped I/O for large files
    fn read_temperatures_mmap(&self, path: &Path, station_id: u32) -> Result<Vec<TemperatureRecord>> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let content = std::str::from_utf8(&mmap)
            .map_err(|e| ProcessingError::InvalidFormat(format!("Invalid UTF-8: {}", e)))?;
        
        let mut records = Vec::new();
        let mut line_count = 0;
        
        for line in content.lines() {
            line_count += 1;
            
            // Skip empty lines
            if line.trim().is_empty() {
                continue;
            }
            
            // Skip header lines
            if self.skip_headers && line_count <= 20 {
                continue;
            }
            
            // Parse temperature data
            if let Some(record) = self.parse_temperature_line(line, station_id)? {
                records.push(record);
            }
        }
        
        Ok(records)
    }
    
    /// Parse a single line from the temperature file with provided station ID
    fn parse_temperature_line(&self, line: &str, station_id: u32) -> Result<Option<TemperatureRecord>> {
        // Expected format: SOUID, DATE, TEMP, Q_TEMP
        let parts: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
        
        if parts.len() < 4 {
            return Ok(None); // Skip malformed lines
        }
        
        // Parse source ID
        let souid = parts[0]
            .parse::<u32>()
            .map_err(|_| ProcessingError::InvalidFormat(format!(
                "Invalid source ID: '{}'", parts[0]
            )))?;
        
        // Parse date (YYYYMMDD format)
        let date_str = parts[1];
        let date = NaiveDate::parse_from_str(date_str, "%Y%m%d")
            .map_err(|_| ProcessingError::InvalidFormat(format!(
                "Invalid date format: '{}'", date_str
            )))?;
        
        // Parse temperature (in 0.1 degrees Celsius)
        let temp_str = parts[2];
        if temp_str == "-9999" {
            return Ok(None); // Skip missing values
        }
        
        let temp_tenths = temp_str
            .parse::<i32>()
            .map_err(|_| ProcessingError::InvalidFormat(format!(
                "Invalid temperature: '{}'", temp_str
            )))?;
        
        let temperature = temp_tenths as f32 / 10.0;
        
        // Parse quality flag
        let quality_flag = parts[3]
            .parse::<u8>()
            .map_err(|_| ProcessingError::InvalidFormat(format!(
                "Invalid quality flag: '{}'", parts[3]
            )))?;
        
        let record = TemperatureRecord::new(
            station_id,
            souid,
            date,
            temperature,
            quality_flag,
        )?;
        
        Ok(Some(record))
    }
    
    /// Read temperature records for a specific station
    pub fn read_station_temperatures(&self, path: &Path, station_id: u32) -> Result<Vec<TemperatureRecord>> {
        let all_records = self.read_temperatures(path)?;
        Ok(all_records.into_iter()
            .filter(|r| r.staid == station_id)
            .collect())
    }
    
    /// Stream temperature records using an iterator (memory efficient for large files)
    pub fn stream_temperatures<'a>(&self, path: &'a Path) -> Result<TemperatureIterator<'a>> {
        TemperatureIterator::new(path, self.skip_headers)
    }
}

impl Default for TemperatureReader {
    fn default() -> Self {
        Self::new()
    }
}

/// Iterator for streaming temperature records
pub struct TemperatureIterator<'a> {
    reader: BufReader<File>,
    skip_headers: bool,
    line_count: usize,
    _path: &'a Path,
}

impl<'a> TemperatureIterator<'a> {
    fn new(path: &'a Path, skip_headers: bool) -> Result<Self> {
        let file = File::open(path)?;
        let reader = BufReader::with_capacity(DEFAULT_BUFFER_SIZE, file);
        
        Ok(Self {
            reader,
            skip_headers,
            line_count: 0,
            _path: path,
        })
    }
}

impl Iterator for TemperatureIterator<'_> {
    type Item = Result<TemperatureRecord>;
    
    fn next(&mut self) -> Option<Self::Item> {
        let mut line = String::new();
        
        loop {
            line.clear();
            
            match self.reader.read_line(&mut line) {
                Ok(0) => return None, // EOF
                Ok(_) => {
                    self.line_count += 1;
                    
                    // Skip empty lines
                    if line.trim().is_empty() {
                        continue;
                    }
                    
                    // Skip headers
                    if self.skip_headers && self.line_count <= 20 {
                        continue;
                    }
                    
                    // Parse line - TODO: Fix iterator to extract station ID from path
                    let reader = TemperatureReader::new();
                    let station_id = reader.extract_station_id_from_path(self._path).ok().unwrap_or(0);
                    match reader.parse_temperature_line(&line, station_id) {
                        Ok(Some(record)) => return Some(Ok(record)),
                        Ok(None) => continue, // Skip invalid/missing data
                        Err(e) => return Some(Err(e)),
                    }
                }
                Err(e) => return Some(Err(e.into())),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;
    
    #[test]
    fn test_parse_temperature_line() {
        let reader = TemperatureReader::new();
        
        // Format: SOUID, DATE, TEMP, Q_TEMP
        let line = "  101, 19500101,  125, 0";
        let record = reader.parse_temperature_line(line, 257).unwrap().unwrap();
        
        assert_eq!(record.staid, 257);
        assert_eq!(record.souid, 101);
        assert_eq!(record.date.format("%Y-%m-%d").to_string(), "1950-01-01");
        assert_eq!(record.temperature, 12.5);
        assert_eq!(record.quality_flag, 0);
    }
    
    #[test]
    fn test_read_temperature_file() -> Result<()> {
        let mut temp_file = NamedTempFile::new()?;
        
        // Write header
        for _ in 0..20 {
            writeln!(temp_file, "Header line")?;
        }
        
        // Write data (SOUID, DATE, TEMP, Q_TEMP format)
        writeln!(temp_file, "  101, 20230101,  125, 0")?;
        writeln!(temp_file, "  101, 20230102,  130, 0")?;
        writeln!(temp_file, "  101, 20230103, -9999, 9")?; // Missing data
        writeln!(temp_file, "  102, 20230101,  145, 0")?;
        
        // Create a temporary file with proper naming convention for station ID extraction
        let test_file = temp_file.path().parent().unwrap().join("TN_STAID000257.txt");
        std::fs::copy(temp_file.path(), &test_file)?;
        
        let reader = TemperatureReader::new();
        let records = reader.read_temperatures(&test_file)?;
        
        // Clean up
        std::fs::remove_file(&test_file)?;
        
        assert_eq!(records.len(), 3); // Missing data excluded
        assert_eq!(records[0].staid, 257); // Station ID from filename
        assert_eq!(records[0].temperature, 12.5);
        assert_eq!(records[1].temperature, 13.0);
        assert_eq!(records[2].temperature, 14.5);
        
        Ok(())
    }
    
    #[test]
    fn test_temperature_validation() {
        let reader = TemperatureReader::new();
        
        // Valid temperature
        let line = "  101, 20230101,  250, 0";
        let record = reader.parse_temperature_line(line, 257).unwrap().unwrap();
        assert!(record.validate().is_ok());
        
        // Invalid temperature (out of range)
        let line = "  101, 20230101,  600, 0"; // 60°C
        let record = reader.parse_temperature_line(line, 257).unwrap().unwrap();
        assert!(record.validate().is_err());
    }
}