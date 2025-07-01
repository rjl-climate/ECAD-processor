use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::error::{ProcessingError, Result};

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct ConsolidatedRecord {
    pub station_id: u32,
    pub station_name: String,
    pub date: NaiveDate,
    
    #[validate(range(min = -90.0, max = 90.0))]
    pub latitude: f64,
    
    #[validate(range(min = -180.0, max = 180.0))]
    pub longitude: f64,
    
    #[validate(range(min = -50.0, max = 50.0))]
    pub min_temp: f32,
    
    #[validate(range(min = -50.0, max = 50.0))]
    pub max_temp: f32,
    
    #[validate(range(min = -50.0, max = 50.0))]
    pub avg_temp: f32,
    
    pub quality_flags: String,
}

impl ConsolidatedRecord {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        station_id: u32,
        station_name: String,
        date: NaiveDate,
        latitude: f64,
        longitude: f64,
        min_temp: f32,
        max_temp: f32,
        avg_temp: f32,
        quality_flags: String,
    ) -> Self {
        Self {
            station_id,
            station_name,
            date,
            latitude,
            longitude,
            min_temp,
            max_temp,
            avg_temp,
            quality_flags,
        }
    }
    
    pub fn validate_relationships(&self) -> Result<()> {
        let tolerance = 1.0; // Increased tolerance for real-world data
        
        if self.min_temp > self.avg_temp + tolerance {
            return Err(ProcessingError::TemperatureValidation {
                message: format!(
                    "Min temperature {} > Avg temperature {} (tolerance={})",
                    self.min_temp, self.avg_temp, tolerance
                ),
            });
        }
        
        if self.avg_temp > self.max_temp + tolerance {
            return Err(ProcessingError::TemperatureValidation {
                message: format!(
                    "Avg temperature {} > Max temperature {} (tolerance={})",
                    self.avg_temp, self.max_temp, tolerance
                ),
            });
        }
        
        self.validate()?;
        
        Ok(())
    }
    
    pub fn temperature_range(&self) -> f32 {
        self.max_temp - self.min_temp
    }
    
    pub fn has_valid_data(&self) -> bool {
        self.quality_flags == "000"
    }
    
    pub fn has_suspect_data(&self) -> bool {
        self.quality_flags.contains('1')
    }
    
    pub fn has_missing_data(&self) -> bool {
        self.quality_flags.contains('9')
    }
    
    pub fn is_complete(&self) -> bool {
        !self.has_missing_data()
    }
}

pub struct ConsolidatedRecordBuilder {
    station_id: Option<u32>,
    station_name: Option<String>,
    date: Option<NaiveDate>,
    latitude: Option<f64>,
    longitude: Option<f64>,
    min_temp: Option<f32>,
    max_temp: Option<f32>,
    avg_temp: Option<f32>,
    quality_flags: Option<String>,
}

impl Default for ConsolidatedRecordBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ConsolidatedRecordBuilder {
    pub fn new() -> Self {
        Self {
            station_id: None,
            station_name: None,
            date: None,
            latitude: None,
            longitude: None,
            min_temp: None,
            max_temp: None,
            avg_temp: None,
            quality_flags: None,
        }
    }
    
    pub fn station_id(mut self, id: u32) -> Self {
        self.station_id = Some(id);
        self
    }
    
    pub fn station_name(mut self, name: String) -> Self {
        self.station_name = Some(name);
        self
    }
    
    pub fn date(mut self, date: NaiveDate) -> Self {
        self.date = Some(date);
        self
    }
    
    pub fn coordinates(mut self, latitude: f64, longitude: f64) -> Self {
        self.latitude = Some(latitude);
        self.longitude = Some(longitude);
        self
    }
    
    pub fn temperatures(mut self, min: f32, avg: f32, max: f32) -> Self {
        self.min_temp = Some(min);
        self.avg_temp = Some(avg);
        self.max_temp = Some(max);
        self
    }
    
    pub fn quality_flags(mut self, flags: String) -> Self {
        self.quality_flags = Some(flags);
        self
    }
    
    pub fn build(self) -> Result<ConsolidatedRecord> {
        let record = ConsolidatedRecord::new(
            self.station_id.ok_or_else(|| ProcessingError::MissingData("station_id".to_string()))?,
            self.station_name.ok_or_else(|| ProcessingError::MissingData("station_name".to_string()))?,
            self.date.ok_or_else(|| ProcessingError::MissingData("date".to_string()))?,
            self.latitude.ok_or_else(|| ProcessingError::MissingData("latitude".to_string()))?,
            self.longitude.ok_or_else(|| ProcessingError::MissingData("longitude".to_string()))?,
            self.min_temp.ok_or_else(|| ProcessingError::MissingData("min_temp".to_string()))?,
            self.max_temp.ok_or_else(|| ProcessingError::MissingData("max_temp".to_string()))?,
            self.avg_temp.ok_or_else(|| ProcessingError::MissingData("avg_temp".to_string()))?,
            self.quality_flags.ok_or_else(|| ProcessingError::MissingData("quality_flags".to_string()))?,
        );
        
        record.validate_relationships()?;
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_consolidated_record_validation() {
        let date = NaiveDate::from_ymd_opt(2023, 7, 15).unwrap();
        
        let record = ConsolidatedRecord::new(
            12345,
            "London Station".to_string(),
            date,
            51.5074,
            -0.1278,
            10.0,
            20.0,
            15.0,
            "000".to_string(),
        );
        
        assert!(record.validate_relationships().is_ok());
        assert!(record.has_valid_data());
        assert!(!record.has_suspect_data());
        assert!(!record.has_missing_data());
        assert!(record.is_complete());
        assert_eq!(record.temperature_range(), 10.0);
    }
    
    #[test]
    fn test_invalid_temperature_relationship() {
        let date = NaiveDate::from_ymd_opt(2023, 7, 15).unwrap();
        
        let record = ConsolidatedRecord::new(
            12345,
            "London Station".to_string(),
            date,
            51.5074,
            -0.1278,
            20.0,  // min > avg
            10.0,  // max < avg
            15.0,
            "000".to_string(),
        );
        
        assert!(record.validate_relationships().is_err());
    }
    
    #[test]
    fn test_builder_pattern() {
        let date = NaiveDate::from_ymd_opt(2023, 7, 15).unwrap();
        
        let record = ConsolidatedRecordBuilder::new()
            .station_id(12345)
            .station_name("Test Station".to_string())
            .date(date)
            .coordinates(51.5074, -0.1278)
            .temperatures(10.0, 15.0, 20.0)
            .quality_flags("000".to_string())
            .build()
            .unwrap();
            
        assert_eq!(record.station_id, 12345);
        assert_eq!(record.station_name, "Test Station");
        assert!(record.validate_relationships().is_ok());
    }
}