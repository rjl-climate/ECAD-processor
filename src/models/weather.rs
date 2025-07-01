use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::error::{ProcessingError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum PhysicalValidity {
    Valid,   // Within normal physical limits
    Suspect, // Unusual but physically possible
    Invalid, // Physically impossible
}

impl PhysicalValidity {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "Valid" => Some(PhysicalValidity::Valid),
            "Suspect" => Some(PhysicalValidity::Suspect),
            "Invalid" => Some(PhysicalValidity::Invalid),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum DataQuality {
    Valid,           // ECAD=0 AND physically valid
    SuspectOriginal, // ECAD=1, physically valid
    SuspectRange,    // ECAD=0, physically suspect
    SuspectBoth,     // ECAD=1 AND physically suspect
    Invalid,         // Physically impossible (regardless of ECAD flag)
    Missing,         // ECAD=9
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct WeatherRecord {
    // Core station info (always present)
    pub station_id: u32,
    pub station_name: String,
    pub date: NaiveDate,

    #[validate(range(min = -90.0, max = 90.0))]
    pub latitude: f64,

    #[validate(range(min = -180.0, max = 180.0))]
    pub longitude: f64,

    // Optional temperature metrics (0.1°C units)
    #[validate(range(min = -50.0, max = 50.0))]
    pub temp_min: Option<f32>,

    #[validate(range(min = -50.0, max = 50.0))]
    pub temp_max: Option<f32>,

    #[validate(range(min = -50.0, max = 50.0))]
    pub temp_avg: Option<f32>,

    // Optional precipitation (0.1mm units)
    #[validate(range(min = 0.0, max = 1000.0))]
    pub precipitation: Option<f32>,

    // Optional wind speed (0.1 m/s units)
    #[validate(range(min = 0.0, max = 100.0))]
    pub wind_speed: Option<f32>,

    // Quality flags per metric type (original ECAD flags)
    pub temp_quality: Option<String>, // "000", "001", etc.
    pub precip_quality: Option<String>,
    pub wind_quality: Option<String>,

    // Physical validation assessments (our validation layer)
    pub temp_validation: Option<PhysicalValidity>,
    pub precip_validation: Option<PhysicalValidity>,
    pub wind_validation: Option<PhysicalValidity>,
}

impl WeatherRecord {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        station_id: u32,
        station_name: String,
        date: NaiveDate,
        latitude: f64,
        longitude: f64,
        temp_min: Option<f32>,
        temp_max: Option<f32>,
        temp_avg: Option<f32>,
        precipitation: Option<f32>,
        wind_speed: Option<f32>,
        temp_quality: Option<String>,
        precip_quality: Option<String>,
        wind_quality: Option<String>,
    ) -> Self {
        let mut record = Self {
            station_id,
            station_name,
            date,
            latitude,
            longitude,
            temp_min,
            temp_max,
            temp_avg,
            precipitation,
            wind_speed,
            temp_quality,
            precip_quality,
            wind_quality,
            temp_validation: None,
            precip_validation: None,
            wind_validation: None,
        };

        // Automatically perform physical validation
        record.perform_physical_validation();
        record
    }

    /// Create a WeatherRecord without automatic validation (for reading from file)
    #[allow(clippy::too_many_arguments)]
    pub fn new_raw(
        station_id: u32,
        station_name: String,
        date: NaiveDate,
        latitude: f64,
        longitude: f64,
        temp_min: Option<f32>,
        temp_max: Option<f32>,
        temp_avg: Option<f32>,
        precipitation: Option<f32>,
        wind_speed: Option<f32>,
        temp_quality: Option<String>,
        precip_quality: Option<String>,
        wind_quality: Option<String>,
        temp_validation: Option<PhysicalValidity>,
        precip_validation: Option<PhysicalValidity>,
        wind_validation: Option<PhysicalValidity>,
    ) -> Self {
        Self {
            station_id,
            station_name,
            date,
            latitude,
            longitude,
            temp_min,
            temp_max,
            temp_avg,
            precipitation,
            wind_speed,
            temp_quality,
            precip_quality,
            wind_quality,
            temp_validation,
            precip_validation,
            wind_validation,
        }
    }

    pub fn builder() -> WeatherRecordBuilder {
        WeatherRecordBuilder::new()
    }

    pub fn validate_relationships(&self) -> Result<()> {
        // Validate temperature relationships if all three are present
        if let (Some(min), Some(avg), Some(max)) = (self.temp_min, self.temp_avg, self.temp_max) {
            let tolerance = 1.0; // Increased tolerance for real-world data

            if min > avg + tolerance {
                return Err(ProcessingError::TemperatureValidation {
                    message: format!(
                        "Min temperature {} > Avg temperature {} (tolerance={})",
                        min, avg, tolerance
                    ),
                });
            }

            if avg > max + tolerance {
                return Err(ProcessingError::TemperatureValidation {
                    message: format!(
                        "Avg temperature {} > Max temperature {} (tolerance={})",
                        avg, max, tolerance
                    ),
                });
            }
        }

        self.validate()?;
        Ok(())
    }

    pub fn has_temperature_data(&self) -> bool {
        self.temp_min.is_some() || self.temp_max.is_some() || self.temp_avg.is_some()
    }

    pub fn has_complete_temperature(&self) -> bool {
        self.temp_min.is_some() && self.temp_max.is_some() && self.temp_avg.is_some()
    }

    pub fn has_precipitation(&self) -> bool {
        self.precipitation.is_some()
    }

    pub fn has_wind_speed(&self) -> bool {
        self.wind_speed.is_some()
    }

    pub fn available_metrics(&self) -> Vec<&str> {
        let mut metrics = Vec::new();
        if self.has_temperature_data() {
            metrics.push("temperature");
        }
        if self.has_precipitation() {
            metrics.push("precipitation");
        }
        if self.has_wind_speed() {
            metrics.push("wind_speed");
        }
        metrics
    }

    pub fn metric_coverage_score(&self) -> f32 {
        let total_metrics = 3.0; // temp, precip, wind
        let available = self.available_metrics().len() as f32;
        available / total_metrics
    }

    pub fn temperature_range(&self) -> Option<f32> {
        match (self.temp_min, self.temp_max) {
            (Some(min), Some(max)) => Some(max - min),
            _ => None,
        }
    }

    pub fn has_valid_temperature_data(&self) -> bool {
        self.temp_quality.as_ref().is_some_and(|q| q == "000")
    }

    pub fn has_valid_precipitation_data(&self) -> bool {
        self.precip_quality.as_ref().is_some_and(|q| q == "0")
    }

    pub fn has_valid_wind_data(&self) -> bool {
        self.wind_quality.as_ref().is_some_and(|q| q == "0")
    }

    pub fn has_suspect_data(&self) -> bool {
        self.temp_quality.as_ref().is_some_and(|q| q.contains('1'))
            || self
                .precip_quality
                .as_ref()
                .is_some_and(|q| q.contains('1'))
            || self.wind_quality.as_ref().is_some_and(|q| q.contains('1'))
    }

    pub fn has_missing_data(&self) -> bool {
        self.temp_quality.as_ref().is_some_and(|q| q.contains('9'))
            || self
                .precip_quality
                .as_ref()
                .is_some_and(|q| q.contains('9'))
            || self.wind_quality.as_ref().is_some_and(|q| q.contains('9'))
    }

    /// Perform physical validation on all metrics
    pub fn perform_physical_validation(&mut self) {
        self.temp_validation = self.validate_temperature_physics();
        self.precip_validation = self.validate_precipitation_physics();
        self.wind_validation = self.validate_wind_physics();
    }

    /// Validate temperature values against physical limits
    fn validate_temperature_physics(&self) -> Option<PhysicalValidity> {
        let temps = [self.temp_min, self.temp_max, self.temp_avg];
        let existing_temps: Vec<f32> = temps.into_iter().flatten().collect();

        if existing_temps.is_empty() {
            return None;
        }

        for &temp in &existing_temps {
            // Physical impossibility (below absolute zero or above physically possible)
            if !(-90.0..=60.0).contains(&temp) {
                return Some(PhysicalValidity::Invalid);
            }

            // Suspect but possible for UK/Ireland climate
            if !(-35.0..=45.0).contains(&temp) {
                return Some(PhysicalValidity::Suspect);
            }
        }

        Some(PhysicalValidity::Valid)
    }

    /// Validate precipitation values against physical limits
    fn validate_precipitation_physics(&self) -> Option<PhysicalValidity> {
        if let Some(precip) = self.precipitation {
            // Physical impossibility
            if !(0.0..=2000.0).contains(&precip) {
                return Some(PhysicalValidity::Invalid);
            }

            // Suspect but possible (extreme rainfall events)
            if precip > 500.0 {
                return Some(PhysicalValidity::Suspect);
            }

            Some(PhysicalValidity::Valid)
        } else {
            None
        }
    }

    /// Validate wind speed values against physical limits
    fn validate_wind_physics(&self) -> Option<PhysicalValidity> {
        if let Some(wind) = self.wind_speed {
            // Physical impossibility
            if !(0.0..=120.0).contains(&wind) {
                return Some(PhysicalValidity::Invalid);
            }

            // Suspect but possible (hurricane-force winds)
            if wind > 50.0 {
                return Some(PhysicalValidity::Suspect);
            }

            Some(PhysicalValidity::Valid)
        } else {
            None
        }
    }

    /// Assess overall temperature data quality combining ECAD flags and physical validation
    pub fn assess_temperature_quality(&self) -> DataQuality {
        match (self.temp_quality.as_deref(), self.temp_validation) {
            (Some(q), _) if q.contains('9') => DataQuality::Missing,
            (_, Some(PhysicalValidity::Invalid)) => DataQuality::Invalid,
            (Some(q), Some(PhysicalValidity::Suspect)) if q.contains('1') => {
                DataQuality::SuspectBoth
            }
            (Some(q), _) if q.contains('1') => DataQuality::SuspectOriginal,
            (_, Some(PhysicalValidity::Suspect)) => DataQuality::SuspectRange,
            _ => DataQuality::Valid,
        }
    }

    /// Assess overall precipitation data quality
    pub fn assess_precipitation_quality(&self) -> DataQuality {
        match (self.precip_quality.as_deref(), self.precip_validation) {
            (Some("9"), _) => DataQuality::Missing,
            (_, Some(PhysicalValidity::Invalid)) => DataQuality::Invalid,
            (Some("1"), Some(PhysicalValidity::Suspect)) => DataQuality::SuspectBoth,
            (Some("1"), _) => DataQuality::SuspectOriginal,
            (_, Some(PhysicalValidity::Suspect)) => DataQuality::SuspectRange,
            _ => DataQuality::Valid,
        }
    }

    /// Assess overall wind data quality
    pub fn assess_wind_quality(&self) -> DataQuality {
        match (self.wind_quality.as_deref(), self.wind_validation) {
            (Some("9"), _) => DataQuality::Missing,
            (_, Some(PhysicalValidity::Invalid)) => DataQuality::Invalid,
            (Some("1"), Some(PhysicalValidity::Suspect)) => DataQuality::SuspectBoth,
            (Some("1"), _) => DataQuality::SuspectOriginal,
            (_, Some(PhysicalValidity::Suspect)) => DataQuality::SuspectRange,
            _ => DataQuality::Valid,
        }
    }

    /// Check if record has any invalid data (physically impossible)
    pub fn has_invalid_data(&self) -> bool {
        matches!(self.assess_temperature_quality(), DataQuality::Invalid)
            || matches!(self.assess_precipitation_quality(), DataQuality::Invalid)
            || matches!(self.assess_wind_quality(), DataQuality::Invalid)
    }

    /// Check if record has high-quality data (valid with no flags)
    pub fn has_high_quality_data(&self) -> bool {
        let temp_ok = self.temp_validation.is_none()
            || matches!(self.assess_temperature_quality(), DataQuality::Valid);
        let precip_ok = self.precip_validation.is_none()
            || matches!(self.assess_precipitation_quality(), DataQuality::Valid);
        let wind_ok = self.wind_validation.is_none()
            || matches!(self.assess_wind_quality(), DataQuality::Valid);

        temp_ok && precip_ok && wind_ok
    }
}

pub struct WeatherRecordBuilder {
    station_id: Option<u32>,
    station_name: Option<String>,
    date: Option<NaiveDate>,
    latitude: Option<f64>,
    longitude: Option<f64>,
    temp_min: Option<f32>,
    temp_max: Option<f32>,
    temp_avg: Option<f32>,
    precipitation: Option<f32>,
    wind_speed: Option<f32>,
    temp_quality: Option<String>,
    precip_quality: Option<String>,
    wind_quality: Option<String>,
    temp_validation: Option<PhysicalValidity>,
    precip_validation: Option<PhysicalValidity>,
    wind_validation: Option<PhysicalValidity>,
}

impl Default for WeatherRecordBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl WeatherRecordBuilder {
    pub fn new() -> Self {
        Self {
            station_id: None,
            station_name: None,
            date: None,
            latitude: None,
            longitude: None,
            temp_min: None,
            temp_max: None,
            temp_avg: None,
            precipitation: None,
            wind_speed: None,
            temp_quality: None,
            precip_quality: None,
            wind_quality: None,
            temp_validation: None,
            precip_validation: None,
            wind_validation: None,
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

    pub fn temp_min(mut self, temp: f32) -> Self {
        self.temp_min = Some(temp);
        self
    }

    pub fn temp_max(mut self, temp: f32) -> Self {
        self.temp_max = Some(temp);
        self
    }

    pub fn temp_avg(mut self, temp: f32) -> Self {
        self.temp_avg = Some(temp);
        self
    }

    pub fn temperatures(mut self, min: f32, avg: f32, max: f32) -> Self {
        self.temp_min = Some(min);
        self.temp_avg = Some(avg);
        self.temp_max = Some(max);
        self
    }

    pub fn precipitation(mut self, precip: f32) -> Self {
        self.precipitation = Some(precip);
        self
    }

    pub fn wind_speed(mut self, speed: f32) -> Self {
        self.wind_speed = Some(speed);
        self
    }

    pub fn temp_quality(mut self, quality: String) -> Self {
        self.temp_quality = Some(quality);
        self
    }

    pub fn precip_quality(mut self, quality: String) -> Self {
        self.precip_quality = Some(quality);
        self
    }

    pub fn wind_quality(mut self, quality: String) -> Self {
        self.wind_quality = Some(quality);
        self
    }

    pub fn build(self) -> Result<WeatherRecord> {
        let mut record = WeatherRecord {
            station_id: self
                .station_id
                .ok_or_else(|| ProcessingError::MissingData("station_id".to_string()))?,
            station_name: self
                .station_name
                .ok_or_else(|| ProcessingError::MissingData("station_name".to_string()))?,
            date: self
                .date
                .ok_or_else(|| ProcessingError::MissingData("date".to_string()))?,
            latitude: self
                .latitude
                .ok_or_else(|| ProcessingError::MissingData("latitude".to_string()))?,
            longitude: self
                .longitude
                .ok_or_else(|| ProcessingError::MissingData("longitude".to_string()))?,
            temp_min: self.temp_min,
            temp_max: self.temp_max,
            temp_avg: self.temp_avg,
            precipitation: self.precipitation,
            wind_speed: self.wind_speed,
            temp_quality: self.temp_quality,
            precip_quality: self.precip_quality,
            wind_quality: self.wind_quality,
            temp_validation: self.temp_validation,
            precip_validation: self.precip_validation,
            wind_validation: self.wind_validation,
        };

        // Perform physical validation if not already set
        if record.temp_validation.is_none()
            || record.precip_validation.is_none()
            || record.wind_validation.is_none()
        {
            record.perform_physical_validation();
        }

        record.validate_relationships()?;
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_weather_record_creation() {
        let date = NaiveDate::from_ymd_opt(2023, 7, 15).unwrap();

        let record = WeatherRecord::new(
            12345,
            "London Station".to_string(),
            date,
            51.5074,
            -0.1278,
            Some(10.0),
            Some(20.0),
            Some(15.0),
            Some(5.0),
            Some(3.2),
            Some("000".to_string()),
            Some("0".to_string()),
            Some("0".to_string()),
        );

        assert_eq!(record.station_id, 12345);
        assert_eq!(record.station_name, "London Station");
        assert!(record.has_complete_temperature());
        assert!(record.has_precipitation());
        assert!(record.has_wind_speed());
        assert_eq!(record.available_metrics().len(), 3);
        assert_eq!(record.metric_coverage_score(), 1.0);
    }

    #[test]
    fn test_temperature_validation() {
        let date = NaiveDate::from_ymd_opt(2023, 7, 15).unwrap();

        let record = WeatherRecord::new(
            12345,
            "Test Station".to_string(),
            date,
            51.5074,
            -0.1278,
            Some(10.0),
            Some(20.0),
            Some(15.0),
            None,
            None,
            Some("000".to_string()),
            None,
            None,
        );

        assert!(record.validate_relationships().is_ok());
        assert_eq!(record.temperature_range(), Some(10.0));
    }

    #[test]
    fn test_invalid_temperature_relationship() {
        let date = NaiveDate::from_ymd_opt(2023, 7, 15).unwrap();

        let record = WeatherRecord::new(
            12345,
            "Test Station".to_string(),
            date,
            51.5074,
            -0.1278,
            Some(20.0), // min > avg
            Some(10.0), // max < avg
            Some(15.0),
            None,
            None,
            Some("000".to_string()),
            None,
            None,
        );

        assert!(record.validate_relationships().is_err());
    }

    #[test]
    fn test_builder_pattern() {
        let date = NaiveDate::from_ymd_opt(2023, 7, 15).unwrap();

        let record = WeatherRecord::builder()
            .station_id(12345)
            .station_name("Test Station".to_string())
            .date(date)
            .coordinates(51.5074, -0.1278)
            .temperatures(10.0, 15.0, 20.0)
            .precipitation(5.5)
            .wind_speed(3.2)
            .temp_quality("000".to_string())
            .build()
            .unwrap();

        assert_eq!(record.station_id, 12345);
        assert_eq!(record.station_name, "Test Station");
        assert!(record.validate_relationships().is_ok());
        assert!(record.has_complete_temperature());
        assert!(record.has_precipitation());
        assert!(record.has_wind_speed());
    }

    #[test]
    fn test_partial_data() {
        let date = NaiveDate::from_ymd_opt(2023, 7, 15).unwrap();

        let record = WeatherRecord::builder()
            .station_id(12345)
            .station_name("Test Station".to_string())
            .date(date)
            .coordinates(51.5074, -0.1278)
            .temp_min(10.0)
            .precipitation(5.5)
            .build()
            .unwrap();

        assert!(record.has_temperature_data());
        assert!(!record.has_complete_temperature());
        assert!(record.has_precipitation());
        assert!(!record.has_wind_speed());
        assert_eq!(record.available_metrics().len(), 2);
        assert!((record.metric_coverage_score() - 0.666).abs() < 0.01);
    }

    #[test]
    fn test_quality_flags() {
        let date = NaiveDate::from_ymd_opt(2023, 7, 15).unwrap();

        let record = WeatherRecord::builder()
            .station_id(12345)
            .station_name("Test Station".to_string())
            .date(date)
            .coordinates(51.5074, -0.1278)
            .temp_min(10.0)
            .temp_quality("001".to_string()) // suspect data
            .build()
            .unwrap();

        assert!(!record.has_valid_temperature_data());
        assert!(record.has_suspect_data());
        assert!(!record.has_missing_data());
    }
}
