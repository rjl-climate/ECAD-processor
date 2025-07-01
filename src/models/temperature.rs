use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

use crate::error::{ProcessingError, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemperatureRecord {
    pub staid: u32,
    pub souid: u32,
    pub date: NaiveDate,
    pub temperature: f32,
    pub quality_flag: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QualityFlag {
    Valid = 0,
    Suspect = 1,
    Missing = 9,
}

impl QualityFlag {
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            0 => Ok(QualityFlag::Valid),
            1 => Ok(QualityFlag::Suspect),
            9 => Ok(QualityFlag::Missing),
            _ => Err(ProcessingError::InvalidQualityFlag(value)),
        }
    }
    
    pub fn as_u8(&self) -> u8 {
        *self as u8
    }
    
    pub fn as_char(&self) -> char {
        match self {
            QualityFlag::Valid => '0',
            QualityFlag::Suspect => '1',
            QualityFlag::Missing => '9',
        }
    }
    
    pub fn should_enforce_strict_validation(&self) -> bool {
        matches!(self, QualityFlag::Valid)
    }
    
    pub fn is_usable(&self) -> bool {
        matches!(self, QualityFlag::Valid | QualityFlag::Suspect)
    }
}

impl TemperatureRecord {
    pub fn new(
        staid: u32,
        souid: u32,
        date: NaiveDate,
        temperature: f32,
        quality_flag: u8,
    ) -> Result<Self> {
        QualityFlag::from_u8(quality_flag)?;
        
        Ok(Self {
            staid,
            souid,
            date,
            temperature,
            quality_flag,
        })
    }
    
    pub fn quality(&self) -> Result<QualityFlag> {
        QualityFlag::from_u8(self.quality_flag)
    }
    
    pub fn is_valid_temperature(&self) -> bool {
        self.temperature >= -50.0 && self.temperature <= 50.0
    }
    
    pub fn validate(&self) -> Result<()> {
        if !self.is_valid_temperature() {
            return Err(ProcessingError::TemperatureValidation {
                message: format!(
                    "Temperature {} is outside valid range [-50, 50]",
                    self.temperature
                ),
            });
        }
        
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct TemperatureSet {
    pub min: Option<TemperatureRecord>,
    pub max: Option<TemperatureRecord>,
    pub avg: Option<TemperatureRecord>,
}

impl Default for TemperatureSet {
    fn default() -> Self {
        Self::new()
    }
}

impl TemperatureSet {
    pub fn new() -> Self {
        Self {
            min: None,
            max: None,
            avg: None,
        }
    }
    
    pub fn validate_relationships(&self) -> Result<()> {
        if let (Some(min), Some(avg), Some(max)) = (&self.min, &self.avg, &self.max) {
            let tolerance = 0.1;
            
            if min.temperature > avg.temperature + tolerance {
                return Err(ProcessingError::TemperatureValidation {
                    message: format!(
                        "Min temperature {} > Avg temperature {}",
                        min.temperature, avg.temperature
                    ),
                });
            }
            
            if avg.temperature > max.temperature + tolerance {
                return Err(ProcessingError::TemperatureValidation {
                    message: format!(
                        "Avg temperature {} > Max temperature {}",
                        avg.temperature, max.temperature
                    ),
                });
            }
        }
        
        Ok(())
    }
    
    pub fn quality_flags_string(&self) -> String {
        let min_flag = self.min
            .as_ref()
            .and_then(|r| r.quality().ok())
            .unwrap_or(QualityFlag::Missing)
            .as_char();
            
        let avg_flag = self.avg
            .as_ref()
            .and_then(|r| r.quality().ok())
            .unwrap_or(QualityFlag::Missing)
            .as_char();
            
        let max_flag = self.max
            .as_ref()
            .and_then(|r| r.quality().ok())
            .unwrap_or(QualityFlag::Missing)
            .as_char();
            
        format!("{}{}{}", min_flag, avg_flag, max_flag)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    
    #[test]
    fn test_quality_flag_conversion() {
        assert_eq!(QualityFlag::from_u8(0).unwrap(), QualityFlag::Valid);
        assert_eq!(QualityFlag::from_u8(1).unwrap(), QualityFlag::Suspect);
        assert_eq!(QualityFlag::from_u8(9).unwrap(), QualityFlag::Missing);
        assert!(QualityFlag::from_u8(5).is_err());
    }
    
    #[test]
    fn test_temperature_validation() {
        let date = NaiveDate::from_ymd_opt(2023, 7, 15).unwrap();
        
        let valid = TemperatureRecord::new(1, 1, date, 25.5, 0).unwrap();
        assert!(valid.validate().is_ok());
        
        let invalid = TemperatureRecord::new(1, 1, date, 55.0, 0).unwrap();
        assert!(invalid.validate().is_err());
    }
    
    #[test]
    fn test_temperature_set_validation() {
        let date = NaiveDate::from_ymd_opt(2023, 7, 15).unwrap();
        
        let mut temp_set = TemperatureSet::new();
        temp_set.min = Some(TemperatureRecord::new(1, 1, date, 10.0, 0).unwrap());
        temp_set.avg = Some(TemperatureRecord::new(1, 1, date, 15.0, 0).unwrap());
        temp_set.max = Some(TemperatureRecord::new(1, 1, date, 20.0, 0).unwrap());
        
        assert!(temp_set.validate_relationships().is_ok());
        assert_eq!(temp_set.quality_flags_string(), "000");
        
        // Invalid relationship
        temp_set.avg = Some(TemperatureRecord::new(1, 1, date, 25.0, 0).unwrap());
        assert!(temp_set.validate_relationships().is_err());
    }
}