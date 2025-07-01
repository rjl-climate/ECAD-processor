use serde::{Deserialize, Serialize};
use validator::Validate;

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct StationMetadata {
    pub staid: u32,

    #[validate(length(min = 1))]
    pub name: String,

    pub country: String,

    #[validate(range(min = -90.0, max = 90.0))]
    pub latitude: f64,

    #[validate(range(min = -180.0, max = 180.0))]
    pub longitude: f64,

    pub elevation: Option<i32>,
}

impl StationMetadata {
    pub fn new(
        staid: u32,
        name: String,
        country: String,
        latitude: f64,
        longitude: f64,
        elevation: Option<i32>,
    ) -> Self {
        Self {
            staid,
            name,
            country,
            latitude,
            longitude,
            elevation,
        }
    }

    pub fn is_uk_station(&self) -> bool {
        self.country.to_uppercase() == "GB"
            || self.country.to_uppercase() == "UK"
            || self.country.to_uppercase() == "UNITED KINGDOM"
    }

    pub fn is_within_uk_bounds(&self) -> bool {
        self.latitude >= 49.5
            && self.latitude <= 61.0
            && self.longitude >= -8.0
            && self.longitude <= 2.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_station_validation() {
        let station = StationMetadata::new(
            12345,
            "London Weather Station".to_string(),
            "GB".to_string(),
            51.5074,
            -0.1278,
            Some(35),
        );

        assert!(station.validate().is_ok());
        assert!(station.is_uk_station());
        assert!(station.is_within_uk_bounds());
    }

    #[test]
    fn test_invalid_coordinates() {
        let station = StationMetadata::new(
            12345,
            "Invalid Station".to_string(),
            "GB".to_string(),
            91.0, // Invalid latitude
            -0.1278,
            None,
        );

        assert!(station.validate().is_err());
    }
}
