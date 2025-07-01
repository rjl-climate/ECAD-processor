pub mod inspector;
pub mod multi_processor;
pub mod processor;
pub mod temp_manager;

pub use inspector::{ArchiveInspector, ArchiveMetadata};
pub use multi_processor::{ArchiveInfo, MultiArchiveProcessor};
pub use processor::ArchiveProcessor;
pub use temp_manager::TempFileManager;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WeatherMetric {
    Temperature(TemperatureType),
    Precipitation,
    WindSpeed,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TemperatureType {
    Minimum, // TN
    Maximum, // TX
    Average, // TG
}

impl WeatherMetric {
    pub fn from_file_prefix(prefix: &str) -> Option<Self> {
        match prefix {
            "TN" => Some(WeatherMetric::Temperature(TemperatureType::Minimum)),
            "TX" => Some(WeatherMetric::Temperature(TemperatureType::Maximum)),
            "TG" => Some(WeatherMetric::Temperature(TemperatureType::Average)),
            "RR" => Some(WeatherMetric::Precipitation),
            "FG" => Some(WeatherMetric::WindSpeed),
            _ => None,
        }
    }

    pub fn to_file_prefix(&self) -> &'static str {
        match self {
            WeatherMetric::Temperature(TemperatureType::Minimum) => "TN",
            WeatherMetric::Temperature(TemperatureType::Maximum) => "TX",
            WeatherMetric::Temperature(TemperatureType::Average) => "TG",
            WeatherMetric::Precipitation => "RR",
            WeatherMetric::WindSpeed => "FG",
        }
    }

    pub fn display_name(&self) -> &'static str {
        match self {
            WeatherMetric::Temperature(TemperatureType::Minimum) => "Temperature (Min)",
            WeatherMetric::Temperature(TemperatureType::Maximum) => "Temperature (Max)",
            WeatherMetric::Temperature(TemperatureType::Average) => "Temperature (Avg)",
            WeatherMetric::Precipitation => "Precipitation",
            WeatherMetric::WindSpeed => "Wind Speed",
        }
    }

    pub fn units(&self) -> &'static str {
        match self {
            WeatherMetric::Temperature(_) => "0.1°C",
            WeatherMetric::Precipitation => "0.1mm",
            WeatherMetric::WindSpeed => "0.1 m/s",
        }
    }
}

impl std::fmt::Display for WeatherMetric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display_name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_weather_metric_from_prefix() {
        assert_eq!(
            WeatherMetric::from_file_prefix("TN"),
            Some(WeatherMetric::Temperature(TemperatureType::Minimum))
        );
        assert_eq!(
            WeatherMetric::from_file_prefix("TX"),
            Some(WeatherMetric::Temperature(TemperatureType::Maximum))
        );
        assert_eq!(
            WeatherMetric::from_file_prefix("TG"),
            Some(WeatherMetric::Temperature(TemperatureType::Average))
        );
        assert_eq!(
            WeatherMetric::from_file_prefix("RR"),
            Some(WeatherMetric::Precipitation)
        );
        assert_eq!(
            WeatherMetric::from_file_prefix("FG"),
            Some(WeatherMetric::WindSpeed)
        );
        assert_eq!(WeatherMetric::from_file_prefix("XX"), None);
    }

    #[test]
    fn test_weather_metric_to_prefix() {
        assert_eq!(
            WeatherMetric::Temperature(TemperatureType::Minimum).to_file_prefix(),
            "TN"
        );
        assert_eq!(
            WeatherMetric::Temperature(TemperatureType::Maximum).to_file_prefix(),
            "TX"
        );
        assert_eq!(
            WeatherMetric::Temperature(TemperatureType::Average).to_file_prefix(),
            "TG"
        );
        assert_eq!(WeatherMetric::Precipitation.to_file_prefix(), "RR");
        assert_eq!(WeatherMetric::WindSpeed.to_file_prefix(), "FG");
    }

    #[test]
    fn test_weather_metric_display() {
        assert_eq!(
            WeatherMetric::Temperature(TemperatureType::Minimum).display_name(),
            "Temperature (Min)"
        );
        assert_eq!(WeatherMetric::Precipitation.display_name(), "Precipitation");
        assert_eq!(WeatherMetric::WindSpeed.display_name(), "Wind Speed");
    }

    #[test]
    fn test_weather_metric_units() {
        assert_eq!(
            WeatherMetric::Temperature(TemperatureType::Minimum).units(),
            "0.1°C"
        );
        assert_eq!(WeatherMetric::Precipitation.units(), "0.1mm");
        assert_eq!(WeatherMetric::WindSpeed.units(), "0.1 m/s");
    }
}
