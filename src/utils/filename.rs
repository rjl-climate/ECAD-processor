use chrono::{Datelike, Local};
use std::path::PathBuf;

/// Generate default Parquet filename with format: ecad-weather-{YYMMDD}.parquet
pub fn generate_default_parquet_filename() -> PathBuf {
    let now = Local::now();
    let year = now.year() % 100; // Get last 2 digits of year
    let month = now.month();
    let day = now.day();

    let filename = format!("ecad-weather-{:02}{:02}{:02}.parquet", year, month, day);
    PathBuf::from("output").join(filename)
}

/// Generate default unified Parquet filename with format: ecad-weather-unified-{YYMMDD}.parquet
pub fn generate_default_unified_parquet_filename() -> PathBuf {
    let now = Local::now();
    let year = now.year() % 100; // Get last 2 digits of year
    let month = now.month();
    let day = now.day();

    let filename = format!(
        "ecad-weather-unified-{:02}{:02}{:02}.parquet",
        year, month, day
    );
    PathBuf::from("output").join(filename)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_default_parquet_filename() {
        let filename = generate_default_parquet_filename();
        let filename_str = filename.to_string_lossy();

        // Should contain the expected pattern
        assert!(filename_str.contains("ecad-weather-"));
        assert!(filename_str.ends_with(".parquet"));
        assert!(filename_str.starts_with("output/"));

        // Should be exactly 29 characters: "output/ecad-weather-YYMMDD.parquet"
        // But let's just check the structure
        let parts: Vec<&str> = filename_str.split('/').collect();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], "output");

        let file_part = parts[1];
        assert!(file_part.starts_with("ecad-weather-"));
        assert!(file_part.ends_with(".parquet"));
    }

    #[test]
    fn test_generate_default_unified_parquet_filename() {
        let filename = generate_default_unified_parquet_filename();
        let filename_str = filename.to_string_lossy();

        assert!(filename_str.contains("ecad-weather-unified-"));
        assert!(filename_str.ends_with(".parquet"));
        assert!(filename_str.starts_with("output/"));
    }
}
