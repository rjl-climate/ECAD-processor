use tempfile::TempDir;
use ecad_processor::models::{ConsolidatedRecord, StationMetadata};
use ecad_processor::writers::ParquetWriter;
use chrono::NaiveDate;
use validator::Validate;

#[tokio::test]
async fn test_cli_integration() {
    // This is a basic integration test to ensure the CLI structure works
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    
    // Create a simple test record
    let date = NaiveDate::from_ymd_opt(2023, 7, 15).unwrap();
    let record = ConsolidatedRecord::new(
        12345,
        "Test Station".to_string(),
        date,
        51.5074,
        -0.1278,
        15.0,
        25.0,
        20.0,
        "000".to_string(),
    );
    
    // Test Parquet writing
    let output_path = temp_dir.path().join("test.parquet");
    let writer = ParquetWriter::new();
    writer.write_records(&[record], &output_path).unwrap();
    
    // Verify file was created
    assert!(output_path.exists());
    
    // Get file info
    let file_info = writer.get_file_info(&output_path).unwrap();
    assert_eq!(file_info.total_rows, 1);
    
    println!("Integration test passed!");
}

#[test]
fn test_station_metadata() {
    let station = StationMetadata::new(
        12345,
        "London Weather Station".to_string(),
        "GB".to_string(),
        51.5074,
        -0.1278,
        Some(35),
    );
    
    assert!(station.is_uk_station());
    assert!(station.is_within_uk_bounds());
    assert!(station.validate().is_ok());
}