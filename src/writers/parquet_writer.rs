use crate::error::Result;
use crate::models::ConsolidatedRecord;
use crate::utils::constants::DEFAULT_ROW_GROUP_SIZE;
use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::Datelike;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, GzipLevel};
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

pub struct ParquetWriter {
    compression: Compression,
    row_group_size: usize,
}

impl ParquetWriter {
    pub fn new() -> Self {
        Self {
            compression: Compression::SNAPPY,
            row_group_size: DEFAULT_ROW_GROUP_SIZE,
        }
    }
    
    pub fn with_compression(mut self, compression: &str) -> Result<Self> {
        self.compression = match compression.to_lowercase().as_str() {
            "snappy" => Compression::SNAPPY,
            "gzip" => Compression::GZIP(GzipLevel::default()),
            "lz4" => Compression::LZ4,
            "zstd" => Compression::ZSTD(parquet::basic::ZstdLevel::default()),
            "none" => Compression::UNCOMPRESSED,
            _ => return Err(crate::error::ProcessingError::Config(
                format!("Unsupported compression: {}", compression)
            )),
        };
        Ok(self)
    }
    
    pub fn with_row_group_size(mut self, size: usize) -> Self {
        self.row_group_size = size;
        self
    }
    
    /// Write consolidated records to Parquet file
    pub fn write_records(&self, records: &[ConsolidatedRecord], path: &Path) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        
        let schema = self.create_schema();
        let batch = self.records_to_batch(records, schema.clone())?;
        
        let file = File::create(path)?;
        let props = WriterProperties::builder()
            .set_compression(self.compression)
            .set_max_row_group_size(self.row_group_size)
            .build();
        
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
        
        Ok(())
    }
    
    /// Write records in batches for memory efficiency
    pub fn write_records_batched(
        &self,
        records: &[ConsolidatedRecord],
        path: &Path,
        batch_size: usize,
    ) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        
        let schema = self.create_schema();
        let file = File::create(path)?;
        let props = WriterProperties::builder()
            .set_compression(self.compression)
            .set_max_row_group_size(self.row_group_size)
            .build();
        
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;
        
        // Write in batches
        for chunk in records.chunks(batch_size) {
            let batch = self.records_to_batch(chunk, schema.clone())?;
            writer.write(&batch)?;
        }
        
        writer.close()?;
        Ok(())
    }
    
    /// Create Arrow schema for temperature data
    fn create_schema(&self) -> Arc<Schema> {
        let fields = vec![
            Field::new("station_id", DataType::UInt32, false),
            Field::new("station_name", DataType::Utf8, false),
            Field::new("date", DataType::Date32, false),
            Field::new("latitude", DataType::Float64, false),
            Field::new("longitude", DataType::Float64, false),
            Field::new("min_temp", DataType::Float32, false),
            Field::new("max_temp", DataType::Float32, false),
            Field::new("avg_temp", DataType::Float32, false),
            Field::new("quality_flags", DataType::Utf8, false),
        ];
        
        Arc::new(Schema::new(fields))
    }
    
    /// Convert records to Arrow RecordBatch
    fn records_to_batch(
        &self,
        records: &[ConsolidatedRecord],
        schema: Arc<Schema>,
    ) -> Result<RecordBatch> {
        // Extract data into separate vectors
        let station_ids: Vec<u32> = records.iter().map(|r| r.station_id).collect();
        let station_names: Vec<String> = records.iter().map(|r| r.station_name.clone()).collect();
        let dates: Vec<i32> = records.iter()
            .map(|r| r.date.num_days_from_ce())
            .collect();
        let latitudes: Vec<f64> = records.iter().map(|r| r.latitude).collect();
        let longitudes: Vec<f64> = records.iter().map(|r| r.longitude).collect();
        let min_temps: Vec<f32> = records.iter().map(|r| r.min_temp).collect();
        let max_temps: Vec<f32> = records.iter().map(|r| r.max_temp).collect();
        let avg_temps: Vec<f32> = records.iter().map(|r| r.avg_temp).collect();
        let quality_flags: Vec<String> = records.iter().map(|r| r.quality_flags.clone()).collect();
        
        // Create Arrow arrays
        let station_id_array = Arc::new(UInt32Array::from(station_ids));
        let station_name_array = Arc::new(StringArray::from(station_names));
        let date_array = Arc::new(Date32Array::from(dates));
        let latitude_array = Arc::new(Float64Array::from(latitudes));
        let longitude_array = Arc::new(Float64Array::from(longitudes));
        let min_temp_array = Arc::new(Float32Array::from(min_temps));
        let max_temp_array = Arc::new(Float32Array::from(max_temps));
        let avg_temp_array = Arc::new(Float32Array::from(avg_temps));
        let quality_flags_array = Arc::new(StringArray::from(quality_flags));
        
        // Create record batch
        let batch = RecordBatch::try_new(
            schema,
            vec![
                station_id_array,
                station_name_array,
                date_array,
                latitude_array,
                longitude_array,
                min_temp_array,
                max_temp_array,
                avg_temp_array,
                quality_flags_array,
            ],
        )?;
        
        Ok(batch)
    }
    
    /// Get file statistics
    pub fn get_file_info(&self, path: &Path) -> Result<ParquetFileInfo> {
        use parquet::file::reader::{FileReader, SerializedFileReader};
        use std::fs::File;
        
        let file = File::open(path)?;
        let reader = SerializedFileReader::new(file)?;
        let metadata = reader.metadata();
        
        let file_metadata = metadata.file_metadata();
        let row_groups = metadata.num_row_groups();
        let total_rows = file_metadata.num_rows();
        let file_size = std::fs::metadata(path)?.len();
        
        let mut row_group_sizes = Vec::new();
        for i in 0..row_groups {
            let rg_metadata = metadata.row_group(i);
            row_group_sizes.push(rg_metadata.num_rows());
        }
        
        Ok(ParquetFileInfo {
            total_rows,
            row_groups: row_groups as i32,
            row_group_sizes,
            file_size,
            compression: self.compression,
        })
    }
}

impl Default for ParquetWriter {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct ParquetFileInfo {
    pub total_rows: i64,
    pub row_groups: i32,
    pub row_group_sizes: Vec<i64>,
    pub file_size: u64,
    pub compression: Compression,
}

impl ParquetFileInfo {
    pub fn summary(&self) -> String {
        format!(
            "Parquet File Summary:\n\
            - Total rows: {}\n\
            - Row groups: {}\n\
            - File size: {:.2} MB\n\
            - Compression: {:?}\n\
            - Avg rows per group: {:.0}",
            self.total_rows,
            self.row_groups,
            self.file_size as f64 / 1_048_576.0, // Convert to MB
            self.compression,
            self.total_rows as f64 / self.row_groups as f64
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::ConsolidatedRecord;
    use chrono::NaiveDate;
    use tempfile::NamedTempFile;
    
    #[test]
    fn test_write_empty_records() {
        let writer = ParquetWriter::new();
        let temp_file = NamedTempFile::new().unwrap();
        
        let result = writer.write_records(&[], temp_file.path());
        assert!(result.is_ok());
    }
    
    #[test]
    fn test_write_single_record() -> Result<()> {
        let writer = ParquetWriter::new();
        let temp_file = NamedTempFile::new().unwrap();
        
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
        
        writer.write_records(&[record], temp_file.path())?;
        
        // Verify file was created and has content
        let metadata = std::fs::metadata(temp_file.path())?;
        assert!(metadata.len() > 0);
        
        Ok(())
    }
    
    #[test]
    fn test_different_compressions() -> Result<()> {
        let compressions = ["snappy", "gzip", "lz4", "zstd", "none"];
        
        for compression in &compressions {
            let writer = ParquetWriter::new().with_compression(compression)?;
            let temp_file = NamedTempFile::new().unwrap();
            
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
            
            let result = writer.write_records(&[record], temp_file.path());
            assert!(result.is_ok(), "Failed with compression: {}", compression);
        }
        
        Ok(())
    }
}