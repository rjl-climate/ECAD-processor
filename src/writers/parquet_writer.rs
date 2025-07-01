use crate::error::Result;
use crate::models::{ConsolidatedRecord, WeatherRecord};
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
            _ => {
                return Err(crate::error::ProcessingError::Config(format!(
                    "Unsupported compression: {}",
                    compression
                )))
            }
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
        let dates: Vec<i32> = records.iter().map(|r| r.date.num_days_from_ce()).collect();
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

    /// Read sample records from Parquet file
    pub fn read_sample_records(
        &self,
        path: &Path,
        limit: usize,
    ) -> Result<Vec<ConsolidatedRecord>> {
        use arrow::array::*;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let file = File::open(path)?;
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(file)?
            .with_batch_size(limit.min(8192))
            .build()?;

        let mut records = Vec::new();
        let mut total_read = 0;

        for batch_result in parquet_reader {
            let batch = batch_result?;

            // Extract arrays from the batch
            let station_ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| {
                    crate::error::ProcessingError::Config(
                        "Invalid station_id column type".to_string(),
                    )
                })?;
            let station_names = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    crate::error::ProcessingError::Config(
                        "Invalid station_name column type".to_string(),
                    )
                })?;
            let dates = batch
                .column(2)
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    crate::error::ProcessingError::Config("Invalid date column type".to_string())
                })?;
            let latitudes = batch
                .column(3)
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    crate::error::ProcessingError::Config(
                        "Invalid latitude column type".to_string(),
                    )
                })?;
            let longitudes = batch
                .column(4)
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    crate::error::ProcessingError::Config(
                        "Invalid longitude column type".to_string(),
                    )
                })?;
            let min_temps = batch
                .column(5)
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    crate::error::ProcessingError::Config(
                        "Invalid min_temp column type".to_string(),
                    )
                })?;
            let max_temps = batch
                .column(6)
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    crate::error::ProcessingError::Config(
                        "Invalid max_temp column type".to_string(),
                    )
                })?;
            let avg_temps = batch
                .column(7)
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    crate::error::ProcessingError::Config(
                        "Invalid avg_temp column type".to_string(),
                    )
                })?;
            let quality_flags = batch
                .column(8)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    crate::error::ProcessingError::Config(
                        "Invalid quality_flags column type".to_string(),
                    )
                })?;

            // Convert to ConsolidatedRecord objects
            let batch_records_to_read = (batch.num_rows()).min(limit - total_read);

            for i in 0..batch_records_to_read {
                let date = chrono::NaiveDate::from_num_days_from_ce_opt(dates.value(i))
                    .ok_or_else(|| {
                        crate::error::ProcessingError::Config(
                            "Invalid date in Parquet file".to_string(),
                        )
                    })?;

                let record = ConsolidatedRecord::new(
                    station_ids.value(i),
                    station_names.value(i).to_string(),
                    date,
                    latitudes.value(i),
                    longitudes.value(i),
                    min_temps.value(i),
                    max_temps.value(i),
                    avg_temps.value(i),
                    quality_flags.value(i).to_string(),
                );

                records.push(record);
                total_read += 1;

                if total_read >= limit {
                    break;
                }
            }

            if total_read >= limit {
                break;
            }
        }

        Ok(records)
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

    /// Write weather records to Parquet file with optional fields
    pub fn write_weather_records(&self, records: &[WeatherRecord], path: &Path) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        let schema = self.create_weather_schema();
        let batch = self.weather_records_to_batch(records, schema.clone())?;

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

    /// Write weather records in batches for memory efficiency
    pub fn write_weather_records_batched(
        &self,
        records: &[WeatherRecord],
        path: &Path,
        batch_size: usize,
    ) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        let schema = self.create_weather_schema();
        let file = File::create(path)?;
        let props = WriterProperties::builder()
            .set_compression(self.compression)
            .set_max_row_group_size(self.row_group_size)
            .build();

        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

        // Write in batches
        for chunk in records.chunks(batch_size) {
            let batch = self.weather_records_to_batch(chunk, schema.clone())?;
            writer.write(&batch)?;
        }

        writer.close()?;
        Ok(())
    }

    /// Create Arrow schema for multi-metric weather data
    fn create_weather_schema(&self) -> Arc<Schema> {
        let fields = vec![
            Field::new("station_id", DataType::UInt32, false),
            Field::new("station_name", DataType::Utf8, false),
            Field::new("date", DataType::Date32, false),
            Field::new("latitude", DataType::Float64, false),
            Field::new("longitude", DataType::Float64, false),
            // Optional temperature fields
            Field::new("temp_min", DataType::Float32, true),
            Field::new("temp_max", DataType::Float32, true),
            Field::new("temp_avg", DataType::Float32, true),
            // Optional precipitation field
            Field::new("precipitation", DataType::Float32, true),
            // Optional wind speed field
            Field::new("wind_speed", DataType::Float32, true),
            // Quality flag fields (original ECAD)
            Field::new("temp_quality", DataType::Utf8, true),
            Field::new("precip_quality", DataType::Utf8, true),
            Field::new("wind_quality", DataType::Utf8, true),
            // Physical validation fields
            Field::new("temp_validation", DataType::Utf8, true),
            Field::new("precip_validation", DataType::Utf8, true),
            Field::new("wind_validation", DataType::Utf8, true),
        ];

        Arc::new(Schema::new(fields))
    }

    /// Convert weather records to Arrow RecordBatch
    fn weather_records_to_batch(
        &self,
        records: &[WeatherRecord],
        schema: Arc<Schema>,
    ) -> Result<RecordBatch> {
        // Extract data into separate vectors
        let station_ids: Vec<u32> = records.iter().map(|r| r.station_id).collect();
        let station_names: Vec<String> = records.iter().map(|r| r.station_name.clone()).collect();
        let dates: Vec<i32> = records.iter().map(|r| r.date.num_days_from_ce()).collect();
        let latitudes: Vec<f64> = records.iter().map(|r| r.latitude).collect();
        let longitudes: Vec<f64> = records.iter().map(|r| r.longitude).collect();

        // Temperature data (optional)
        let temp_mins: Vec<Option<f32>> = records.iter().map(|r| r.temp_min).collect();
        let temp_maxs: Vec<Option<f32>> = records.iter().map(|r| r.temp_max).collect();
        let temp_avgs: Vec<Option<f32>> = records.iter().map(|r| r.temp_avg).collect();

        // Other weather metrics (optional)
        let precipitations: Vec<Option<f32>> = records.iter().map(|r| r.precipitation).collect();
        let wind_speeds: Vec<Option<f32>> = records.iter().map(|r| r.wind_speed).collect();

        // Quality flags (optional)
        let temp_qualities: Vec<Option<String>> =
            records.iter().map(|r| r.temp_quality.clone()).collect();
        let precip_qualities: Vec<Option<String>> =
            records.iter().map(|r| r.precip_quality.clone()).collect();
        let wind_qualities: Vec<Option<String>> =
            records.iter().map(|r| r.wind_quality.clone()).collect();

        // Physical validation flags (optional)
        let temp_validations: Vec<Option<String>> = records
            .iter()
            .map(|r| r.temp_validation.map(|v| format!("{:?}", v)))
            .collect();
        let precip_validations: Vec<Option<String>> = records
            .iter()
            .map(|r| r.precip_validation.map(|v| format!("{:?}", v)))
            .collect();
        let wind_validations: Vec<Option<String>> = records
            .iter()
            .map(|r| r.wind_validation.map(|v| format!("{:?}", v)))
            .collect();

        // Create Arrow arrays
        let station_id_array = Arc::new(UInt32Array::from(station_ids));
        let station_name_array = Arc::new(StringArray::from(station_names));
        let date_array = Arc::new(Date32Array::from(dates));
        let latitude_array = Arc::new(Float64Array::from(latitudes));
        let longitude_array = Arc::new(Float64Array::from(longitudes));

        // Create optional arrays
        let temp_min_array = Arc::new(Float32Array::from(temp_mins));
        let temp_max_array = Arc::new(Float32Array::from(temp_maxs));
        let temp_avg_array = Arc::new(Float32Array::from(temp_avgs));
        let precipitation_array = Arc::new(Float32Array::from(precipitations));
        let wind_speed_array = Arc::new(Float32Array::from(wind_speeds));

        let temp_quality_array = Arc::new(StringArray::from(temp_qualities));
        let precip_quality_array = Arc::new(StringArray::from(precip_qualities));
        let wind_quality_array = Arc::new(StringArray::from(wind_qualities));

        let temp_validation_array = Arc::new(StringArray::from(temp_validations));
        let precip_validation_array = Arc::new(StringArray::from(precip_validations));
        let wind_validation_array = Arc::new(StringArray::from(wind_validations));

        // Create record batch
        let batch = RecordBatch::try_new(
            schema,
            vec![
                station_id_array,
                station_name_array,
                date_array,
                latitude_array,
                longitude_array,
                temp_min_array,
                temp_max_array,
                temp_avg_array,
                precipitation_array,
                wind_speed_array,
                temp_quality_array,
                precip_quality_array,
                wind_quality_array,
                temp_validation_array,
                precip_validation_array,
                wind_validation_array,
            ],
        )?;

        Ok(batch)
    }

    /// Read sample weather records from Parquet file
    pub fn read_sample_weather_records(
        &self,
        path: &Path,
        limit: usize,
    ) -> Result<Vec<WeatherRecord>> {
        use arrow::array::*;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let file = File::open(path)?;
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(file)?
            .with_batch_size(limit.min(8192))
            .build()?;

        let mut records = Vec::new();
        let mut total_read = 0;

        for batch_result in parquet_reader {
            let batch = batch_result?;

            // Extract arrays from the batch - handle both old and new schema
            let num_columns = batch.num_columns();

            if num_columns < 13 {
                // Old schema format - return empty for now
                return Ok(Vec::new());
            }

            let has_validation_fields = num_columns >= 16;

            // New WeatherRecord schema
            let station_ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| {
                    crate::error::ProcessingError::Config(
                        "Invalid station_id column type".to_string(),
                    )
                })?;
            let station_names = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    crate::error::ProcessingError::Config(
                        "Invalid station_name column type".to_string(),
                    )
                })?;
            let dates = batch
                .column(2)
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    crate::error::ProcessingError::Config("Invalid date column type".to_string())
                })?;
            let latitudes = batch
                .column(3)
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    crate::error::ProcessingError::Config(
                        "Invalid latitude column type".to_string(),
                    )
                })?;
            let longitudes = batch
                .column(4)
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    crate::error::ProcessingError::Config(
                        "Invalid longitude column type".to_string(),
                    )
                })?;

            // Optional temperature fields
            let temp_mins = batch
                .column(5)
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    crate::error::ProcessingError::Config(
                        "Invalid temp_min column type".to_string(),
                    )
                })?;
            let temp_maxs = batch
                .column(6)
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    crate::error::ProcessingError::Config(
                        "Invalid temp_max column type".to_string(),
                    )
                })?;
            let temp_avgs = batch
                .column(7)
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    crate::error::ProcessingError::Config(
                        "Invalid temp_avg column type".to_string(),
                    )
                })?;

            // Optional other weather metrics
            let precipitations = batch
                .column(8)
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    crate::error::ProcessingError::Config(
                        "Invalid precipitation column type".to_string(),
                    )
                })?;
            let wind_speeds = batch
                .column(9)
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    crate::error::ProcessingError::Config(
                        "Invalid wind_speed column type".to_string(),
                    )
                })?;

            // Optional quality flags
            let temp_qualities = batch
                .column(10)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    crate::error::ProcessingError::Config(
                        "Invalid temp_quality column type".to_string(),
                    )
                })?;
            let precip_qualities = batch
                .column(11)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    crate::error::ProcessingError::Config(
                        "Invalid precip_quality column type".to_string(),
                    )
                })?;
            let wind_qualities = batch
                .column(12)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    crate::error::ProcessingError::Config(
                        "Invalid wind_quality column type".to_string(),
                    )
                })?;

            // Optional validation fields (new schema)
            let temp_validations = if has_validation_fields {
                Some(
                    batch
                        .column(13)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            crate::error::ProcessingError::Config(
                                "Invalid temp_validation column type".to_string(),
                            )
                        })?,
                )
            } else {
                None
            };
            let precip_validations = if has_validation_fields {
                Some(
                    batch
                        .column(14)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            crate::error::ProcessingError::Config(
                                "Invalid precip_validation column type".to_string(),
                            )
                        })?,
                )
            } else {
                None
            };
            let wind_validations = if has_validation_fields {
                Some(
                    batch
                        .column(15)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            crate::error::ProcessingError::Config(
                                "Invalid wind_validation column type".to_string(),
                            )
                        })?,
                )
            } else {
                None
            };

            // Convert to WeatherRecord objects
            let batch_records_to_read = (batch.num_rows()).min(limit - total_read);

            for i in 0..batch_records_to_read {
                let date = chrono::NaiveDate::from_num_days_from_ce_opt(dates.value(i))
                    .ok_or_else(|| {
                        crate::error::ProcessingError::Config(
                            "Invalid date in Parquet file".to_string(),
                        )
                    })?;

                use crate::models::weather::PhysicalValidity;

                let record = WeatherRecord::new_raw(
                    station_ids.value(i),
                    station_names.value(i).to_string(),
                    date,
                    latitudes.value(i),
                    longitudes.value(i),
                    if temp_mins.is_null(i) {
                        None
                    } else {
                        Some(temp_mins.value(i))
                    },
                    if temp_maxs.is_null(i) {
                        None
                    } else {
                        Some(temp_maxs.value(i))
                    },
                    if temp_avgs.is_null(i) {
                        None
                    } else {
                        Some(temp_avgs.value(i))
                    },
                    if precipitations.is_null(i) {
                        None
                    } else {
                        Some(precipitations.value(i))
                    },
                    if wind_speeds.is_null(i) {
                        None
                    } else {
                        Some(wind_speeds.value(i))
                    },
                    if temp_qualities.is_null(i) {
                        None
                    } else {
                        Some(temp_qualities.value(i).to_string())
                    },
                    if precip_qualities.is_null(i) {
                        None
                    } else {
                        Some(precip_qualities.value(i).to_string())
                    },
                    if wind_qualities.is_null(i) {
                        None
                    } else {
                        Some(wind_qualities.value(i).to_string())
                    },
                    // Parse validation fields if available
                    temp_validations.and_then(|arr| {
                        if arr.is_null(i) {
                            None
                        } else {
                            PhysicalValidity::parse(arr.value(i))
                        }
                    }),
                    precip_validations.and_then(|arr| {
                        if arr.is_null(i) {
                            None
                        } else {
                            PhysicalValidity::parse(arr.value(i))
                        }
                    }),
                    wind_validations.and_then(|arr| {
                        if arr.is_null(i) {
                            None
                        } else {
                            PhysicalValidity::parse(arr.value(i))
                        }
                    }),
                );

                records.push(record);
                total_read += 1;

                if total_read >= limit {
                    break;
                }
            }

            if total_read >= limit {
                break;
            }
        }

        Ok(records)
    }

    /// Detect the schema type of a Parquet file
    pub fn detect_schema_type(&self, path: &Path) -> Result<SchemaType> {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let schema = builder.schema();

        // Check number of columns to determine schema type
        let num_columns = schema.fields().len();

        if num_columns == 9 {
            // Old ConsolidatedRecord schema: station_id, station_name, date, lat, lon, min_temp, max_temp, avg_temp, quality_flags
            Ok(SchemaType::ConsolidatedRecord)
        } else if num_columns == 13 || num_columns == 16 {
            // WeatherRecord schema: 13 cols = original, 16 cols = with validation fields
            Ok(SchemaType::WeatherRecord)
        } else {
            Ok(SchemaType::Unknown)
        }
    }

    /// Analyze a WeatherRecord Parquet file comprehensively
    pub fn analyze_weather_dataset(
        &self,
        path: &Path,
        sample_size: usize,
    ) -> Result<WeatherDatasetSummary> {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use std::collections::HashSet;

        let file = File::open(path)?;
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;

        let mut total_records = 0;
        let mut stations: HashSet<u32> = HashSet::new();
        let mut all_records = Vec::new();

        // Bounds tracking
        let mut min_lat = f64::MAX;
        let mut max_lat = f64::MIN;
        let mut min_lon = f64::MAX;
        let mut max_lon = f64::MIN;
        let mut min_date = None;
        let mut max_date = None;

        // Metric statistics
        let mut temp_records = 0;
        let mut temp_stations: HashSet<u32> = HashSet::new();
        let mut temp_dates = Vec::new();
        let mut precip_records = 0;
        let mut precip_stations: HashSet<u32> = HashSet::new();
        let mut precip_dates = Vec::new();
        let mut wind_records = 0;
        let mut wind_stations: HashSet<u32> = HashSet::new();
        let mut wind_dates = Vec::new();

        // Extreme tracking
        let mut coldest_record: Option<WeatherRecord> = None;
        let mut hottest_record: Option<WeatherRecord> = None;
        let mut wettest_record: Option<WeatherRecord> = None;
        let mut windiest_record: Option<WeatherRecord> = None;
        let mut min_temp_val = f32::MAX;
        let mut max_temp_val = f32::MIN;
        let mut max_precip_val = f32::MIN;
        let mut max_wind_val = f32::MIN;

        // Enhanced data quality tracking
        let mut ecad_valid = 0;
        let mut ecad_suspect = 0;
        let mut ecad_missing = 0;
        let mut physically_valid = 0;
        let mut physically_suspect = 0;
        let mut physically_invalid = 0;
        let mut combined_valid = 0;
        let mut combined_suspect_original = 0;
        let mut combined_suspect_range = 0;
        let mut combined_suspect_both = 0;
        let mut combined_invalid = 0;
        let mut combined_missing = 0;

        for batch_result in parquet_reader {
            let batch = batch_result?;
            let num_rows = batch.num_rows();

            if batch.num_columns() < 13 {
                continue; // Skip if not WeatherRecord format
            }

            let has_validation_fields = batch.num_columns() >= 16;

            // Extract arrays
            let station_ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap();
            let station_names = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let dates = batch
                .column(2)
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap();
            let latitudes = batch
                .column(3)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let longitudes = batch
                .column(4)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let temp_mins = batch
                .column(5)
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap();
            let temp_maxs = batch
                .column(6)
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap();
            let temp_avgs = batch
                .column(7)
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap();
            let precipitations = batch
                .column(8)
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap();
            let wind_speeds = batch
                .column(9)
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap();
            let temp_qualities = batch
                .column(10)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let precip_qualities = batch
                .column(11)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let wind_qualities = batch
                .column(12)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            // Optional validation fields
            let temp_validations = if has_validation_fields {
                Some(
                    batch
                        .column(13)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap(),
                )
            } else {
                None
            };
            let precip_validations = if has_validation_fields {
                Some(
                    batch
                        .column(14)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap(),
                )
            } else {
                None
            };
            let wind_validations = if has_validation_fields {
                Some(
                    batch
                        .column(15)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap(),
                )
            } else {
                None
            };

            for i in 0..num_rows {
                total_records += 1;
                let station_id = station_ids.value(i);
                stations.insert(station_id);

                let date = chrono::NaiveDate::from_num_days_from_ce_opt(dates.value(i)).unwrap();

                // Update date bounds
                min_date = Some(min_date.map_or(date, |d: chrono::NaiveDate| d.min(date)));
                max_date = Some(max_date.map_or(date, |d: chrono::NaiveDate| d.max(date)));

                // Update geographic bounds
                let lat = latitudes.value(i);
                let lon = longitudes.value(i);
                min_lat = min_lat.min(lat);
                max_lat = max_lat.max(lat);
                min_lon = min_lon.min(lon);
                max_lon = max_lon.max(lon);

                // Create record for sampling and analysis
                use crate::models::weather::{DataQuality, PhysicalValidity};

                let record = WeatherRecord::new_raw(
                    station_id,
                    station_names.value(i).to_string(),
                    date,
                    lat,
                    lon,
                    if temp_mins.is_null(i) {
                        None
                    } else {
                        Some(temp_mins.value(i))
                    },
                    if temp_maxs.is_null(i) {
                        None
                    } else {
                        Some(temp_maxs.value(i))
                    },
                    if temp_avgs.is_null(i) {
                        None
                    } else {
                        Some(temp_avgs.value(i))
                    },
                    if precipitations.is_null(i) {
                        None
                    } else {
                        Some(precipitations.value(i))
                    },
                    if wind_speeds.is_null(i) {
                        None
                    } else {
                        Some(wind_speeds.value(i))
                    },
                    if temp_qualities.is_null(i) {
                        None
                    } else {
                        Some(temp_qualities.value(i).to_string())
                    },
                    if precip_qualities.is_null(i) {
                        None
                    } else {
                        Some(precip_qualities.value(i).to_string())
                    },
                    if wind_qualities.is_null(i) {
                        None
                    } else {
                        Some(wind_qualities.value(i).to_string())
                    },
                    // Parse validation fields if available
                    temp_validations.and_then(|arr| {
                        if arr.is_null(i) {
                            None
                        } else {
                            PhysicalValidity::parse(arr.value(i))
                        }
                    }),
                    precip_validations.and_then(|arr| {
                        if arr.is_null(i) {
                            None
                        } else {
                            PhysicalValidity::parse(arr.value(i))
                        }
                    }),
                    wind_validations.and_then(|arr| {
                        if arr.is_null(i) {
                            None
                        } else {
                            PhysicalValidity::parse(arr.value(i))
                        }
                    }),
                );

                // Track metrics
                if record.has_temperature_data() {
                    temp_records += 1;
                    temp_stations.insert(station_id);
                    temp_dates.push(date);

                    // Track extreme temperatures (exclude invalid values)
                    let temp_quality = record.assess_temperature_quality();
                    if !matches!(temp_quality, DataQuality::Invalid) {
                        if let Some(min_temp) = record.temp_min {
                            if min_temp < min_temp_val {
                                min_temp_val = min_temp;
                                coldest_record = Some(record.clone());
                            }
                        }

                        if let Some(max_temp) = record.temp_max {
                            if max_temp > max_temp_val {
                                max_temp_val = max_temp;
                                hottest_record = Some(record.clone());
                            }
                        }
                    }
                }

                if record.has_precipitation() {
                    precip_records += 1;
                    precip_stations.insert(station_id);
                    precip_dates.push(date);

                    // Track extreme precipitation (exclude invalid values)
                    let precip_quality = record.assess_precipitation_quality();
                    if !matches!(precip_quality, DataQuality::Invalid) {
                        if let Some(precip) = record.precipitation {
                            if precip > max_precip_val {
                                max_precip_val = precip;
                                wettest_record = Some(record.clone());
                            }
                        }
                    }
                }

                if record.has_wind_speed() {
                    wind_records += 1;
                    wind_stations.insert(station_id);
                    wind_dates.push(date);

                    // Track extreme wind speed (exclude invalid values)
                    let wind_quality = record.assess_wind_quality();
                    if !matches!(wind_quality, DataQuality::Invalid) {
                        if let Some(wind) = record.wind_speed {
                            if wind > max_wind_val {
                                max_wind_val = wind;
                                windiest_record = Some(record.clone());
                            }
                        }
                    }
                }

                // Enhanced data quality analysis

                // Track ECAD quality flags for each metric present

                // Temperature ECAD flags
                if record.has_temperature_data() {
                    if let Some(temp_quality) = &record.temp_quality {
                        if temp_quality.contains('0') {
                            ecad_valid += 1;
                        } else if temp_quality.contains('1') {
                            ecad_suspect += 1;
                        } else if temp_quality.contains('9') {
                            ecad_missing += 1;
                        }
                    }
                }

                // Precipitation ECAD flags
                if record.has_precipitation() {
                    if let Some(precip_quality) = &record.precip_quality {
                        if precip_quality == "0" {
                            ecad_valid += 1;
                        } else if precip_quality == "1" {
                            ecad_suspect += 1;
                        } else if precip_quality == "9" {
                            ecad_missing += 1;
                        }
                    }
                }

                // Wind speed ECAD flags
                if record.has_wind_speed() {
                    if let Some(wind_quality) = &record.wind_quality {
                        if wind_quality == "0" {
                            ecad_valid += 1;
                        } else if wind_quality == "1" {
                            ecad_suspect += 1;
                        } else if wind_quality == "9" {
                            ecad_missing += 1;
                        }
                    }
                }

                // Track physical validation for each metric present
                if let Some(validation) = record.temp_validation {
                    match validation {
                        PhysicalValidity::Valid => physically_valid += 1,
                        PhysicalValidity::Suspect => physically_suspect += 1,
                        PhysicalValidity::Invalid => physically_invalid += 1,
                    }
                }
                if let Some(validation) = record.precip_validation {
                    match validation {
                        PhysicalValidity::Valid => physically_valid += 1,
                        PhysicalValidity::Suspect => physically_suspect += 1,
                        PhysicalValidity::Invalid => physically_invalid += 1,
                    }
                }
                if let Some(validation) = record.wind_validation {
                    match validation {
                        PhysicalValidity::Valid => physically_valid += 1,
                        PhysicalValidity::Suspect => physically_suspect += 1,
                        PhysicalValidity::Invalid => physically_invalid += 1,
                    }
                }

                // Track combined quality assessment
                let temp_quality = record.assess_temperature_quality();
                let precip_quality = record.assess_precipitation_quality();
                let wind_quality = record.assess_wind_quality();

                for quality in [temp_quality, precip_quality, wind_quality] {
                    match quality {
                        DataQuality::Valid => combined_valid += 1,
                        DataQuality::SuspectOriginal => combined_suspect_original += 1,
                        DataQuality::SuspectRange => combined_suspect_range += 1,
                        DataQuality::SuspectBoth => combined_suspect_both += 1,
                        DataQuality::Invalid => combined_invalid += 1,
                        DataQuality::Missing => combined_missing += 1,
                    }
                }

                // Store for sampling
                all_records.push(record);
            }
        }

        // Create diverse sampling
        let sample_records = self.create_diverse_sample(&all_records, sample_size);

        // Calculate temporal ranges per metric
        temp_dates.sort();
        precip_dates.sort();
        wind_dates.sort();

        let temperature_range = if !temp_dates.is_empty() {
            Some((temp_dates[0], temp_dates[temp_dates.len() - 1]))
        } else {
            None
        };

        let precipitation_range = if !precip_dates.is_empty() {
            Some((precip_dates[0], precip_dates[precip_dates.len() - 1]))
        } else {
            None
        };

        let wind_range = if !wind_dates.is_empty() {
            Some((wind_dates[0], wind_dates[wind_dates.len() - 1]))
        } else {
            None
        };

        // Countries (simplified - could be enhanced with actual geographic lookup)
        let countries = if min_lon < -5.0 && max_lat > 53.0 {
            vec!["GB".to_string(), "IE".to_string()]
        } else if max_lat > 55.0 {
            vec!["GB".to_string()]
        } else {
            vec!["IE".to_string()]
        };

        Ok(WeatherDatasetSummary {
            total_records,
            total_stations: stations.len(),
            geographic_bounds: GeographicBounds {
                min_lat,
                max_lat,
                min_lon,
                max_lon,
                countries,
            },
            temporal_coverage: TemporalCoverage {
                overall_start: min_date
                    .unwrap_or_else(|| chrono::NaiveDate::from_ymd_opt(1900, 1, 1).unwrap()),
                overall_end: max_date
                    .unwrap_or_else(|| chrono::NaiveDate::from_ymd_opt(2000, 1, 1).unwrap()),
                temperature_range,
                precipitation_range,
                wind_range,
            },
            metric_statistics: MetricStatistics {
                temperature_records: temp_records,
                temperature_stations: temp_stations.len(),
                precipitation_records: precip_records,
                precipitation_stations: precip_stations.len(),
                wind_records,
                wind_stations: wind_stations.len(),
                temperature_range: if min_temp_val != f32::MAX && max_temp_val != f32::MIN {
                    Some((min_temp_val, max_temp_val))
                } else {
                    None
                },
                precipitation_range: if max_precip_val != f32::MIN {
                    Some((0.0, max_precip_val))
                } else {
                    None
                },
                wind_range: if max_wind_val != f32::MIN {
                    Some((0.0, max_wind_val))
                } else {
                    None
                },
            },
            data_quality: EnhancedDataQuality {
                ecad_valid,
                ecad_suspect,
                ecad_missing,
                physically_valid,
                physically_suspect,
                physically_invalid,
                combined_valid,
                combined_suspect_original,
                combined_suspect_range,
                combined_suspect_both,
                combined_invalid,
                combined_missing,
                validation_errors: 0, // Will be populated from integrity report if available
            },
            sample_records,
            extreme_records: ExtremeRecords {
                coldest: coldest_record,
                hottest: hottest_record,
                wettest: wettest_record,
                windiest: windiest_record,
            },
        })
    }

    /// Create diverse sample of records for display
    fn create_diverse_sample(
        &self,
        all_records: &[WeatherRecord],
        sample_size: usize,
    ) -> Vec<WeatherRecord> {
        use std::collections::HashMap;

        if all_records.is_empty() || sample_size == 0 {
            return Vec::new();
        }

        let mut samples = Vec::new();
        let mut station_counts: HashMap<String, usize> = HashMap::new();

        // Strategy: Sample diverse stations and metric combinations
        let total_records = all_records.len();
        let step = if total_records > sample_size * 100 {
            total_records / (sample_size * 50) // Sample more spread out for large datasets
        } else {
            std::cmp::max(1, total_records / sample_size)
        };

        for (i, record) in all_records.iter().enumerate() {
            if i % step == 0 && samples.len() < sample_size {
                // Limit samples per station for diversity
                let station_count = station_counts
                    .entry(record.station_name.clone())
                    .or_insert(0);
                if *station_count < 2 {
                    // Max 2 samples per station
                    samples.push(record.clone());
                    *station_count += 1;
                }
            }
        }

        // If we still need more samples, fill with any remaining records
        if samples.len() < sample_size {
            for record in all_records.iter().step_by(step * 2) {
                if samples.len() >= sample_size {
                    break;
                }
                if !samples
                    .iter()
                    .any(|s| s.station_name == record.station_name && s.date == record.date)
                {
                    samples.push(record.clone());
                }
            }
        }

        samples.truncate(sample_size);
        samples
    }
}

#[derive(Debug, PartialEq)]
pub enum SchemaType {
    ConsolidatedRecord,
    WeatherRecord,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct WeatherDatasetSummary {
    pub total_records: usize,
    pub total_stations: usize,
    pub geographic_bounds: GeographicBounds,
    pub temporal_coverage: TemporalCoverage,
    pub metric_statistics: MetricStatistics,
    pub data_quality: EnhancedDataQuality,
    pub sample_records: Vec<WeatherRecord>,
    pub extreme_records: ExtremeRecords,
}

#[derive(Debug, Clone)]
pub struct GeographicBounds {
    pub min_lat: f64,
    pub max_lat: f64,
    pub min_lon: f64,
    pub max_lon: f64,
    pub countries: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct TemporalCoverage {
    pub overall_start: chrono::NaiveDate,
    pub overall_end: chrono::NaiveDate,
    pub temperature_range: Option<(chrono::NaiveDate, chrono::NaiveDate)>,
    pub precipitation_range: Option<(chrono::NaiveDate, chrono::NaiveDate)>,
    pub wind_range: Option<(chrono::NaiveDate, chrono::NaiveDate)>,
}

#[derive(Debug, Clone)]
pub struct MetricStatistics {
    pub temperature_records: usize,
    pub temperature_stations: usize,
    pub precipitation_records: usize,
    pub precipitation_stations: usize,
    pub wind_records: usize,
    pub wind_stations: usize,
    pub temperature_range: Option<(f32, f32)>,
    pub precipitation_range: Option<(f32, f32)>,
    pub wind_range: Option<(f32, f32)>,
}

#[derive(Debug, Clone)]
pub struct EnhancedDataQuality {
    // ECAD quality flag assessment
    pub ecad_valid: usize,
    pub ecad_suspect: usize,
    pub ecad_missing: usize,

    // Physical validation assessment
    pub physically_valid: usize,
    pub physically_suspect: usize,
    pub physically_invalid: usize,

    // Combined quality assessment
    pub combined_valid: usize,
    pub combined_suspect_original: usize,
    pub combined_suspect_range: usize,
    pub combined_suspect_both: usize,
    pub combined_invalid: usize,
    pub combined_missing: usize,

    pub validation_errors: usize,
}

#[derive(Debug, Clone)]
pub struct ExtremeRecords {
    pub coldest: Option<WeatherRecord>,
    pub hottest: Option<WeatherRecord>,
    pub wettest: Option<WeatherRecord>,
    pub windiest: Option<WeatherRecord>,
}

impl WeatherDatasetSummary {
    pub fn display_comprehensive_summary(&self) -> String {
        let mut summary = String::new();

        // Header
        summary.push_str("UNIFIED WEATHER DATASET ANALYSIS\n");
        summary.push_str("================================\n\n");

        // Dataset Overview
        summary.push_str(&format!(
            "Dataset Overview:\n\
            - Records: {} unified weather records\n\
            - Stations: {} across {} ({:.1}°N-{:.1}°N, {:.1}°W-{:.1}°E)\n\
            - Timespan: {} to {} ({} years)\n\n",
            self.total_records,
            self.total_stations,
            self.geographic_bounds.countries.join("/"),
            self.geographic_bounds.min_lat,
            self.geographic_bounds.max_lat,
            if self.geographic_bounds.min_lon < 0.0 {
                -self.geographic_bounds.min_lon
            } else {
                self.geographic_bounds.min_lon
            },
            self.geographic_bounds.max_lon,
            self.temporal_coverage.overall_start,
            self.temporal_coverage.overall_end,
            self.temporal_coverage.overall_end.year() - self.temporal_coverage.overall_start.year()
        ));

        // Metric Coverage Table
        summary.push_str("Metric Coverage:\n");
        summary.push_str(
            "┌─────────────────┬──────────┬─────────────┬──────────────┬─────────────┐\n",
        );
        summary.push_str(
            "│ Metric          │ Stations │ Records     │ Coverage     │ Date Range  │\n",
        );
        summary.push_str(
            "├─────────────────┼──────────┼─────────────┼──────────────┼─────────────┤\n",
        );

        // Temperature row
        let temp_coverage = if self.total_records > 0 {
            (self.metric_statistics.temperature_records as f32 / self.total_records as f32) * 100.0
        } else {
            0.0
        };

        let temp_range = if let Some((start, end)) = self.temporal_coverage.temperature_range {
            format!("{}-{}", start.year(), end.year())
        } else {
            "N/A".to_string()
        };

        summary.push_str(&format!(
            "│ Temperature     │ {:8} │ {:11} │ {:10.1}%  │ {:11} │\n",
            self.metric_statistics.temperature_stations,
            self.metric_statistics.temperature_records,
            temp_coverage,
            temp_range
        ));

        // Precipitation row
        let precip_coverage = if self.total_records > 0 {
            (self.metric_statistics.precipitation_records as f32 / self.total_records as f32)
                * 100.0
        } else {
            0.0
        };

        let precip_range = if let Some((start, end)) = self.temporal_coverage.precipitation_range {
            format!("{}-{}", start.year(), end.year())
        } else {
            "N/A".to_string()
        };

        summary.push_str(&format!(
            "│ Precipitation   │ {:8} │ {:11} │ {:10.1}%  │ {:11} │\n",
            self.metric_statistics.precipitation_stations,
            self.metric_statistics.precipitation_records,
            precip_coverage,
            precip_range
        ));

        // Wind row
        let wind_coverage = if self.total_records > 0 {
            (self.metric_statistics.wind_records as f32 / self.total_records as f32) * 100.0
        } else {
            0.0
        };

        let wind_range = if let Some((start, end)) = self.temporal_coverage.wind_range {
            format!("{}-{}", start.year(), end.year())
        } else {
            "N/A".to_string()
        };

        summary.push_str(&format!(
            "│ Wind Speed      │ {:8} │ {:11} │ {:10.1}%  │ {:11} │\n",
            self.metric_statistics.wind_stations,
            self.metric_statistics.wind_records,
            wind_coverage,
            wind_range
        ));

        summary.push_str(
            "└─────────────────┴──────────┴─────────────┴──────────────┴─────────────┘\n\n",
        );

        // Sample Records (diverse)
        if !self.sample_records.is_empty() {
            summary.push_str("Sample Records (diverse stations & metrics):\n");
            for (i, record) in self.sample_records.iter().enumerate() {
                let mut metrics_display = Vec::new();

                // Temperature display
                let temp_parts: Vec<String> = [
                    record.temp_min.map(|t| format!("min={:.1}°C", t)),
                    record.temp_avg.map(|t| format!("avg={:.1}°C", t)),
                    record.temp_max.map(|t| format!("max={:.1}°C", t)),
                ]
                .into_iter()
                .flatten()
                .collect();

                if !temp_parts.is_empty() {
                    metrics_display.push(format!("temp({})", temp_parts.join(", ")));
                }

                if let Some(precip) = record.precipitation {
                    metrics_display.push(format!("precip={:.1}mm", precip));
                }

                if let Some(wind) = record.wind_speed {
                    metrics_display.push(format!("wind={:.1}m/s", wind));
                }

                let metrics_str = if metrics_display.is_empty() {
                    "no data".to_string()
                } else {
                    metrics_display.join(", ")
                };

                summary.push_str(&format!(
                    "{}. {} on {}: {}\n",
                    i + 1,
                    record.station_name,
                    record.date,
                    metrics_str
                ));
            }
            summary.push('\n');
        }

        // Extreme Records
        summary.push_str("Extreme Records:\n");
        if let Some(ref coldest) = self.extreme_records.coldest {
            if let Some(min_temp) = coldest.temp_min {
                summary.push_str(&format!(
                    "- Coldest: {:.1}°C at {} ({})\n",
                    min_temp, coldest.station_name, coldest.date
                ));
            }
        }

        if let Some(ref hottest) = self.extreme_records.hottest {
            if let Some(max_temp) = hottest.temp_max {
                summary.push_str(&format!(
                    "- Hottest: {:.1}°C at {} ({})\n",
                    max_temp, hottest.station_name, hottest.date
                ));
            }
        }

        if let Some(ref wettest) = self.extreme_records.wettest {
            if let Some(precip) = wettest.precipitation {
                summary.push_str(&format!(
                    "- Wettest: {:.1}mm at {} ({})\n",
                    precip, wettest.station_name, wettest.date
                ));
            }
        }

        if let Some(ref windiest) = self.extreme_records.windiest {
            if let Some(wind) = windiest.wind_speed {
                summary.push_str(&format!(
                    "- Windiest: {:.1}m/s at {} ({})\n",
                    wind, windiest.station_name, windiest.date
                ));
            }
        }
        summary.push('\n');

        // Enhanced Data Quality Analysis
        summary.push_str("Data Quality Analysis:\n");

        let total_ecad = self.data_quality.ecad_valid
            + self.data_quality.ecad_suspect
            + self.data_quality.ecad_missing;
        if total_ecad > 0 {
            summary.push_str("├─ ECAD Assessment:\n");
            summary.push_str(&format!(
                "│  ├─ Valid (flag=0): {} ({:.1}%)\n",
                self.data_quality.ecad_valid,
                (self.data_quality.ecad_valid as f32 / total_ecad as f32) * 100.0
            ));
            summary.push_str(&format!(
                "│  ├─ Suspect (flag=1): {} ({:.1}%)\n",
                self.data_quality.ecad_suspect,
                (self.data_quality.ecad_suspect as f32 / total_ecad as f32) * 100.0
            ));
            summary.push_str(&format!(
                "│  └─ Missing (flag=9): {} ({:.1}%)\n",
                self.data_quality.ecad_missing,
                (self.data_quality.ecad_missing as f32 / total_ecad as f32) * 100.0
            ));
            summary.push_str("│\n");
        }

        let total_physical = self.data_quality.physically_valid
            + self.data_quality.physically_suspect
            + self.data_quality.physically_invalid;
        if total_physical > 0 {
            summary.push_str("├─ Physical Validation:\n");
            summary.push_str(&format!(
                "│  ├─ Valid: {} ({:.1}%)\n",
                self.data_quality.physically_valid,
                (self.data_quality.physically_valid as f32 / total_physical as f32) * 100.0
            ));
            summary.push_str(&format!(
                "│  ├─ Suspect: {} ({:.1}%)\n",
                self.data_quality.physically_suspect,
                (self.data_quality.physically_suspect as f32 / total_physical as f32) * 100.0
            ));
            summary.push_str(&format!(
                "│  └─ Invalid: {} ({:.3}%)\n",
                self.data_quality.physically_invalid,
                (self.data_quality.physically_invalid as f32 / total_physical as f32) * 100.0
            ));
            summary.push_str("│\n");
        }

        let total_combined = self.data_quality.combined_valid
            + self.data_quality.combined_suspect_original
            + self.data_quality.combined_suspect_range
            + self.data_quality.combined_suspect_both
            + self.data_quality.combined_invalid
            + self.data_quality.combined_missing;
        if total_combined > 0 {
            summary.push_str("└─ Combined Quality:\n");
            summary.push_str(&format!(
                "   ├─ Valid: {} ({:.1}%)\n",
                self.data_quality.combined_valid,
                (self.data_quality.combined_valid as f32 / total_combined as f32) * 100.0
            ));
            summary.push_str(&format!(
                "   ├─ Suspect (original): {} ({:.1}%)\n",
                self.data_quality.combined_suspect_original,
                (self.data_quality.combined_suspect_original as f32 / total_combined as f32)
                    * 100.0
            ));
            summary.push_str(&format!(
                "   ├─ Suspect (range): {} ({:.2}%)\n",
                self.data_quality.combined_suspect_range,
                (self.data_quality.combined_suspect_range as f32 / total_combined as f32) * 100.0
            ));
            if self.data_quality.combined_invalid > 0 {
                summary.push_str(&format!(
                    "   ├─ Invalid: {} ({:.3}%)\n",
                    self.data_quality.combined_invalid,
                    (self.data_quality.combined_invalid as f32 / total_combined as f32) * 100.0
                ));
            }
            summary.push_str(&format!(
                "   └─ Missing: {} ({:.1}%)\n",
                self.data_quality.combined_missing,
                (self.data_quality.combined_missing as f32 / total_combined as f32) * 100.0
            ));
        }

        if self.data_quality.physically_invalid > 0 {
            summary.push_str(&format!(
                "\n⚠️  Found {} physically impossible values that were excluded from extreme records analysis\n",
                self.data_quality.physically_invalid
            ));
        }

        summary
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
