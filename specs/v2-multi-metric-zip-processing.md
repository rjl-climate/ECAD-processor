# ECAD Processor v2: Multi-Metric Zip Processing Specification

**Document Version:** 1.0  
**Date:** July 2025  
**Status:** Implementation Planning  

## Executive Summary

This specification defines the migration from ECAD Processor v1 (folder-based, temperature-only) to v2 (zip-based, multi-metric processing). The v2 implementation will support processing multiple weather metrics (temperature, precipitation, wind speed) directly from zip archives with automatic content-based inference and optimized storage using nullable Parquet columns.

### Key Changes
- **Input Processing:** Zip archives only (remove folder-based processing)
- **Metric Support:** Multi-metric (temperature, precipitation, wind speed)
- **Data Inference:** Content-based country and metric detection
- **Storage Strategy:** Optional fields with Parquet null optimization
- **Temporary Management:** RAII-based cleanup with automatic resource management

## Data Analysis & Requirements

### Station Distribution Analysis

| Metric | Station Count | File Pattern | Units |
|--------|---------------|--------------|-------|
| Temperature Min | 131 | `TN_STAID*.txt` | 0.1°C |
| Temperature Max | 131 | `TX_STAID*.txt` | 0.1°C |
| Temperature Avg | 120 | `TG_STAID*.txt` | 0.1°C |
| Precipitation | 349 | `RR_STAID*.txt` | 0.1mm |
| Wind Speed | 329 | `FG_STAID*.txt` | 0.1 m/s |
| **Total Unique Stations** | **541** | | |

### Data Sparsity Challenges
- **No complete coverage:** No single station has all 5 metrics
- **High sparsity:** ~65% of station-metric combinations are missing
- **Temperature clustering:** Min/max temperature stations largely overlap
- **Precipitation dominance:** Highest station count (349 vs ~130 for temperature)

### Content-Based Inference Requirements
- **Country Detection:** Parse ISO country codes from `stations.txt` (e.g., "GB")
- **Metric Detection:** Identify metrics from file prefixes and `elements.txt`
- **Validation:** Cross-reference file patterns with metadata definitions
- **Error Handling:** Detect corrupted or mixed archives

## Technical Architecture

### Archive Processing Pipeline

```
┌─────────────┐    ┌──────────────┐    ┌─────────────────┐    ┌─────────────┐
│ Input       │───▶│ Archive      │───▶│ Temporary       │───▶│ Multi-      │
│ Zip File    │    │ Inspector    │    │ Extraction      │    │ Metric      │
│             │    │              │    │                 │    │ Processing  │
└─────────────┘    └──────────────┘    └─────────────────┘    └─────────────┘
                           │                       │                    │
                           ▼                       ▼                    ▼
                   ┌──────────────┐    ┌─────────────────┐    ┌─────────────┐
                   │ Archive      │    │ Temp File       │    │ Parquet     │
                   │ Metadata     │    │ Manager         │    │ Output      │
                   │              │    │                 │    │             │
                   └──────────────┘    └─────────────────┘    └─────────────┘
```

### Core Components

#### 1. Archive Inspector
```rust
pub struct ArchiveInspector;

pub struct ArchiveMetadata {
    pub country: String,                    // From stations.txt CN field
    pub metrics: Vec<WeatherMetric>,        // From file prefixes + elements.txt
    pub station_count: usize,
    pub date_range: Option<(NaiveDate, NaiveDate)>,
    pub file_counts: HashMap<WeatherMetric, usize>,
}

impl ArchiveInspector {
    pub fn inspect_zip(zip_path: &Path) -> Result<ArchiveMetadata> {
        // 1. Scan zip file list for metric patterns (TX_*, RR_*, etc.)
        // 2. Extract and parse stations.txt for country codes
        // 3. Extract and validate elements.txt for metric definitions
        // 4. Return comprehensive metadata
    }
}
```

#### 2. Temporary File Manager
```rust
pub struct TempFileManager {
    temp_dir: TempDir,
    extracted_files: HashMap<String, PathBuf>,
}

impl TempFileManager {
    pub fn extract_selective(&mut self, zip_path: &Path, file_patterns: &[&str]) -> Result<Vec<PathBuf>> {
        // Extract only required files to temporary directory
        // Stream large files without full memory load
        // Track all extracted files for cleanup
    }
    
    pub fn cleanup(&mut self) -> Result<()> {
        // Explicit cleanup with error handling
        // Automatic cleanup on Drop as backup
    }
}
```

#### 3. Multi-Metric Data Structure
```rust
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
    
    // Quality flags per metric type
    pub temp_quality: Option<String>,      // "000", "001", etc.
    pub precip_quality: Option<String>,
    pub wind_quality: Option<String>,
}
```

## Data Schema Design

### Parquet Schema Optimization

The nullable column approach provides several advantages:

1. **Storage Efficiency:** Parquet's null encoding compresses efficiently
2. **Query Performance:** Column pruning and predicate pushdown work well
3. **Schema Evolution:** Easy to add new weather metrics
4. **Type Safety:** Rust's `Option<T>` prevents accessing missing data

### Example Parquet Schema
```
message weather_record {
  required int32 station_id;
  required binary station_name (UTF8);
  required int32 date (DATE);
  required double latitude;
  required double longitude;
  optional float temp_min;
  optional float temp_max;
  optional float temp_avg;
  optional float precipitation;
  optional float wind_speed;
  optional binary temp_quality (UTF8);
  optional binary precip_quality (UTF8);
  optional binary wind_quality (UTF8);
}
```

### Data Coverage Analysis
```rust
pub struct DataCoverageReport {
    pub total_records: usize,
    pub total_stations: usize,
    pub coverage_by_metric: HashMap<WeatherMetric, MetricCoverage>,
    pub stations_with_complete_temperature: usize,
    pub stations_with_any_temperature: usize,
    pub stations_with_precipitation: usize,
    pub stations_with_wind: usize,
    pub multi_metric_stations: usize,
}

pub struct MetricCoverage {
    pub station_count: usize,
    pub record_count: usize,
    pub coverage_percentage: f64,
    pub date_range: Option<(NaiveDate, NaiveDate)>,
}
```

## CLI Interface Changes

### Updated Command Structure
```rust
#[derive(Subcommand)]
pub enum Commands {
    /// Process weather data from zip archive
    Process {
        #[arg(short, long, help = "Input zip archive file")]
        input_archive: PathBuf,
        
        #[arg(short, long, default_value = "output/weather_data.parquet")]
        output_file: PathBuf,
        
        #[arg(short, long, default_value = "snappy")]
        compression: String,
        
        #[arg(short, long)]
        station_id: Option<u32>,
        
        #[arg(long, default_value = "false")]
        validate_only: bool,
        
        #[arg(long, default_value_t = num_cpus::get())]
        max_workers: usize,
        
        #[arg(long, default_value = "1000")]
        chunk_size: usize,
    },
    
    /// Validate archive data without processing
    Validate {
        #[arg(short, long, help = "Input zip archive file")]
        input_archive: PathBuf,
        
        #[arg(long, default_value_t = num_cpus::get())]
        max_workers: usize,
    },
    
    /// Display information about processed weather data
    Info {
        #[arg(short, long)]
        file: PathBuf,
        
        #[arg(short, long, default_value = "10")]
        sample: usize,
        
        #[arg(long, default_value = "0")]
        analysis_limit: usize,
    },
}
```

### Breaking Changes
- **Removed:** `input_dir` parameter (folder-based processing)
- **Added:** `input_archive` parameter (zip-file processing)
- **Changed:** Default output filename from `temperatures.parquet` to `weather_data.parquet`
- **Enhanced:** Archive metadata display in info command

## Implementation Plan

### Phase 1: Archive Processing Core (Week 1-2)
**Dependencies:**
```toml
zip = "0.6"
tempfile = "3.8"  # Move from dev-dependencies
```

**New Modules:**
- `src/archive/mod.rs` - Archive processing coordination
- `src/archive/inspector.rs` - Content-based inference
- `src/archive/temp_manager.rs` - Temporary file management
- `src/archive/processor.rs` - Main processing logic

**Tasks:**
1. Implement `ArchiveInspector` for content-based inference
2. Create `TempFileManager` with RAII cleanup
3. Add zip file pattern scanning and validation
4. Implement archive metadata extraction

### Phase 2: Multi-Metric Data Models (Week 2-3)
**Tasks:**
1. Create new `WeatherRecord` struct with optional fields
2. Implement metric-specific validation logic
3. Add data coverage analysis tools
4. Create unified quality flag handling

### Phase 3: Processing Pipeline Updates (Week 3-4)
**Tasks:**
1. Refactor readers to work with temporary files
2. Update data merger for multi-metric records
3. Modify Parquet writer for nullable columns
4. Add parallel processing support for all metrics

### Phase 4: CLI & Integration (Week 4-5)
**Tasks:**
1. Update CLI interface and argument parsing
2. Modify command processing logic
3. Add comprehensive error handling
4. Update progress reporting and user feedback

### Phase 5: Testing & Documentation (Week 5-6)
**Tasks:**
1. Create comprehensive test suite
2. Add performance benchmarks
3. Update documentation and examples
4. Validate against existing data sets

## Migration Strategy

### Removed Features
- **Folder-based processing:** Complete removal of directory input support
- **Temperature-only models:** `ConsolidatedRecord` replaced by `WeatherRecord`
- **Directory constants:** Remove hardcoded folder names from `constants.rs`

### Deprecated Components
- `src/readers/concurrent_reader.rs` - Replace with archive-based reader
- Folder-specific logic in `ParallelProcessor`
- Directory validation in CLI commands

### Migration Guide for Users
1. **Archive Preparation:** Users must use zip files instead of extracted folders
2. **Command Updates:** Change `--input-dir` to `--input-archive` 
3. **Output Changes:** Default output filename changed to `weather_data.parquet`
4. **Schema Changes:** Parquet schema now includes nullable columns for all metrics

## Performance & Storage Considerations

### Storage Optimization
- **Null Compression:** Parquet null encoding provides ~80% compression for missing values
- **Column Pruning:** Query only required metrics without reading unused columns
- **Row Group Sizing:** Optimize for typical query patterns (by station, by date range)

### Memory Management
- **Streaming Extraction:** Process zip files without full decompression
- **Temporary Cleanup:** Automatic cleanup prevents disk space issues
- **Batch Processing:** Configurable chunk sizes for memory control

### Query Performance
```sql
-- Stations with complete temperature data
SELECT station_id, station_name, COUNT(*) as record_count
FROM weather_data 
WHERE temp_min IS NOT NULL 
  AND temp_max IS NOT NULL 
  AND temp_avg IS NOT NULL
GROUP BY station_id, station_name;

-- Multi-metric correlation analysis
SELECT 
  DATE_TRUNC('month', date) as month,
  AVG(temp_avg) as avg_temperature,
  AVG(precipitation) as avg_precipitation
FROM weather_data 
WHERE temp_avg IS NOT NULL 
  AND precipitation IS NOT NULL
GROUP BY month
ORDER BY month;

-- Data coverage by metric
SELECT 
  COUNT(CASE WHEN temp_min IS NOT NULL THEN 1 END) as temp_coverage,
  COUNT(CASE WHEN precipitation IS NOT NULL THEN 1 END) as precip_coverage,
  COUNT(CASE WHEN wind_speed IS NOT NULL THEN 1 END) as wind_coverage,
  COUNT(*) as total_records
FROM weather_data;
```

## Error Handling & Edge Cases

### Archive Validation
- **Corrupted zips:** Early detection with clear error messages
- **Missing metadata:** Graceful handling when stations.txt or elements.txt are missing
- **Mixed countries:** Detect and report multi-country archives
- **Unknown metrics:** Handle archives with unrecognized file patterns

### Data Quality
- **Incomplete records:** Allow processing with partial metric availability
- **Quality flag validation:** Validate quality flags per metric type
- **Date range validation:** Detect and report suspicious date ranges
- **Geographic bounds:** Validate station coordinates against expected ranges

### Resource Management
- **Disk space:** Estimate temporary storage requirements before extraction
- **Memory usage:** Monitor memory consumption during large archive processing
- **Cleanup failures:** Robust cleanup with detailed error reporting
- **Process interruption:** Signal handlers for graceful shutdown

## Success Criteria

### Functional Requirements
- ✅ Process all 5 supported weather metrics from zip archives
- ✅ Automatic country and metric inference from archive content
- ✅ Efficient storage using Parquet nullable columns
- ✅ Complete removal of folder-based processing
- ✅ RAII-based temporary file management

### Performance Requirements
- ✅ No regression in processing speed vs v1 for temperature data
- ✅ Memory usage scales linearly with active metrics
- ✅ Parquet file size optimized for sparse data patterns
- ✅ Query performance suitable for analytical workloads

### Quality Requirements
- ✅ Comprehensive test coverage (>90% for new components)
- ✅ Error handling for all identified edge cases
- ✅ Data integrity validation across all metrics
- ✅ Documentation and examples for all new features

## Future Extensions

### Additional Metrics
- **Pressure:** Support for atmospheric pressure data (PP_* files)
- **Humidity:** Relative humidity processing (HU_* files)  
- **Snow:** Snow depth and coverage metrics (SD_* files)

### Enhanced Features
- **Multi-format output:** Support for CSV, JSON output alongside Parquet
- **Streaming processing:** Real-time processing of large archives
- **Distributed processing:** Horizontal scaling across multiple nodes
- **Time series optimization:** Specialized storage for time series analysis

---

**Document Status:** Ready for Implementation  
**Next Review:** Upon completion of Phase 1 implementation  
**Stakeholders:** Development Team, Data Users, Infrastructure Team