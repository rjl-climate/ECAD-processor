# European Climate Assessment & Dataset Processor V2

A high-performance Rust application for processing multi-metric weather data from the European Climate Assessment & Dataset (ECA&D) format into efficient Parquet files for analysis.

## Features

- **Multi-Metric Processing**: Temperature, precipitation, and wind speed data in unified format
- **Archive-Based Processing**: Direct ZIP file processing with automatic format detection
- **Advanced Validation**: Two-layer quality system with ECAD flags and physical validation
- **Sparse Data Support**: Efficient handling of records with different metric combinations
- **Memory Efficient**: Streaming processing with configurable batch sizes
- **Production Ready**: Single binary distribution with no runtime dependencies

## Installation

### Prerequisites

- Rust 1.70 or later
- Git

### Build from Source

```bash
git clone <repository-url>
cd ECAD-processor
cargo build --release
```

The compiled binary will be available at `target/release/ecad-processor`.

### Dependencies

The project uses several high-performance Rust crates:
- **Arrow/Parquet**: Columnar data processing
- **Tokio**: Async runtime for concurrent I/O
- **Rayon**: Data parallelism
- **Clap**: Command-line interface
- **Chrono**: Date/time handling

## Usage

### Basic Commands

```bash
# Process single ZIP archive into multi-metric Parquet format (uses date-based filename)
ecad-processor process --input-archive data/weather.zip

# Process all ZIP files in directory into unified dataset (uses date-based filename)
ecad-processor process-directory --input-dir data/

# Validate archive integrity without generating output
ecad-processor validate --input-archive data/weather.zip

# Analyze existing Parquet file (auto-detects v1/v2 schema)
ecad-processor info --file output/weather.parquet
```

### Command Options

#### Process Command (Single Archive)
```bash
ecad-processor process [OPTIONS]

Options:
  -i, --input-archive <FILE>     Input ZIP archive containing weather data
  -o, --output-file <FILE>       Output Parquet file path [default: ecad-weather-{YYMMDD}.parquet]
  -c, --compression <TYPE>       Compression type: snappy, gzip, lz4, zstd, none [default: snappy]
  -s, --station-id <ID>          Process only specific station ID
      --validate-only            Run validation without generating output
      --max-workers <NUM>        Maximum number of worker threads [default: CPU count]
      --chunk-size <SIZE>        Processing batch size [default: 1000]
  -v, --verbose                  Enable verbose logging
```

#### Process Directory Command (Multi-Archive)
```bash
ecad-processor process-directory [OPTIONS]

Options:
  -i, --input-dir <DIR>          Directory containing ZIP archives
  -o, --output-file <FILE>       Output unified Parquet file path [default: ecad-weather-unified-{YYMMDD}.parquet]
      --file-pattern <PATTERN>   Filter archives by filename pattern
  -s, --station-id <ID>          Process only specific station ID
      --validate-only            Run validation without generating output
      --max-workers <NUM>        Maximum number of worker threads [default: CPU count]
      --chunk-size <SIZE>        Processing batch size [default: 1000]
  -v, --verbose                  Enable verbose logging
```

#### Validate Command
```bash
ecad-processor validate [OPTIONS]

Options:
  -i, --input-archive <FILE>    Input ZIP archive to validate
      --max-workers <NUM>       Maximum worker threads [default: CPU count]
  -v, --verbose                Enable verbose logging
```

#### Info Command
```bash
ecad-processor info [OPTIONS]

Options:
  -f, --file <FILE>                  Parquet file to analyze
  -s, --sample <NUM>                 Number of sample records to display [default: 10]
      --analysis-limit <LIMIT>       Maximum records to analyze (0 = all records) [default: 0]
  -v, --verbose                      Enable verbose logging
```

### Example Usage

```bash
# Process single weather archive with GZIP compression (uses default date-based filename)
ecad-processor process -i UK_TEMPERATURE.zip -c gzip

# Process single weather archive with custom filename
ecad-processor process -i UK_TEMPERATURE.zip -o weather.parquet -c gzip

# Process all archives in directory into unified dataset (uses default date-based filename)
ecad-processor process-directory -i data/

# Process only temperature archives using pattern filter with custom filename
ecad-processor process-directory -i data/ --file-pattern TEMP -o temp_only.parquet

# Process single station across all metrics with high parallelism and custom filename
ecad-processor process-directory -i data/ -s 257 --max-workers 16 -o station_257.parquet

# Validate archive integrity with detailed reporting
ecad-processor validate -i UK_TEMPERATURE.zip --verbose

# Analyze generated Parquet file (detects v1/v2 schema automatically)
ecad-processor info -f weather.parquet
```

## Data Format Requirements (V2)

### Archive-Based Processing

V2 processes weather data directly from ECA&D ZIP archives without requiring extraction:

```
data/
├── UK_TEMPERATURE_MIN.zip        # Temperature minimum archive
├── UK_TEMPERATURE_MAX.zip        # Temperature maximum archive
├── UK_TEMPERATURE_AVG.zip        # Temperature average archive
├── UK_PRECIPITATION.zip          # Precipitation archive
├── UK_WIND_SPEED.zip            # Wind speed archive
└── ...                          # Additional weather metrics
```

### Archive Contents Structure
Each ZIP archive contains the standard ECA&D format:
```
UK_TEMPERATURE_MIN.zip
├── stations.txt                 # Station metadata
├── sources.txt                  # Data source information
├── elements.txt                 # Element definitions
├── TN_STAID000257.txt          # Station temperature files
├── TN_STAID000500.txt
└── ...
```

### Multi-Archive Processing

The processor can combine multiple archives into unified weather records:
- **Single Archive**: Process one metric type (e.g., temperature only)
- **Multi-Archive**: Combine temperature, precipitation, wind speed into unified records
- **Filtered Processing**: Use filename patterns to select specific archives

### To obtain the European Climate Assessment datasets,

1. Go to [Custom query in ASCII](https://www.ecad.eu/dailydata/customquery.php)
2. Select country (e.g., United Kingdom)
3. Skip `Location` for all stations or select specific regions
4. Select `Element` (Temperature, Precipitation, Wind Speed, etc.)
5. Click `Next` and then `Download`
6. Repeat for additional weather metrics as needed
7. Keep ZIP files for direct processing - no extraction required

### File Formats

#### Station Files (`stations.txt`)
ECA&D format with metadata header followed by CSV data:
```
STAID,STANAME                                 ,CN,      LAT,       LON,HGHT
  257,CET CENTRAL ENGLAND                     ,GB,+52:25:12,-001:49:48,  78
```

#### Temperature Files (`T*_STAID*.txt`)
ECA&D format with temperature readings:
```
 SOUID,    DATE,   TN, Q_TN
100805,18810101,   14,    0
```

## Output Schema (V2)

The V2 processor generates multi-metric weather records supporting sparse data patterns. For complete schema documentation, see [output/SCHEMA.md](output/SCHEMA.md).

### Key Features
- **16-column format** supporting temperature, precipitation, and wind speed
- **Sparse data model** - records can contain any combination of metrics
- **Two-layer quality system** - ECAD flags + physical validation
- **Backward compatibility** with V1 temperature-only files

### Core Fields
| Column | Type | Description |
|--------|------|-------------|
| `station_id` | UInt32 | Unique station identifier |
| `station_name` | String | Human-readable station name |
| `date` | Date32 | Measurement date (YYYY-MM-DD) |
| `latitude` | Float64 | Station latitude in decimal degrees |
| `longitude` | Float64 | Station longitude in decimal degrees |

### Weather Metrics (Nullable)
| Column | Type | Description |
|--------|------|-------------|
| `temp_min` | Float32 | Daily minimum temperature (°C) |
| `temp_max` | Float32 | Daily maximum temperature (°C) |
| `temp_avg` | Float32 | Daily average temperature (°C) |
| `precipitation` | Float32 | Daily precipitation (mm) |
| `wind_speed` | Float32 | Daily wind speed (m/s) |

### Quality Flags & Validation
| Column | Type | Description |
|--------|------|-------------|
| `temp_quality` | String | ECAD temperature quality flags (3 digits) |
| `precip_quality` | String | ECAD precipitation quality flag (1 digit) |
| `wind_quality` | String | ECAD wind speed quality flag (1 digit) |
| `temp_validation` | String | Physical validation: Valid/Suspect/Invalid |
| `precip_validation` | String | Physical validation: Valid/Suspect/Invalid |
| `wind_validation` | String | Physical validation: Valid/Suspect/Invalid |

### Example Multi-Metric Record
```json
{
  "station_id": 257,
  "station_name": "CET CENTRAL ENGLAND",
  "date": "2023-07-15",
  "latitude": 52.42,
  "longitude": -1.83,
  "temp_min": 12.5,
  "temp_max": 25.3,
  "temp_avg": 18.9,
  "precipitation": 2.1,
  "wind_speed": 4.7,
  "temp_quality": "000",
  "precip_quality": "0",
  "wind_quality": "0",
  "temp_validation": "Valid",
  "precip_validation": "Valid",
  "wind_validation": "Valid"
}
```

### Sparse Data Handling
- **NULL values** indicate missing measurements (not measurement errors)
- Records may contain any combination of weather metrics
- Quality flags and validation only present for available metrics
- Efficient columnar storage optimizes sparse data patterns

## Data Validation (V2)

The V2 processor implements a comprehensive two-layer quality system for multi-metric weather data:

### Two-Layer Quality Architecture

#### 1. ECAD Quality Flags (Original)
- **Source**: European Climate Assessment & Dataset quality indicators
- **Values**: `0` (Valid), `1` (Suspect), `9` (Missing)
- **Coverage**: All weather metrics with original quality assessments

#### 2. Physical Validation (Enhanced)
- **Source**: ECAD processor physical plausibility checks
- **Values**: `Valid`, `Suspect`, `Invalid`
- **Thresholds**: UK/Ireland-specific physical limits

### Physical Validation Thresholds

#### Temperature (°C)
- **Valid**: -35.0 to 45.0 (normal UK/Ireland range)
- **Suspect**: -90.0 to 60.0 (extreme but possible)
- **Invalid**: Below -90.0 or above 60.0 (physically impossible)

#### Precipitation (mm/day)
- **Valid**: 0.0 to 500.0 (normal range)
- **Suspect**: 500.0 to 2000.0 (extreme rainfall events)
- **Invalid**: Above 2000.0 or negative (impossible)

#### Wind Speed (m/s)
- **Valid**: 0.0 to 50.0 (normal to strong winds)
- **Suspect**: 50.0 to 120.0 (hurricane-force winds)
- **Invalid**: Above 120.0 or negative (impossible)

### Multi-Metric Validation Features
- **Sparse Data Support**: Validation applied only to available metrics
- **Cross-Metric Consistency**: Temperature relationship validation (min ≤ avg ≤ max)
- **Geographic Bounds**: Station coordinates within UK/Ireland boundaries
- **Quality Flag Consistency**: Combined assessment of ECAD and physical validation

### Validation Reporting

The V2 system generates comprehensive integrity reports for multi-metric datasets:

```
=== Multi-Metric Integrity Report ===
Dataset Composition:
  Metrics in Parquet: ["temperature", "precipitation", "wind_speed"]
  Total records: 2,498,741

Metric Coverage:
  Temperature: 2,299,078/2,498,741 (92.0%)
  Precipitation: 1,847,395/2,498,741 (74.0%)
  Wind Speed: 956,428/2,498,741 (38.3%)

ECAD Assessment:
  Valid Records: 2,453,867 (98.2%)
  Suspect Records: 44,874 (1.8%)
  Invalid Records: 0 (0.0%)

Physical Validation:
  Valid: 2,486,523 (99.5%)
  Suspect: 12,218 (0.5%)
  Invalid: 0 (0.0%)

Data Quality Summary:
  • Combined quality assessment across all metrics
  • Invalid records excluded from extreme value analysis
  • Sparse data patterns efficiently handled
```

## Quality Codes (V2)

Multi-metric weather data includes quality flags for each available measurement type:

### ECAD Quality Flag Values
| Code | Status | Description | Action |
|------|--------|-------------|---------|
| `0` | **Valid** | High-quality measurement | ✅ Use for analysis |
| `1` | **Suspect** | Questionable measurement | ⚠️ Use with caution |
| `9` | **Missing** | No measurement available | ❌ Exclude from analysis |

### Physical Validation Values
| Status | Description | Action |
|--------|-------------|---------|
| `Valid` | Within normal physical limits | ✅ Use for analysis |
| `Suspect` | Extreme but physically possible | ⚠️ Flag for review |
| `Invalid` | Physically impossible | ❌ Exclude from analysis |

### Quality Flag Formats

#### Temperature Quality (`temp_quality`)
3-character string for min/avg/max temperatures:
- **Position 1**: Minimum temperature quality
- **Position 2**: Average temperature quality
- **Position 3**: Maximum temperature quality
- **Examples**: `"000"` (all valid), `"190"` (min suspect, avg missing, max valid)

#### Precipitation/Wind Quality (`precip_quality`, `wind_quality`)
1-character string for single metric:
- **Examples**: `"0"` (valid), `"1"` (suspect), `"9"` (missing)

### Data Quality Best Practices (V2)

#### For Multi-Metric Analysis
1. **High Confidence**: Use only physically valid data with ECAD quality `0`
2. **Standard Analysis**: Include valid and suspect data, exclude invalid
3. **Research Applications**: Consider all data but weight by combined quality assessment

#### Quality Filtering Examples
```sql
-- High quality temperature data only
SELECT * FROM weather_data
WHERE temp_validation = 'Valid' AND temp_quality = '000';

-- Include all physically valid data across metrics
SELECT * FROM weather_data
WHERE (temp_validation != 'Invalid' OR temp_validation IS NULL)
  AND (precip_validation != 'Invalid' OR precip_validation IS NULL)
  AND (wind_validation != 'Invalid' OR wind_validation IS NULL);

-- Valid precipitation measurements only
SELECT * FROM weather_data
WHERE precipitation IS NOT NULL
  AND precip_validation = 'Valid'
  AND precip_quality = '0';

-- Multi-metric records with all data valid
SELECT * FROM weather_data
WHERE temp_min IS NOT NULL AND precipitation IS NOT NULL AND wind_speed IS NOT NULL
  AND temp_validation = 'Valid' AND precip_validation = 'Valid' AND wind_validation = 'Valid';
```

## Performance Characteristics (V2)

### Multi-Archive Processing Speed
- **Single Archive**: ~1M records processed per second
- **Multi-Archive**: Concurrent processing of multiple ZIP files
- **Memory Usage**: <200MB for large multi-metric datasets
- **Concurrency**: Automatic scaling to available CPU cores
- **I/O Efficiency**: Direct ZIP processing without extraction

### Output Optimization
- **Compression**: 5-10x compression ratio with Snappy (default)
- **Sparse Data**: Efficient NULL value handling in columnar format
- **Query Performance**: Optimized for analytical workloads
- **Row Groups**: Configurable sizing for memory-efficient streaming
- **Schema Detection**: Automatic v1/v2 format recognition

### Scalability Features
- **Memory Efficient**: Streaming processing with configurable chunk sizes
- **CPU Scaling**: Linear performance improvement with additional cores
- **Storage Optimization**: Columnar format with metric-specific compression
- **Archive Concurrency**: Parallel processing of multiple weather data sources

## Error Handling (V2)

The V2 processor provides comprehensive error reporting for multi-archive processing:

### Common Issues
1. **Archive Not Found**: Check ZIP file paths and permissions
2. **Invalid Archive Format**: Verify ECA&D ZIP archive structure
3. **Unsupported Archive**: Some weather metrics may not be supported
4. **Memory Issues**: Reduce `--chunk-size` for memory-constrained environments
5. **Permission Errors**: Ensure write access to output directory

### Troubleshooting V2 Features
- Use `--verbose` flag for detailed multi-archive processing logs
- Verify ZIP archives contain valid ECA&D format files
- Check file patterns when using directory processing
- Monitor system resources during concurrent archive processing
- Use schema detection to verify output format compatibility

## Contributing

### Development Setup
```bash
# Install Rust development tools
rustup component add clippy rustfmt

# Run tests
cargo test

# Format code
cargo fmt

# Lint code
cargo clippy

# Run benchmarks
cargo bench
```

### Code Organization (V2)
- `src/models/`: Multi-metric data structures and validation
- `src/archive/`: ZIP archive processing and multi-archive coordination
- `src/readers/`: File parsing and concurrent I/O operations
- `src/processors/`: Data transformation and integrity checking
- `src/writers/`: Multi-schema Parquet file generation with schema detection
- `src/analyzers/`: Weather dataset analysis and statistics
- `src/cli/`: Enhanced command-line interface for archive processing

## License

MIT License

Copyright (c) 2025 Richard Lyon

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

## Acknowledgments

- European Climate Assessment & Dataset (ECA&D) for providing comprehensive weather data archives
- UK Met Office for weather station infrastructure and data collection
- Rust community for excellent crates enabling high-performance data processing
- Arrow and Parquet ecosystems for efficient columnar data storage
