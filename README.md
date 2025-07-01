# European Climate Assessment & Dataset data processor

A high-performance Rust application for processing UK temperature data from the European Climate Assessment & Dataset (ECA&D) format into efficient Parquet files for analysis.

## Features

- **Data Validation**: Comprehensive integrity checking with detailed quality reporting
- **Multiple Formats**: Support for min, max, and average temperature data consolidation
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

The compiled binary will be available at `target/release/ECAD-processor`.

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
# Process temperature data into Parquet format
ECAD-processor process --input-dir data/ --output-file output/temperatures.parquet

# Validate data integrity without generating output
ECAD-processor validate --input-dir data/

# Analyze existing Parquet file
ECAD-processor info --file output/temperatures.parquet
```

### Command Options

#### Process Command
```bash
ECAD-processor process [OPTIONS]

Options:
  -i, --input-dir <DIR>          Input directory containing UK temperature data [default: data]
  -o, --output-file <FILE>       Output Parquet file path [default: output/temperatures.parquet]
  -c, --compression <TYPE>       Compression type: snappy, gzip, lz4, zstd, none [default: snappy]
  -s, --station-id <ID>          Process only specific station ID
      --validate-only            Run validation without generating output
      --max-workers <NUM>        Maximum number of worker threads [default: CPU count]
      --chunk-size <SIZE>        Processing batch size [default: 1000]
  -v, --verbose                  Enable verbose logging
```

#### Validate Command
```bash
ECAD-processor validate [OPTIONS]

Options:
  -i, --input-dir <DIR>     Input directory [default: data]
      --max-workers <NUM>   Maximum worker threads [default: CPU count]
  -v, --verbose            Enable verbose logging
```

#### Info Command
```bash
ECAD-processor info [OPTIONS]

Options:
  -f, --file <FILE>        Parquet file to analyze
  -s, --sample <NUM>       Number of sample records to display [default: 10]
```

### Example Usage

```bash
# Process all UK temperature data with GZIP compression
ECAD-processor process -i data/ -o uk_temps.parquet -c gzip

# Process single station with high parallelism
ECAD-processor process -s 257 --max-workers 16 -o station_257.parquet

# Validate data integrity with detailed reporting
ECAD-processor validate -i data/ --verbose

# Analyze generated Parquet file
ECAD-processor info -f uk_temps.parquet
```

## Data Format Requirements

### Input Directory Structure
```
data/
├── uk_temp_min/
│   ├── stations.txt          # Station metadata
│   ├── TN_STAID000257.txt   # Min temperature files
│   └── ...
├── uk_temp_max/
│   ├── stations.txt          # Station metadata
│   ├── TX_STAID000257.txt   # Max temperature files
│   └── ...
└── uk_temp_avg/
    ├── stations.txt          # Station metadata
    ├── TG_STAID000257.txt   # Avg temperature files
    └── ...
```

### To obtain the European Climate Assessment datasets,

1. Go to [Custom query in ASCII](https://www.ecad.eu/dailydata/customquery.php)
2. Select country
3. Skip `Location` for all data
3. Select `Element` ('Maximum Temperature', etc.)
4. Select `Next`
5. Click `Download`
6. Repeat for Minimum and Average Temperature

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

## Output Schema

The generated Parquet file contains consolidated temperature records with the following schema:

| Column | Type | Description |
|--------|------|-------------|
| `station_id` | UInt32 | Unique station identifier |
| `station_name` | String | Human-readable station name |
| `date` | Date32 | Measurement date (YYYY-MM-DD) |
| `latitude` | Float64 | Station latitude in decimal degrees |
| `longitude` | Float64 | Station longitude in decimal degrees |
| `min_temp` | Float32 | Daily minimum temperature (°C) |
| `max_temp` | Float32 | Daily maximum temperature (°C) |
| `avg_temp` | Float32 | Daily average temperature (°C) |
| `quality_flags` | String | Quality flags for min/avg/max (3 digits) |

### Example Record
```json
{
  "station_id": 257,
  "station_name": "CET CENTRAL ENGLAND",
  "date": "1881-01-01",
  "latitude": 52.42,
  "longitude": -1.83,
  "min_temp": 1.4,
  "max_temp": 6.2,
  "avg_temp": 3.9,
  "quality_flags": "000"
}
```

### Missing Data Handling
- Missing temperatures are stored as `-9999.0`
- Missing data does not prevent record creation
- Quality flags indicate data availability and reliability

## Data Validation

The processor performs comprehensive data validation across multiple levels:

### 1. Individual Record Validation
- **Temperature Range**: Values between -50°C and +50°C
- **Date Validity**: Proper date formatting and reasonable ranges
- **Coordinate Bounds**: UK geographic boundaries (49.5°-61.0°N, -8.0°-2.0°E)
- **Quality Flag Format**: Valid quality codes (0, 1, 9)

### 2. Cross-Station Validation
- **Station Metadata**: Consistent station information across temperature types
- **Geographic Consistency**: Station coordinates within expected UK bounds
- **Data Completeness**: Identification of missing data patterns

### 3. Time Series Validation
- **Temporal Consistency**: Identification of suspicious temperature jumps
- **Seasonal Patterns**: Detection of anomalous seasonal variations
- **Long-term Trends**: Assessment of data continuity

### 4. Data Relationship Validation (Informational)
- **Physical Constraints**: Monitoring of min ≤ avg ≤ max relationships
- **Cross-Source Consistency**: Comparison across different measurement sources
- **Quality Assessment**: Statistical analysis of data reliability

### Validation Reporting

The system generates detailed integrity reports including:

```
=== Integrity Check Report ===
Total Records: 2,498,741
Valid Records: 2,299,078 (92.0%)
Suspect Records: 199,663 (8.0%)
Invalid Records: 0 (0.0%)
Missing Data Records: 0

Temperature Violations: 97,112

Top 10 Violations:
  1. Station 257 on 1881-01-13: Avg temperature -2.9 > Max temperature -4.3
  2. Station 257 on 1881-02-10: Avg temperature 6.1 > Max temperature 4.7
  ...
```

## Quality Codes

Temperature data includes quality flags that indicate measurement reliability:

### Quality Flag Values
| Code | Status | Description | Action |
|------|--------|-------------|---------|
| `0` | **Valid** | High-quality measurement | ✅ Use for analysis |
| `1` | **Suspect** | Questionable measurement | ⚠️ Use with caution |
| `9` | **Missing** | No measurement available | ❌ Exclude from analysis |

### Quality Flag Format
The `quality_flags` field contains a 3-character string representing quality for each temperature type:
- **Position 1**: Minimum temperature quality
- **Position 2**: Average temperature quality
- **Position 3**: Maximum temperature quality

#### Examples
- `"000"` = All temperatures are valid
- `"090"` = Min/max valid, average missing
- `"111"` = All temperatures suspect
- `"901"` = Min missing, avg valid, max suspect

### Data Quality Best Practices

#### For Analysis
1. **High Confidence**: Use only records with `"000"` quality flags
2. **Standard Analysis**: Include valid (`0`) and suspect (`1`) data with appropriate filtering
3. **Research Applications**: Consider all data but weight by quality flags

#### Quality Filtering Examples
```sql
-- High quality data only
SELECT * FROM temperatures WHERE quality_flags = '000';

-- Include valid and suspect data
SELECT * FROM temperatures
WHERE quality_flags NOT LIKE '%9%';

-- Valid minimum temperatures only
SELECT * FROM temperatures
WHERE substr(quality_flags, 1, 1) = '0';
```

## Performance Characteristics

### Processing Speed
- **Throughput**: ~2.5M records processed in seconds
- **Memory Usage**: <100MB for entire UK dataset
- **Concurrency**: Automatic scaling to available CPU cores
- **I/O Efficiency**: Memory-mapped file reading for large datasets

### Output Optimization
- **Compression**: 22.9MB output for 2.5M records (SNAPPY)
- **Query Performance**: Columnar Parquet format optimized for analytics
- **Row Groups**: Configurable sizing for optimal query patterns
- **Schema Evolution**: Forward-compatible Parquet schema

### Scalability
- **Memory Efficient**: Streaming processing prevents memory exhaustion
- **CPU Scaling**: Linear performance improvement with additional cores
- **Storage**: Compressed output 10-20x smaller than CSV equivalent

## Error Handling

The processor provides comprehensive error reporting:

### Common Issues
1. **File Not Found**: Check input directory structure and file permissions
2. **Invalid Data Format**: Verify ECA&D file format compliance
3. **Memory Issues**: Reduce `--chunk-size` for memory-constrained environments
4. **Permission Errors**: Ensure write access to output directory

### Troubleshooting
- Use `--verbose` flag for detailed logging
- Check file permissions and directory structure
- Verify input data format matches ECA&D specification
- Monitor system resources during large dataset processing

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

### Code Organization
- `src/models/`: Data structures and validation
- `src/readers/`: File parsing and I/O operations
- `src/processors/`: Data transformation and validation logic
- `src/writers/`: Parquet file generation
- `src/cli/`: Command-line interface

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

- European Climate Assessment & Dataset (ECA&D) for providing the temperature data
- UK Met Office for weather station infrastructure
- Rust community for excellent crates and tooling
