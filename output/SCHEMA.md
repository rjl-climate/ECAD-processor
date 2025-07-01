# UK Temperature Data Parquet Schema

This document describes the schema of the consolidated temperature data Parquet files produced by the UK Temperature Processor.

## Overview

The Parquet file contains consolidated daily temperature records from UK weather stations, combining minimum, maximum, and average temperature measurements into a single denormalized format optimized for analytical queries.

## Schema Definition

| Column Name | Data Type | Description | Constraints |
|------------|-----------|-------------|-------------|
| `station_id` | INT32 | Unique station identifier (STAID) | NOT NULL |
| `station_name` | STRING | Name of the weather station | NOT NULL, Min length: 1 |
| `date` | DATE32 | Measurement date | NOT NULL |
| `latitude` | DOUBLE | Station latitude in decimal degrees | NOT NULL, Range: -90.0 to 90.0 |
| `longitude` | DOUBLE | Station longitude in decimal degrees | NOT NULL, Range: -180.0 to 180.0 |
| `min_temp` | FLOAT | Daily minimum temperature (°C) | NOT NULL, Range: -50.0 to 50.0 |
| `max_temp` | FLOAT | Daily maximum temperature (°C) | NOT NULL, Range: -50.0 to 50.0 |
| `avg_temp` | FLOAT | Daily average temperature (°C) | NOT NULL, Range: -50.0 to 50.0 |
| `quality_flags` | STRING | Quality flag indicators for min/avg/max | NOT NULL |

## Data Types

- **INT32**: 32-bit signed integer
- **STRING**: UTF-8 encoded variable-length string
- **DATE32**: Days since Unix epoch (1970-01-01)
- **DOUBLE**: 64-bit floating point (for precise coordinates)
- **FLOAT**: 32-bit floating point (sufficient for temperature precision)

## Data Integrity Rules

1. **Temperature Relationships**: The following relationship must hold (with 0.1°C tolerance):
   ```
   min_temp ≤ avg_temp ≤ max_temp
   ```

2. **Temperature Range**: All temperature values must be within realistic ranges for UK climate:
   - Minimum: -50.0°C to 50.0°C
   - Maximum: -50.0°C to 50.0°C
   - Average: -50.0°C to 50.0°C

3. **Coordinate Validation**:
   - Latitude: -90.0 to 90.0 (UK typically 49.5 to 61.0)
   - Longitude: -180.0 to 180.0 (UK typically -8.0 to 2.0)

## Quality Flags

The `quality_flags` field is a 3-character string encoding the quality status of each temperature measurement:

- **Position 1**: Minimum temperature quality
- **Position 2**: Average temperature quality  
- **Position 3**: Maximum temperature quality

### Quality Flag Values:
- `0`: Valid - passed all quality checks
- `1`: Suspect - flagged as potentially problematic but included
- `9`: Missing - no data available

Example: `"001"` means valid minimum temperature, valid average temperature, suspect maximum temperature.

## File Organization

### Compression
- **Default**: Snappy compression for balanced performance/size
- **Alternative options**: GZIP, LZ4, ZSTD (configurable via CLI)

### Row Group Size
- **Default**: 10,000 rows per row group
- Optimized for:
  - Efficient parallel processing
  - Memory-efficient streaming reads
  - Good compression ratios

### Sorting
Files are sorted by:
1. `station_id` (ascending)
2. `date` (ascending)

This ordering optimizes queries that:
- Select data for specific stations
- Perform time-series analysis
- Aggregate by station or date ranges

## Statistics

The Parquet file includes column statistics for query optimization:
- **Min/Max values** for all numeric columns
- **Null count** (should be 0 for all columns)
- **Distinct count** estimates for categorical columns

## Example Queries

### 1. Get all data for a specific station
```sql
SELECT * FROM temperatures 
WHERE station_id = 12345
ORDER BY date;
```

### 2. Find days with largest temperature range
```sql
SELECT date, station_name, (max_temp - min_temp) as temp_range
FROM temperatures
WHERE quality_flags = '000'  -- Only valid data
ORDER BY temp_range DESC
LIMIT 10;
```

### 3. Monthly average temperatures by station
```sql
SELECT 
    station_id,
    station_name,
    DATE_TRUNC('month', date) as month,
    AVG(avg_temp) as monthly_avg
FROM temperatures
GROUP BY station_id, station_name, month
ORDER BY station_id, month;
```

### 4. Find stations with suspect or missing data
```sql
SELECT DISTINCT station_id, station_name
FROM temperatures
WHERE quality_flags != '000';
```

## Performance Considerations

1. **Columnar Storage**: Parquet's columnar format enables:
   - Efficient compression (typically 5-10x)
   - Column pruning for queries
   - Vectorized processing

2. **Predicate Pushdown**: Statistics enable skipping row groups that don't match query predicates

3. **Memory Efficiency**: Streaming readers can process files larger than available RAM

## Version Information

- **Schema Version**: 1.0
- **Parquet Version**: 2.6
- **Arrow Compatibility**: Arrow format version 1.0