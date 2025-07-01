# Weather Data Parquet Schemas

This document describes the Parquet schema formats supported by the ECAD processor.

## Overview

The Parquet file contains unified daily weather records from UK/Ireland weather stations, combining temperature, precipitation, and wind speed measurements into a single denormalized format optimized for analytical queries. The system supports multi-metric sparse data where individual records may contain different combinations of weather measurements.

## Current Schema: Multi-Metric Weather Records (v2.0)

### Core Schema Definition

| Column Name | Data Type | Description | Constraints |
|------------|-----------|-------------|-------------|
| `station_id` | UINT32 | Unique station identifier (STAID) | NOT NULL |
| `station_name` | STRING | Name of the weather station | NOT NULL, Min length: 1 |
| `date` | DATE32 | Measurement date | NOT NULL |
| `latitude` | DOUBLE | Station latitude in decimal degrees | NOT NULL, Range: -90.0 to 90.0 |
| `longitude` | DOUBLE | Station longitude in decimal degrees | NOT NULL, Range: -180.0 to 180.0 |

### Weather Metrics (Optional Fields)

| Column Name | Data Type | Description | Constraints |
|------------|-----------|-------------|-------------|
| `temp_min` | FLOAT | Daily minimum temperature (°C) | NULLABLE, Range: -90.0 to 60.0 |
| `temp_max` | FLOAT | Daily maximum temperature (°C) | NULLABLE, Range: -90.0 to 60.0 |
| `temp_avg` | FLOAT | Daily average temperature (°C) | NULLABLE, Range: -90.0 to 60.0 |
| `precipitation` | FLOAT | Daily precipitation (mm) | NULLABLE, Range: 0.0 to 2000.0 |
| `wind_speed` | FLOAT | Daily wind speed (m/s) | NULLABLE, Range: 0.0 to 120.0 |

### Quality Flags (ECAD Original)

| Column Name | Data Type | Description | Constraints |
|------------|-----------|-------------|-------------|
| `temp_quality` | STRING | Temperature quality flags | NULLABLE, Format: "000" (min/avg/max) |
| `precip_quality` | STRING | Precipitation quality flag | NULLABLE, Format: "0" |
| `wind_quality` | STRING | Wind speed quality flag | NULLABLE, Format: "0" |

### Physical Validation Fields

| Column Name | Data Type | Description | Constraints |
|------------|-----------|-------------|-------------|
| `temp_validation` | STRING | Physical validation: "Valid", "Suspect", "Invalid" | NULLABLE |
| `precip_validation` | STRING | Physical validation: "Valid", "Suspect", "Invalid" | NULLABLE |
| `wind_validation` | STRING | Physical validation: "Valid", "Suspect", "Invalid" | NULLABLE |

## Data Types

- **UINT32**: 32-bit unsigned integer
- **STRING**: UTF-8 encoded variable-length string
- **DATE32**: Days since Unix epoch (1970-01-01)
- **DOUBLE**: 64-bit floating point (for precise coordinates)
- **FLOAT**: 32-bit floating point (sufficient for weather measurements)

## Sparse Data Model

The schema is designed to handle **sparse data** where:
- Not all stations measure all weather metrics
- Individual records may have any combination of temperature, precipitation, and wind data
- NULL values indicate missing measurements (not measurement errors)

### Example Record Patterns:
```
Station A: temp_min=5.0, temp_max=15.0, temp_avg=10.0, precipitation=NULL, wind_speed=NULL
Station B: temp_min=NULL, temp_max=NULL, temp_avg=NULL, precipitation=12.5, wind_speed=NULL  
Station C: temp_min=8.0, temp_max=18.0, temp_avg=13.0, precipitation=2.1, wind_speed=5.3
```

## Quality System Architecture

### Two-Layer Quality Assessment

#### 1. ECAD Quality Flags (Original)
- **Source**: European Climate Assessment & Dataset quality indicators
- **Values**: 
  - `0`: Valid - passed all ECAD quality checks
  - `1`: Suspect - flagged as potentially problematic but included
  - `9`: Missing - no data available
- **Format**: 
  - Temperature: 3-character string (min/avg/max)
  - Precipitation/Wind: 1-character string

#### 2. Physical Validation (Enhanced)
- **Source**: ECAD processor physical plausibility checks
- **Values**:
  - `Valid`: Within normal physical limits for UK/Ireland
  - `Suspect`: Unusual but physically possible (extreme events)
  - `Invalid`: Physically impossible (excluded from analysis)

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

## Data Integrity Rules

### 1. Temperature Relationships
When all three temperature measurements are present:
```
temp_min ≤ temp_avg ≤ temp_max (within 1.0°C tolerance)
```

### 2. Geographic Constraints
- **UK/Ireland bounds**: Latitude 49.2°N-60.9°N, Longitude 8.1°W-1.7°E
- **Global bounds**: Latitude -90.0 to 90.0, Longitude -180.0 to 180.0

### 3. Quality Flag Consistency
- Physical validation may flag records as invalid even if ECAD flags show valid
- Combined quality assessment considers both layers

## File Organization

### Compression
- **Default**: Snappy compression for balanced performance/size
- **Alternative options**: GZIP, LZ4, ZSTD (configurable via CLI)

### Row Group Size
- **Default**: 10,000 rows per row group
- Optimized for memory-efficient streaming and good compression ratios

### Sorting
Files are sorted by:
1. `station_id` (ascending)
2. `date` (ascending)

This ordering optimizes queries for:
- Station-specific time series analysis
- Date range queries across stations
- Efficient predicate pushdown

## Example Queries

### 1. Get all available data for a specific station
```sql
SELECT * FROM weather_data 
WHERE station_id = 12345
ORDER BY date;
```

### 2. Find records with multiple weather metrics
```sql
SELECT station_name, date, temp_avg, precipitation, wind_speed
FROM weather_data
WHERE temp_avg IS NOT NULL 
  AND precipitation IS NOT NULL 
  AND wind_speed IS NOT NULL
ORDER BY date DESC
LIMIT 100;
```

### 3. Analyze data quality by metric
```sql
SELECT 
    COUNT(*) as total_records,
    COUNT(temp_min) as temp_records,
    COUNT(precipitation) as precip_records,
    COUNT(wind_speed) as wind_records,
    SUM(CASE WHEN temp_validation = 'Invalid' THEN 1 ELSE 0 END) as invalid_temp
FROM weather_data;
```

### 4. Find extreme weather events (excluding invalid data)
```sql
SELECT date, station_name, temp_max, precipitation, wind_speed
FROM weather_data
WHERE (temp_max > 35.0 OR precipitation > 100.0 OR wind_speed > 30.0)
  AND temp_validation != 'Invalid'
  AND precip_validation != 'Invalid'
  AND wind_validation != 'Invalid'
ORDER BY temp_max DESC, precipitation DESC, wind_speed DESC;
```

### 5. Monthly averages with quality filtering
```sql
SELECT 
    station_id,
    station_name,
    DATE_TRUNC('month', date) as month,
    AVG(temp_avg) as monthly_temp,
    AVG(precipitation) as monthly_precip,
    COUNT(*) as record_count
FROM weather_data
WHERE temp_validation = 'Valid'
  AND precip_validation = 'Valid'
GROUP BY station_id, station_name, month
HAVING COUNT(*) >= 20  -- Ensure sufficient data points
ORDER BY station_id, month;
```

## Performance Considerations

### 1. Columnar Storage Benefits
- **Column pruning**: Only read required metrics
- **Efficient compression**: Weather data compresses 5-10x
- **Vectorized processing**: Fast analytical operations

### 2. Null Value Optimization
- Parquet efficiently stores sparse data with many NULL values
- Query engines can skip processing NULL columns

### 3. Predicate Pushdown
- Station and date filters push down to row group level
- Quality validation filters optimize on encoded values

### 4. Memory Efficiency
- Streaming readers handle files larger than available RAM
- Row group size balances memory usage and I/O efficiency

## Schema Compatibility

### Backward Compatibility
- **Legacy format**: 9-column consolidated temperature records (deprecated)
- **Current format**: 16-column multi-metric records with validation
- **Schema detection**: Automatic detection based on column count

### Migration Path
Files are automatically upgraded to the current schema when processed through the ECAD processor.

## Version Information

- **Schema Version**: 2.0 (Multi-Metric with Physical Validation)
- **Previous Version**: 1.0 (Temperature-only consolidated records)
- **Parquet Version**: 2.6
- **Arrow Compatibility**: Arrow format version 1.0
- **ECAD Processor**: Rust implementation v1.0

## Quality Assurance

### Data Quality Statistics (Typical)
- **ECAD Valid**: ~98% of measurements
- **Physical Valid**: ~99.9% of measurements  
- **Invalid Records**: <0.01% (excluded from analysis)
- **Suspect Records**: 0.1-2% (flagged for review)

### Extreme Value Detection
The processor automatically identifies and flags physically impossible values:
- Temperature below absolute zero
- Negative precipitation or wind speed
- Measurements exceeding global records by significant margins

These values are preserved in the dataset but excluded from statistical analysis and extreme record identification.