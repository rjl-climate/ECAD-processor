/// Temperature data type identifiers
pub const TEMP_TYPE_MIN: &str = "min";
pub const TEMP_TYPE_MAX: &str = "max";
pub const TEMP_TYPE_AVG: &str = "avg";

/// File names
pub const STATIONS_FILE: &str = "stations.txt";
pub const METADATA_FILE: &str = "metadata.txt";
pub const SOURCES_FILE: &str = "sources.txt";
pub const ELEMENTS_FILE: &str = "elements.txt";

/// Directory names
pub const UK_TEMP_MIN_DIR: &str = "uk_temp_min";
pub const UK_TEMP_MAX_DIR: &str = "uk_temp_max";
pub const UK_TEMP_AVG_DIR: &str = "uk_temp_avg";

/// Temperature constraints
pub const MIN_VALID_TEMP: f32 = -50.0;
pub const MAX_VALID_TEMP: f32 = 50.0;
pub const TEMP_TOLERANCE: f32 = 0.1;

/// UK geographic bounds
pub const UK_MIN_LAT: f64 = 49.5;
pub const UK_MAX_LAT: f64 = 61.0;
pub const UK_MIN_LON: f64 = -8.0;
pub const UK_MAX_LON: f64 = 2.0;

/// Processing defaults
pub const DEFAULT_CHUNK_SIZE: usize = 1000;
pub const DEFAULT_ROW_GROUP_SIZE: usize = 10000;
pub const DEFAULT_BUFFER_SIZE: usize = 8192 * 16; // 128KB

/// Quality flag values
pub const QUALITY_VALID: u8 = 0;
pub const QUALITY_SUSPECT: u8 = 1;
pub const QUALITY_MISSING: u8 = 9;

/// Parquet compression options
pub const COMPRESSION_SNAPPY: &str = "snappy";
pub const COMPRESSION_GZIP: &str = "gzip";
pub const COMPRESSION_LZ4: &str = "lz4";
pub const COMPRESSION_ZSTD: &str = "zstd";
pub const COMPRESSION_NONE: &str = "none";
