use thiserror::Error;

pub type Result<T> = std::result::Result<T, ProcessingError>;

#[derive(Error, Debug)]
pub enum ProcessingError {
    #[error("File I/O error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("CSV parsing error: {0}")]
    Csv(#[from] csv::Error),
    
    #[error("Date parsing error: {0}")]
    DateParse(#[from] chrono::ParseError),
    
    #[error("Temperature validation error: {message}")]
    TemperatureValidation { message: String },
    
    #[error("Station {station_id} not found")]
    StationNotFound { station_id: u32 },
    
    #[error("Parquet write error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    
    #[error("Configuration error: {0}")]
    Config(String),
    
    #[error("Validation error: {0}")]
    Validation(#[from] validator::ValidationErrors),
    
    #[error("Invalid coordinate format: {0}")]
    InvalidCoordinate(String),
    
    #[error("Invalid quality flag: {0}")]
    InvalidQualityFlag(u8),
    
    #[error("Data merge error: {0}")]
    DataMerge(String),
    
    #[error("Missing required data: {0}")]
    MissingData(String),
    
    #[error("Invalid data format: {0}")]
    InvalidFormat(String),
    
    #[error("Processing cancelled by user")]
    Cancelled,
    
    #[error("Async task error: {0}")]
    TaskJoin(#[from] tokio::task::JoinError),
}