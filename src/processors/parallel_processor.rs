use crate::error::Result;
use crate::models::{ConsolidatedRecord, StationMetadata};
use crate::processors::{DataMerger, IntegrityChecker, IntegrityReport};
use crate::readers::ConcurrentReader;
use crate::utils::progress::ProgressReporter;
use rayon::prelude::*;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub struct ParallelProcessor {
    max_workers: usize,
    chunk_size: usize,
    allow_incomplete: bool,
    strict_validation: bool,
}

impl ParallelProcessor {
    pub fn new(max_workers: usize) -> Self {
        Self {
            max_workers,
            chunk_size: 1000,
            allow_incomplete: false,
            strict_validation: false,
        }
    }

    pub fn with_chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    pub fn with_allow_incomplete(mut self, allow_incomplete: bool) -> Self {
        self.allow_incomplete = allow_incomplete;
        self
    }

    pub fn with_strict_validation(mut self, strict_validation: bool) -> Self {
        self.strict_validation = strict_validation;
        self
    }

    /// Process all temperature data using parallel processing
    pub async fn process_all_data(
        &self,
        base_path: &Path,
        progress: Option<&ProgressReporter>,
    ) -> Result<(Vec<ConsolidatedRecord>, IntegrityReport)> {
        if let Some(p) = progress {
            p.set_message("Reading temperature data...");
        }

        // Read all temperature data concurrently
        let reader = ConcurrentReader::new(self.max_workers);
        let temperature_data = reader.read_all_temperature_data(base_path).await?;

        if let Some(p) = progress {
            p.set_message("Merging temperature data...");
        }

        // Merge data into consolidated records
        let merger = DataMerger::with_allow_incomplete(self.allow_incomplete);
        let consolidated_records = merger.merge_temperature_data(&temperature_data)?;

        if let Some(p) = progress {
            p.set_message("Checking data integrity...");
        }

        // Check integrity
        let checker = IntegrityChecker::with_strict_mode(self.strict_validation);
        let integrity_report = checker.check_integrity(&consolidated_records)?;

        if let Some(p) = progress {
            p.finish_with_message("Processing complete");
        }

        Ok((consolidated_records, integrity_report))
    }

    /// Process temperature data by station in parallel
    pub fn process_by_stations(
        &self,
        stations: Vec<StationMetadata>,
        base_path: &Path,
        progress: Option<&ProgressReporter>,
    ) -> Result<(Vec<ConsolidatedRecord>, IntegrityReport)> {
        let total_stations = stations.len();
        let processed_count = Arc::new(AtomicUsize::new(0));

        if let Some(p) = progress {
            p.set_message(&format!("Processing {} stations...", total_stations));
        }

        // Configure Rayon thread pool
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(self.max_workers)
            .build()
            .map_err(|e| crate::error::ProcessingError::Config(e.to_string()))?;

        // Process stations in parallel
        let all_records: Result<Vec<Vec<ConsolidatedRecord>>> = pool.install(|| {
            stations
                .par_iter()
                .map(|station| {
                    let result = self.process_single_station(station, base_path);

                    // Update progress
                    let count = processed_count.fetch_add(1, Ordering::Relaxed) + 1;
                    if let Some(p) = progress {
                        p.update(count as u64);
                    }

                    result
                })
                .collect()
        });

        let all_records = all_records?;

        // Flatten results
        let mut consolidated_records: Vec<ConsolidatedRecord> =
            all_records.into_iter().flatten().collect();

        // Sort by station ID and date
        consolidated_records.sort_by(|a, b| {
            a.station_id
                .cmp(&b.station_id)
                .then_with(|| a.date.cmp(&b.date))
        });

        if let Some(p) = progress {
            p.set_message("Checking data integrity...");
        }

        // Check integrity
        let checker = IntegrityChecker::with_strict_mode(self.strict_validation);
        let integrity_report = checker.check_integrity(&consolidated_records)?;

        if let Some(p) = progress {
            p.finish_with_message(&format!("Processed {} stations", total_stations));
        }

        Ok((consolidated_records, integrity_report))
    }

    /// Process a single station's data
    fn process_single_station(
        &self,
        station: &StationMetadata,
        base_path: &Path,
    ) -> Result<Vec<ConsolidatedRecord>> {
        let reader = ConcurrentReader::new(1);
        let station_data = reader.process_station_data(station.staid, base_path)?;

        let merger = DataMerger::with_allow_incomplete(self.allow_incomplete);
        merger.merge_station_data(
            station,
            station_data.min_temperatures,
            station_data.max_temperatures,
            station_data.avg_temperatures,
        )
    }

    /// Process records in batches for memory efficiency
    pub fn process_in_batches<F>(
        &self,
        records: Vec<ConsolidatedRecord>,
        batch_processor: F,
        progress: Option<&ProgressReporter>,
    ) -> Result<()>
    where
        F: Fn(&[ConsolidatedRecord]) -> Result<()> + Sync + Send,
    {
        let total_batches = records.len().div_ceil(self.chunk_size);
        let processed_batches = Arc::new(AtomicUsize::new(0));

        if let Some(p) = progress {
            p.set_message(&format!("Processing {} batches...", total_batches));
        }

        // Process batches in parallel
        records.par_chunks(self.chunk_size).try_for_each(|batch| {
            let result = batch_processor(batch);

            // Update progress
            let count = processed_batches.fetch_add(1, Ordering::Relaxed) + 1;
            if let Some(p) = progress {
                p.update(count as u64);
            }

            result
        })?;

        if let Some(p) = progress {
            p.finish_with_message("Batch processing complete");
        }

        Ok(())
    }
}

impl Default for ParallelProcessor {
    fn default() -> Self {
        Self::new(num_cpus::get())
    }
}
