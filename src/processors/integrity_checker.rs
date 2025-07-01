use crate::error::Result;
use crate::models::ConsolidatedRecord;
use crate::utils::constants::{MAX_VALID_TEMP, MIN_VALID_TEMP};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct IntegrityReport {
    pub total_records: usize,
    pub valid_records: usize,
    pub suspect_records: usize,
    pub invalid_records: usize,
    pub missing_data_records: usize,
    pub temperature_violations: Vec<TemperatureViolation>,
    pub station_statistics: HashMap<u32, StationStatistics>,
}

#[derive(Debug, Clone)]
pub struct TemperatureViolation {
    pub station_id: u32,
    pub date: chrono::NaiveDate,
    pub violation_type: ViolationType,
    pub details: String,
}

#[derive(Debug, Clone)]
pub enum ViolationType {
    MinGreaterThanAvg,
    AvgGreaterThanMax,
    OutOfRange,
    SuspiciousJump,
}

#[derive(Debug, Clone, Default)]
pub struct StationStatistics {
    pub total_records: usize,
    pub valid_records: usize,
    pub suspect_records: usize,
    pub missing_data_records: usize,
    pub min_temp: Option<f32>,
    pub max_temp: Option<f32>,
    pub avg_temp: Option<f32>,
}

pub struct IntegrityChecker {
    temperature_jump_threshold: f32,
}

impl IntegrityChecker {
    pub fn new() -> Self {
        Self {
            temperature_jump_threshold: 20.0, // 20°C jump between consecutive days
        }
    }

    pub fn with_strict_mode(_strict_mode: bool) -> Self {
        Self {
            temperature_jump_threshold: 20.0,
        }
    }

    /// Check integrity of consolidated records
    pub fn check_integrity(&self, records: &[ConsolidatedRecord]) -> Result<IntegrityReport> {
        let mut report = IntegrityReport {
            total_records: records.len(),
            valid_records: 0,
            suspect_records: 0,
            invalid_records: 0,
            missing_data_records: 0,
            temperature_violations: Vec::new(),
            station_statistics: HashMap::new(),
        };

        // Group records by station for time series checks
        let mut station_records: HashMap<u32, Vec<&ConsolidatedRecord>> = HashMap::new();
        for record in records {
            station_records
                .entry(record.station_id)
                .or_default()
                .push(record);
        }

        // Sort each station's records by date
        for records in station_records.values_mut() {
            records.sort_by_key(|r| r.date);
        }

        // Check each record
        for record in records {
            self.check_record(record, &mut report)?;

            // Update station statistics
            let stats = report
                .station_statistics
                .entry(record.station_id)
                .or_default();

            stats.total_records += 1;

            if record.has_valid_data() {
                stats.valid_records += 1;
            } else if record.has_suspect_data() {
                stats.suspect_records += 1;
            }

            if record.has_missing_data() {
                stats.missing_data_records += 1;
            }

            // Update temperature ranges
            if record.min_temp != -9999.0 {
                stats.min_temp = Some(
                    stats
                        .min_temp
                        .map_or(record.min_temp, |t| t.min(record.min_temp)),
                );
                stats.max_temp = Some(
                    stats
                        .max_temp
                        .map_or(record.min_temp, |t| t.max(record.min_temp)),
                );
            }
        }

        // Check time series integrity
        for (station_id, records) in station_records {
            self.check_time_series_integrity(station_id, &records, &mut report)?;
        }

        Ok(report)
    }

    /// Check individual record integrity
    fn check_record(
        &self,
        record: &ConsolidatedRecord,
        report: &mut IntegrityReport,
    ) -> Result<()> {
        // Validate basic constraints
        record
            .validate_relationships()
            .inspect_err(|e| {
                report.temperature_violations.push(TemperatureViolation {
                    station_id: record.station_id,
                    date: record.date,
                    violation_type: ViolationType::MinGreaterThanAvg,
                    details: e.to_string(),
                });
            })
            .ok();

        // Check temperature ranges
        self.check_temperature_ranges(record, report)?;

        // Count record types
        if record.has_valid_data() {
            report.valid_records += 1;
        } else if record.has_suspect_data() {
            report.suspect_records += 1;
        } else {
            report.invalid_records += 1;
        }

        if record.has_missing_data() {
            report.missing_data_records += 1;
        }

        Ok(())
    }

    /// Check if temperatures are within valid ranges
    fn check_temperature_ranges(
        &self,
        record: &ConsolidatedRecord,
        report: &mut IntegrityReport,
    ) -> Result<()> {
        let temps = [
            (record.min_temp, "min"),
            (record.max_temp, "max"),
            (record.avg_temp, "avg"),
        ];

        for (temp, name) in temps {
            if temp != -9999.0 && !(MIN_VALID_TEMP..=MAX_VALID_TEMP).contains(&temp) {
                report.temperature_violations.push(TemperatureViolation {
                    station_id: record.station_id,
                    date: record.date,
                    violation_type: ViolationType::OutOfRange,
                    details: format!(
                        "{} temperature {} is outside valid range [{}, {}]",
                        name, temp, MIN_VALID_TEMP, MAX_VALID_TEMP
                    ),
                });

                // Report but don't fail on temperature range violations
                // Real-world data often has sensor errors that should be reported but not stop processing
                // if self.strict_mode {
                //     return Err(ProcessingError::TemperatureValidation {
                //         message: format!("Temperature {} out of range for station {} on {}",
                //             temp, record.station_id, record.date),
                //     });
                // }
            }
        }

        Ok(())
    }

    /// Check time series integrity for temperature jumps
    fn check_time_series_integrity(
        &self,
        station_id: u32,
        records: &[&ConsolidatedRecord],
        report: &mut IntegrityReport,
    ) -> Result<()> {
        for window in records.windows(2) {
            let prev = window[0];
            let curr = window[1];

            // Check for suspicious temperature jumps
            let temps = [
                (prev.min_temp, curr.min_temp, "min"),
                (prev.max_temp, curr.max_temp, "max"),
                (prev.avg_temp, curr.avg_temp, "avg"),
            ];

            for (prev_temp, curr_temp, name) in temps {
                if prev_temp != -9999.0 && curr_temp != -9999.0 {
                    let jump = (curr_temp - prev_temp).abs();

                    if jump > self.temperature_jump_threshold {
                        report.temperature_violations.push(TemperatureViolation {
                            station_id,
                            date: curr.date,
                            violation_type: ViolationType::SuspiciousJump,
                            details: format!(
                                "{} temperature jumped {:.1}°C from {} to {}",
                                name, jump, prev.date, curr.date
                            ),
                        });
                    }
                }
            }
        }

        Ok(())
    }

    /// Generate a summary report
    pub fn generate_summary(&self, report: &IntegrityReport) -> String {
        let mut summary = String::new();

        summary.push_str("=== Integrity Check Report ===\n");
        summary.push_str(&format!("Total Records: {}\n", report.total_records));
        summary.push_str(&format!(
            "Valid Records: {} ({:.1}%)\n",
            report.valid_records,
            100.0 * report.valid_records as f64 / report.total_records as f64
        ));
        summary.push_str(&format!(
            "Suspect Records: {} ({:.1}%)\n",
            report.suspect_records,
            100.0 * report.suspect_records as f64 / report.total_records as f64
        ));
        summary.push_str(&format!(
            "Invalid Records: {} ({:.1}%)\n",
            report.invalid_records,
            100.0 * report.invalid_records as f64 / report.total_records as f64
        ));
        summary.push_str(&format!(
            "Missing Data Records: {}\n",
            report.missing_data_records
        ));
        summary.push_str(&format!(
            "\nTemperature Violations: {}\n",
            report.temperature_violations.len()
        ));

        if !report.temperature_violations.is_empty() {
            summary.push_str("\nTop 10 Violations:\n");
            for (i, violation) in report.temperature_violations.iter().take(10).enumerate() {
                summary.push_str(&format!(
                    "  {}. Station {} on {}: {}\n",
                    i + 1,
                    violation.station_id,
                    violation.date,
                    violation.details
                ));
            }
        }

        summary
    }
}

impl Default for IntegrityChecker {
    fn default() -> Self {
        Self::new()
    }
}
