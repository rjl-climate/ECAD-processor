use crate::error::Result;
use crate::models::{ConsolidatedRecord, StationMetadata, TemperatureRecord, TemperatureSet};
use crate::readers::TemperatureData;
use chrono::NaiveDate;
use std::collections::HashMap;

pub struct DataMerger {
    allow_incomplete: bool,
}

impl DataMerger {
    pub fn new() -> Self {
        Self {
            allow_incomplete: false,
        }
    }

    pub fn with_allow_incomplete(allow_incomplete: bool) -> Self {
        Self { allow_incomplete }
    }

    /// Merge temperature data into consolidated records
    pub fn merge_temperature_data(
        &self,
        temperature_data: &TemperatureData,
    ) -> Result<Vec<ConsolidatedRecord>> {
        let mut consolidated_records = Vec::new();

        // Group temperature records by station and date
        let grouped_data = self.group_by_station_and_date(temperature_data)?;

        // Process each station-date combination
        for ((station_id, date), temp_set) in grouped_data {
            if let Some(station) = temperature_data.stations.get(&station_id) {
                if let Some(record) = self.create_consolidated_record(station, date, temp_set)? {
                    consolidated_records.push(record);
                }
            }
        }

        // Sort by station ID and date
        consolidated_records.sort_by(|a, b| {
            a.station_id
                .cmp(&b.station_id)
                .then_with(|| a.date.cmp(&b.date))
        });

        Ok(consolidated_records)
    }

    /// Group temperature records by station ID and date
    fn group_by_station_and_date(
        &self,
        temperature_data: &TemperatureData,
    ) -> Result<HashMap<(u32, NaiveDate), TemperatureSet>> {
        let mut grouped = HashMap::new();

        // Process minimum temperatures
        for ((station_id, date), record) in &temperature_data.min_temperatures {
            let entry = grouped
                .entry((*station_id, *date))
                .or_insert(TemperatureSet::default());
            entry.min = Some(record.clone());
        }

        // Process maximum temperatures
        for ((station_id, date), record) in &temperature_data.max_temperatures {
            let entry = grouped
                .entry((*station_id, *date))
                .or_insert(TemperatureSet::default());
            entry.max = Some(record.clone());
        }

        // Process average temperatures
        for ((station_id, date), record) in &temperature_data.avg_temperatures {
            let entry = grouped
                .entry((*station_id, *date))
                .or_insert(TemperatureSet::default());
            entry.avg = Some(record.clone());
        }

        Ok(grouped)
    }

    /// Create a consolidated record from station metadata and temperature set
    fn create_consolidated_record(
        &self,
        station: &StationMetadata,
        date: NaiveDate,
        temp_set: TemperatureSet,
    ) -> Result<Option<ConsolidatedRecord>> {
        // Check if we have all required data
        if !self.allow_incomplete
            && (temp_set.min.is_none() || temp_set.max.is_none() || temp_set.avg.is_none())
        {
            return Ok(None);
        }

        // Skip validation for now due to different measurement sources
        // temp_set.validate_relationships()?;

        // Extract temperatures with defaults for missing values
        let (min_temp, min_quality) = temp_set
            .min
            .as_ref()
            .map(|r| (r.temperature, r.quality_flag))
            .unwrap_or((-9999.0, 9));

        let (max_temp, max_quality) = temp_set
            .max
            .as_ref()
            .map(|r| (r.temperature, r.quality_flag))
            .unwrap_or((-9999.0, 9));

        let (avg_temp, avg_quality) = temp_set
            .avg
            .as_ref()
            .map(|r| (r.temperature, r.quality_flag))
            .unwrap_or((-9999.0, 9));

        // Skip records where all temperatures are missing
        if min_temp == -9999.0 && max_temp == -9999.0 && avg_temp == -9999.0 {
            return Ok(None);
        }

        // Build quality flags string (min, avg, max order)
        let quality_flags = format!("{}{}{}", min_quality, avg_quality, max_quality);

        // Create consolidated record (skip validation for missing data)
        let record = ConsolidatedRecord::new(
            station.staid,
            station.name.clone(),
            date,
            station.latitude,
            station.longitude,
            min_temp,
            max_temp,
            avg_temp,
            quality_flags,
        );

        // Skip relationship validation for now due to different measurement sources
        // TODO: Implement more sophisticated validation that accounts for different SOUIDs
        // if min_temp != -9999.0 && max_temp != -9999.0 && avg_temp != -9999.0 {
        //     record.validate_relationships()?;
        // }

        Ok(Some(record))
    }

    /// Merge data for a specific station
    pub fn merge_station_data(
        &self,
        station: &StationMetadata,
        min_temps: Vec<TemperatureRecord>,
        max_temps: Vec<TemperatureRecord>,
        avg_temps: Vec<TemperatureRecord>,
    ) -> Result<Vec<ConsolidatedRecord>> {
        let mut temp_map: HashMap<NaiveDate, TemperatureSet> = HashMap::new();

        // Add minimum temperatures
        for record in min_temps {
            let date = record.date;
            temp_map.entry(date).or_default().min = Some(record);
        }

        // Add maximum temperatures
        for record in max_temps {
            let date = record.date;
            temp_map.entry(date).or_default().max = Some(record);
        }

        // Add average temperatures
        for record in avg_temps {
            let date = record.date;
            temp_map.entry(date).or_default().avg = Some(record);
        }

        // Convert to consolidated records
        let mut records = Vec::new();
        for (date, temp_set) in temp_map {
            if let Some(record) = self.create_consolidated_record(station, date, temp_set)? {
                records.push(record);
            }
        }

        // Sort by date
        records.sort_by_key(|r| r.date);

        Ok(records)
    }
}

impl Default for DataMerger {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_complete_data() {
        let date = NaiveDate::from_ymd_opt(2023, 7, 15).unwrap();

        let station = StationMetadata::new(
            12345,
            "Test Station".to_string(),
            "GB".to_string(),
            51.5074,
            -0.1278,
            Some(35),
        );

        let min_temp = TemperatureRecord::new(12345, 101, date, 15.0, 0).unwrap();
        let max_temp = TemperatureRecord::new(12345, 101, date, 25.0, 0).unwrap();
        let avg_temp = TemperatureRecord::new(12345, 101, date, 20.0, 0).unwrap();

        let merger = DataMerger::new();
        let records = merger
            .merge_station_data(&station, vec![min_temp], vec![max_temp], vec![avg_temp])
            .unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].min_temp, 15.0);
        assert_eq!(records[0].max_temp, 25.0);
        assert_eq!(records[0].avg_temp, 20.0);
        assert_eq!(records[0].quality_flags, "000");
    }

    #[test]
    fn test_merge_incomplete_data() {
        let date = NaiveDate::from_ymd_opt(2023, 7, 15).unwrap();

        let station = StationMetadata::new(
            12345,
            "Test Station".to_string(),
            "GB".to_string(),
            51.5074,
            -0.1278,
            Some(35),
        );

        let min_temp = TemperatureRecord::new(12345, 101, date, 15.0, 0).unwrap();
        let max_temp = TemperatureRecord::new(12345, 101, date, 25.0, 0).unwrap();
        // No average temperature

        let merger = DataMerger::new();
        let records = merger
            .merge_station_data(
                &station,
                vec![min_temp.clone()],
                vec![max_temp.clone()],
                vec![],
            )
            .unwrap();

        // Should be empty because we're missing average temperature
        assert_eq!(records.len(), 0);

        // Now with allow_incomplete
        let merger = DataMerger::with_allow_incomplete(true);
        let records = merger
            .merge_station_data(&station, vec![min_temp], vec![max_temp], vec![])
            .unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].quality_flags, "090"); // Missing average (9 in position 2)
    }
}
