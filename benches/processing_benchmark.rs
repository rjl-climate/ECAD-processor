use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use chrono::NaiveDate;
use ecad_processor::models::{StationMetadata, TemperatureRecord};
use ecad_processor::processors::{DataMerger, IntegrityChecker};
use ecad_processor::utils::coordinates::dms_to_decimal;
use std::collections::HashMap;

// Create test data for benchmarking
fn create_test_station_data(station_count: usize, days: usize) -> (Vec<StationMetadata>, Vec<TemperatureRecord>) {
    let mut stations = Vec::with_capacity(station_count);
    let mut temperature_records = Vec::new();
    
    for station_id in 1..=station_count {
        // Create station metadata
        let station = StationMetadata {
            staid: station_id as u32,
            name: format!("Test Station {}", station_id),
            country: "UK".to_string(),
            latitude: 51.0 + (station_id as f64) * 0.01,
            longitude: -1.0 - (station_id as f64) * 0.01,
            elevation: Some(100 + (station_id as i32) * 10),
        };
        stations.push(station);
        
        // Create temperature records for each day
        let base_date = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
        for day in 0..days {
            let date = base_date + chrono::Duration::days(day as i64);
            let base_temp = 15.0 + (day as f32) * 0.1 + (station_id as f32) * 0.5;
            
            temperature_records.push(TemperatureRecord {
                staid: station_id as u32,
                souid: 1,
                date,
                temperature: base_temp - 5.0, // min
                quality_flag: 0,
            });
            
            temperature_records.push(TemperatureRecord {
                staid: station_id as u32,
                souid: 2,
                date,
                temperature: base_temp + 5.0, // max
                quality_flag: 0,
            });
            
            temperature_records.push(TemperatureRecord {
                staid: station_id as u32,
                souid: 3,
                date,
                temperature: base_temp, // avg
                quality_flag: 0,
            });
        }
    }
    
    (stations, temperature_records)
}

fn benchmark_data_merger(c: &mut Criterion) {
    let (stations, temp_records) = create_test_station_data(10, 30);
    let station_map: HashMap<u32, StationMetadata> = stations.into_iter()
        .map(|s| (s.staid, s))
        .collect();
    
    c.bench_function("data_merger_v2", |b| {
        b.iter(|| {
            let merger = DataMerger::new();
            
            // Group temperatures by station and type
            let mut min_temps: HashMap<u32, Vec<TemperatureRecord>> = HashMap::new();
            let mut max_temps: HashMap<u32, Vec<TemperatureRecord>> = HashMap::new();
            let mut avg_temps: HashMap<u32, Vec<TemperatureRecord>> = HashMap::new();
            
            for record in &temp_records {
                match record.souid {
                    1 => min_temps.entry(record.staid).or_insert_with(Vec::new).push(record.clone()),
                    2 => max_temps.entry(record.staid).or_insert_with(Vec::new).push(record.clone()),
                    3 => avg_temps.entry(record.staid).or_insert_with(Vec::new).push(record.clone()),
                    _ => {}
                }
            }
            
            let mut results = Vec::new();
            for (station_id, min_vec) in min_temps {
                if let Some(station) = station_map.get(&station_id) {
                    let max_vec = max_temps.get(&station_id).cloned().unwrap_or_default();
                    let avg_vec = avg_temps.get(&station_id).cloned().unwrap_or_default();
                    
                    if let Ok(consolidated) = merger.merge_station_data(station, min_vec, max_vec, avg_vec) {
                        results.extend(consolidated);
                    }
                }
            }
            
            black_box(results.len())
        })
    });
}

fn benchmark_integrity_checker(c: &mut Criterion) {
    let (stations, temp_records) = create_test_station_data(5, 20);
    let station_map: HashMap<u32, StationMetadata> = stations.into_iter()
        .map(|s| (s.staid, s))
        .collect();
    
    // Create consolidated records for validation
    let merger = DataMerger::new();
    let mut consolidated_records = Vec::new();
    
    // Group temperatures by station and type
    let mut min_temps: HashMap<u32, Vec<TemperatureRecord>> = HashMap::new();
    let mut max_temps: HashMap<u32, Vec<TemperatureRecord>> = HashMap::new();
    let mut avg_temps: HashMap<u32, Vec<TemperatureRecord>> = HashMap::new();
    
    for record in &temp_records {
        match record.souid {
            1 => min_temps.entry(record.staid).or_insert_with(Vec::new).push(record.clone()),
            2 => max_temps.entry(record.staid).or_insert_with(Vec::new).push(record.clone()),
            3 => avg_temps.entry(record.staid).or_insert_with(Vec::new).push(record.clone()),
            _ => {}
        }
    }
    
    for (station_id, min_vec) in min_temps {
        if let Some(station) = station_map.get(&station_id) {
            let max_vec = max_temps.get(&station_id).cloned().unwrap_or_default();
            let avg_vec = avg_temps.get(&station_id).cloned().unwrap_or_default();
            
            if let Ok(consolidated) = merger.merge_station_data(station, min_vec, max_vec, avg_vec) {
                consolidated_records.extend(consolidated);
            }
        }
    }
    
    c.bench_function("integrity_checker_v2", |b| {
        b.iter(|| {
            let checker = IntegrityChecker::new();
            let report = checker.check_integrity(&consolidated_records);
            black_box(report.map(|r| r.total_records).unwrap_or(0))
        })
    });
}

fn benchmark_temperature_validation(c: &mut Criterion) {
    let test_temps: Vec<f32> = vec![
        -45.0, -30.0, -15.0, 0.0, 15.0, 30.0, 45.0, 50.0, 60.0, -60.0
    ];
    
    c.bench_function("temperature_validation_v2", |b| {
        b.iter(|| {
            let mut valid_count = 0;
            for &temp in &test_temps {
                // V2 validation logic - basic range check
                if temp >= -50.0 && temp <= 50.0 && temp.is_finite() {
                    valid_count += 1;
                }
            }
            black_box(valid_count)
        })
    });
}

fn benchmark_coordinate_conversion(c: &mut Criterion) {
    let dms_coordinates = vec![
        "51:30:15",
        "52:12:30", 
        "50:45:22",
        "53:18:45",
        "49:55:30"
    ];
    
    c.bench_function("coordinate_conversion_v2", |b| {
        b.iter(|| {
            let mut results = Vec::new();
            for dms in &dms_coordinates {
                if let Ok(decimal) = dms_to_decimal(dms) {
                    results.push(decimal);
                }
            }
            black_box(results.len())
        })
    });
}

fn benchmark_varying_data_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("data_processing_by_size");
    
    for &size in &[10, 50, 100, 500] {
        group.bench_with_input(
            BenchmarkId::new("stations", size),
            &size,
            |b, &station_count| {
                let (stations, temp_records) = create_test_station_data(station_count, 30);
                let station_map: HashMap<u32, StationMetadata> = stations.into_iter()
                    .map(|s| (s.staid, s))
                    .collect();
                
                b.iter(|| {
                    let merger = DataMerger::new();
                    
                    // Group temperatures by station and type
                    let mut min_temps: HashMap<u32, Vec<TemperatureRecord>> = HashMap::new();
                    let mut max_temps: HashMap<u32, Vec<TemperatureRecord>> = HashMap::new();
                    let mut avg_temps: HashMap<u32, Vec<TemperatureRecord>> = HashMap::new();
                    
                    for record in &temp_records {
                        match record.souid {
                            1 => min_temps.entry(record.staid).or_insert_with(Vec::new).push(record.clone()),
                            2 => max_temps.entry(record.staid).or_insert_with(Vec::new).push(record.clone()),
                            3 => avg_temps.entry(record.staid).or_insert_with(Vec::new).push(record.clone()),
                            _ => {}
                        }
                    }
                    
                    let mut results = Vec::new();
                    for (station_id, min_vec) in min_temps {
                        if let Some(station) = station_map.get(&station_id) {
                            let max_vec = max_temps.get(&station_id).cloned().unwrap_or_default();
                            let avg_vec = avg_temps.get(&station_id).cloned().unwrap_or_default();
                            
                            if let Ok(consolidated) = merger.merge_station_data(station, min_vec, max_vec, avg_vec) {
                                results.extend(consolidated);
                            }
                        }
                    }
                    
                    black_box(results.len())
                })
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    benchmark_data_merger,
    benchmark_integrity_checker,
    benchmark_temperature_validation,
    benchmark_coordinate_conversion,
    benchmark_varying_data_sizes
);
criterion_main!(benches);
