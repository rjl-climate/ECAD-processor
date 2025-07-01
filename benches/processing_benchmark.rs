use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn benchmark_station_processing(c: &mut Criterion) {
    c.bench_function("process_station", |b| {
        b.iter(|| {
            // TODO: Implement actual benchmark
            black_box(42)
        })
    });
}

criterion_group!(benches, benchmark_station_processing);
criterion_main!(benches);
