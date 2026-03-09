mod support;

use std::time::Instant;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use sure_copy_core::infrastructure::{ChecksumProvider, Sha256ChecksumProvider};

use support::{build_runtime, create_file, file_size_label, join_path, temp_dir};

fn checksum_benchmark(c: &mut Criterion) {
    let runtime = build_runtime();
    let provider = Sha256ChecksumProvider::new();
    let fixture = temp_dir("checksum");
    let mut group = c.benchmark_group("checksum_file");

    for size_bytes in [4 * 1024, 1024 * 1024, 16 * 1024 * 1024] {
        let file_path = join_path(
            fixture.path(),
            &format!("{}.bin", file_size_label(size_bytes)),
        );
        create_file(&file_path, size_bytes);

        group.throughput(Throughput::Bytes(size_bytes as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(file_size_label(size_bytes)),
            &file_path,
            |b, file_path| {
                b.iter_custom(|iters| {
                    let start = Instant::now();
                    runtime.block_on(async {
                        for _ in 0..iters {
                            let digest = provider
                                .checksum_file(file_path)
                                .await
                                .expect("checksum benchmark should succeed");
                            black_box(digest);
                        }
                    });
                    start.elapsed()
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, checksum_benchmark);
criterion_main!(benches);
