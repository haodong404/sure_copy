mod support;

use std::time::Instant;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use sure_copy_core::infrastructure::{FileSystem, LocalFileSystem};

use support::{build_runtime, create_file_tree, join_path, temp_dir};

fn fs_walk_benchmark(c: &mut Criterion) {
    let runtime = build_runtime();
    let adapter = LocalFileSystem::new();
    let fixture = temp_dir("fs_walk");
    let mut group = c.benchmark_group("read_dir_recursive");

    for file_count in [0_usize, 1_000, 10_000] {
        let root = join_path(fixture.path(), &format!("tree-{file_count}"));
        create_file_tree(&root, file_count, 4 * 1024);

        group.throughput(Throughput::Elements(file_count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(file_count), &root, |b, root| {
            b.iter_custom(|iters| {
                let start = Instant::now();
                runtime.block_on(async {
                    for _ in 0..iters {
                        let files = adapter
                            .read_dir_recursive(root)
                            .await
                            .expect("directory walk benchmark should succeed");
                        black_box(files);
                    }
                });
                start.elapsed()
            });
        });
    }

    group.finish();
}

criterion_group!(benches, fs_walk_benchmark);
criterion_main!(benches);
