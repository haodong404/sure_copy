mod support;

use std::time::{Duration, Instant};

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use sure_copy_core::{
    CopyTask, OrchestratorConfig, SqliteTaskOrchestrator, TaskOptions, TaskOrchestrator,
    VerificationPolicy,
};

use support::{build_runtime, create_file_tree, join_path, temp_dir};

fn persistent_copy_benchmark(c: &mut Criterion) {
    let runtime = build_runtime();
    let fixture_root = temp_dir("persistent_copy");
    let mut group = c.benchmark_group("persistent_copy_run");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));

    for (case_name, file_count, file_size_bytes, verification_policy, destination_count) in [
        (
            "tiny_files_no_verify",
            1_000_usize,
            4 * 1024_usize,
            VerificationPolicy::None,
            1_usize,
        ),
        (
            "tiny_files_post_copy",
            1_000_usize,
            4 * 1024_usize,
            VerificationPolicy::PostWrite,
            1_usize,
        ),
        (
            "large_files_no_verify",
            8_usize,
            8 * 1024 * 1024_usize,
            VerificationPolicy::None,
            1_usize,
        ),
        (
            "large_files_post_copy",
            8_usize,
            8 * 1024 * 1024_usize,
            VerificationPolicy::PostWrite,
            1_usize,
        ),
        (
            "multi_dest_source_observer",
            128_usize,
            64 * 1024_usize,
            VerificationPolicy::SourceHashAndPostWrite,
            2_usize,
        ),
    ] {
        let source_root = join_path(fixture_root.path(), &format!("source-{case_name}"));
        let total_bytes = create_file_tree(&source_root, file_count, file_size_bytes);

        group.throughput(Throughput::Bytes(total_bytes * destination_count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(case_name),
            &source_root,
            |b, source_root| {
                b.iter_custom(|iters| {
                    let start = Instant::now();
                    for iteration in 0..iters {
                        let run_root = temp_dir("persistent_copy_run");
                        let db_path = join_path(run_root.path(), "tasks.db");
                        let destinations = (0..destination_count)
                            .map(|index| join_path(run_root.path(), &format!("dest-{index}")))
                            .collect::<Vec<_>>();

                        runtime.block_on(async {
                            let orchestrator = SqliteTaskOrchestrator::new(
                                &db_path,
                                OrchestratorConfig::default(),
                            )
                            .await
                            .expect("persistent copy benchmark should initialize");

                            let mut options = TaskOptions::default();
                            options.verification_policy = verification_policy;

                            let task = CopyTask::new(
                                format!("bench-copy-{case_name}-{iteration}"),
                                source_root.clone(),
                                destinations,
                                options,
                            );

                            let handle = orchestrator
                                .submit(task)
                                .await
                                .expect("benchmark task submit should succeed");
                            handle
                                .run()
                                .await
                                .expect("benchmark task run should succeed");
                            black_box(handle.progress().await.expect("progress should load"));
                        });
                    }
                    start.elapsed()
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, persistent_copy_benchmark);
criterion_main!(benches);
