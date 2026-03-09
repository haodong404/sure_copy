mod support;

use std::fs;
use std::time::{Duration, Instant};

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use sure_copy_core::{OrchestratorConfig, SqliteTaskOrchestrator};

use support::{build_runtime, join_path, seed_recovery_database, temp_dir};

fn sqlite_recovery_benchmark(c: &mut Criterion) {
    let runtime = build_runtime();
    let template_root = temp_dir("sqlite_recovery_template");
    let mut group = c.benchmark_group("sqlite_startup_recovery");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));

    for (task_count, checkpoints_per_task) in [(100_usize, 10_usize), (1_000, 10)] {
        let template_db = join_path(
            template_root.path(),
            &format!("recovery-{task_count}-{checkpoints_per_task}.db"),
        );
        runtime.block_on(seed_recovery_database(
            &template_db,
            task_count,
            checkpoints_per_task,
        ));

        let total_checkpoints = (task_count * checkpoints_per_task) as u64;
        group.throughput(Throughput::Elements(total_checkpoints));
        group.bench_with_input(
            BenchmarkId::new("checkpoints", total_checkpoints),
            &template_db,
            |b, template_db| {
                b.iter_custom(|iters| {
                    let start = Instant::now();
                    for _ in 0..iters {
                        let run_root = temp_dir("sqlite_recovery_run");
                        let run_db = join_path(run_root.path(), "recovery.db");
                        fs::copy(template_db, &run_db)
                            .expect("benchmark template db should be copied");

                        let orchestrator = runtime.block_on(async {
                            SqliteTaskOrchestrator::new(&run_db, OrchestratorConfig::default())
                                .await
                                .expect("SQLite recovery benchmark should initialize")
                        });
                        black_box(orchestrator);
                    }
                    start.elapsed()
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, sqlite_recovery_benchmark);
criterion_main!(benches);
