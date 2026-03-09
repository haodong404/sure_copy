mod common;

use std::sync::Arc;

use async_trait::async_trait;
use sqlx::Row;
use sure_copy_core::infrastructure::Sha256ChecksumProvider;
use sure_copy_core::pipeline::{
    PostWriteContext, PostWritePipeline, PostWritePipelineMode, PostWriteStage, StageArtifacts,
    TaskFlowPlan,
};
use sure_copy_core::testing::create_sqlite_persistent_task;
use sure_copy_core::{CopyError, TaskOptions, TaskState, VerificationPolicy};

use common::unique_temp_dir;

struct ArtifactEmittingPostWriteStage;

#[async_trait]
impl PostWriteStage for ArtifactEmittingPostWriteStage {
    fn id(&self) -> sure_copy_core::StageId {
        "artifact-post-write"
    }

    async fn execute(&self, _ctx: &PostWriteContext) -> Result<StageArtifacts, CopyError> {
        Ok(StageArtifacts::new().with_value("outcome", "verified"))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_write_stage_artifacts_are_persisted_per_destination() {
    let source_root = unique_temp_dir("persistent_task_post_write_artifacts_src");
    let destination_root = unique_temp_dir("persistent_task_post_write_artifacts_dst");
    std::fs::create_dir_all(&source_root).expect("source root should be creatable");
    std::fs::write(source_root.join("file.txt"), "artifact payload")
        .expect("source file should be writable");

    let mut options = TaskOptions::default();
    options.verification_policy = VerificationPolicy::None;

    let harness = create_sqlite_persistent_task(
        "post-write-artifacts-task",
        &source_root,
        vec![destination_root.clone()],
        TaskState::Created,
        options,
        TaskFlowPlan::new().with_post_write(
            PostWritePipeline::new(PostWritePipelineMode::SerialAfterWrite)
                .with_stage(Arc::new(ArtifactEmittingPostWriteStage)),
        ),
        Arc::new(Sha256ChecksumProvider::new()),
    )
    .await;

    harness.run().await.expect("task should run");

    let row = sqlx::query(
        "SELECT artifacts_json FROM task_destination_artifacts WHERE task_id = ? AND source_path = ? AND destination_path = ? AND stage_key = ?",
    )
    .bind("post-write-artifacts-task")
    .bind(source_root.join("file.txt").to_string_lossy().to_string())
    .bind(destination_root.join("file.txt").to_string_lossy().to_string())
    .bind("artifact-post-write")
    .fetch_one(harness.pool())
    .await
    .expect("destination artifacts should persist");

    let artifacts_json = row
        .try_get::<String, _>("artifacts_json")
        .expect("artifacts_json should decode");
    let artifacts =
        serde_json::from_str::<StageArtifacts>(&artifacts_json).expect("artifacts should decode");
    assert_eq!(artifacts.get_string("outcome"), Some("verified"));
}
