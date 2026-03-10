use std::path::{Path, PathBuf};

use async_trait::async_trait;
use sqlx::{Row, SqlitePool};

use crate::domain::{CopyError, CopyErrorCategory};
use crate::pipeline::{PipelineArtifacts, StageArtifacts};

use super::artifact_store::{ArtifactStore, SourceFingerprint, StoredSourceArtifacts};

pub(crate) const TASK_SOURCE_ARTIFACTS_SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS task_source_artifacts (
    task_id TEXT NOT NULL,
    source_path TEXT NOT NULL,
    stage_key TEXT NOT NULL,
    artifacts_json TEXT NOT NULL,
    source_fingerprint_json TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (task_id, source_path, stage_key)
);
"#;

pub(crate) const TASK_DESTINATION_ARTIFACTS_SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS task_destination_artifacts (
    task_id TEXT NOT NULL,
    source_path TEXT NOT NULL,
    destination_path TEXT NOT NULL,
    stage_key TEXT NOT NULL,
    artifacts_json TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (task_id, source_path, destination_path, stage_key)
);
"#;

pub(crate) const TASK_STAGE_STATES_SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS task_stage_states (
    task_id TEXT NOT NULL,
    source_path TEXT NOT NULL,
    destination_path TEXT NOT NULL DEFAULT '',
    stage_key TEXT NOT NULL,
    state_json TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (task_id, source_path, destination_path, stage_key)
);
"#;

fn db_error(context: &'static str, err: sqlx::Error) -> CopyError {
    CopyError {
        category: CopyErrorCategory::Io,
        code: "DB_ERROR",
        message: format!("database error in {}: {}", context, err),
    }
}

fn serde_error(context: &'static str, err: serde_json::Error) -> CopyError {
    CopyError {
        category: CopyErrorCategory::Io,
        code: "SERDE_ERROR",
        message: format!("serialization error in {}: {}", context, err),
    }
}

fn now_unix_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub(crate) struct SqliteArtifactStore {
    pool: SqlitePool,
}

impl SqliteArtifactStore {
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ArtifactStore for SqliteArtifactStore {
    async fn load_source_artifacts(
        &self,
        task_id: &str,
        source: &Path,
    ) -> Result<Option<StoredSourceArtifacts>, CopyError> {
        let source_path = source.to_string_lossy().to_string();
        let rows = sqlx::query(
            "SELECT stage_key, artifacts_json, source_fingerprint_json FROM task_source_artifacts WHERE task_id = ? AND source_path = ? ORDER BY stage_key",
        )
        .bind(task_id)
        .bind(&source_path)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| db_error("SqliteArtifactStore::load_source_artifacts", err))?;

        if rows.is_empty() {
            return Ok(None);
        }

        let mut artifacts = PipelineArtifacts::new();
        let mut fingerprint = None;
        for row in rows {
            let stage_key = row
                .try_get::<String, _>("stage_key")
                .map_err(|err| db_error("SqliteArtifactStore::load_source_stage_key", err))?;
            let artifacts_json = row
                .try_get::<String, _>("artifacts_json")
                .map_err(|err| db_error("SqliteArtifactStore::load_source_artifacts_json", err))?;
            let fingerprint_json = row
                .try_get::<String, _>("source_fingerprint_json")
                .map_err(|err| {
                    db_error("SqliteArtifactStore::load_source_fingerprint_json", err)
                })?;

            let stage_artifacts = serde_json::from_str::<StageArtifacts>(&artifacts_json)
                .map_err(|err| serde_error("SqliteArtifactStore::decode_source_artifacts", err))?;
            let current_fingerprint = serde_json::from_str::<SourceFingerprint>(&fingerprint_json)
                .map_err(|err| {
                    serde_error("SqliteArtifactStore::decode_source_fingerprint", err)
                })?;

            if let Some(existing) = fingerprint.as_ref() {
                if existing != &current_fingerprint {
                    return Err(CopyError {
                        category: CopyErrorCategory::Io,
                        code: "ARTIFACT_FINGERPRINT_MISMATCH",
                        message: format!(
                            "inconsistent source artifact fingerprints for task '{}' and source '{}'",
                            task_id, source_path
                        ),
                    });
                }
            } else {
                fingerprint = Some(current_fingerprint);
            }

            artifacts.insert_stage_output(stage_key, stage_artifacts);
        }

        Ok(Some(StoredSourceArtifacts {
            source_path: PathBuf::from(source),
            fingerprint: fingerprint.expect("non-empty rows should yield a fingerprint"),
            artifacts,
        }))
    }

    async fn save_source_artifacts(
        &self,
        task_id: &str,
        source: &Path,
        fingerprint: &SourceFingerprint,
        artifacts: &PipelineArtifacts,
    ) -> Result<(), CopyError> {
        self.delete_source_artifacts(task_id, source).await?;
        if artifacts.is_empty() {
            return Ok(());
        }

        let source_path = source.to_string_lossy().to_string();
        let fingerprint_json = serde_json::to_string(fingerprint)
            .map_err(|err| serde_error("SqliteArtifactStore::encode_source_fingerprint", err))?;
        let updated_at_ms = now_unix_ms();
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|err| db_error("SqliteArtifactStore::save_source_artifacts_begin", err))?;

        for (stage_key, stage_artifacts) in artifacts.iter() {
            let artifacts_json = serde_json::to_string(stage_artifacts)
                .map_err(|err| serde_error("SqliteArtifactStore::encode_source_artifacts", err))?;
            sqlx::query(
                "INSERT OR REPLACE INTO task_source_artifacts (task_id, source_path, stage_key, artifacts_json, source_fingerprint_json, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(task_id)
            .bind(&source_path)
            .bind(stage_key)
            .bind(artifacts_json)
            .bind(&fingerprint_json)
            .bind(updated_at_ms)
            .execute(&mut *tx)
            .await
            .map_err(|err| db_error("SqliteArtifactStore::save_source_artifacts_insert", err))?;
        }

        tx.commit()
            .await
            .map_err(|err| db_error("SqliteArtifactStore::save_source_artifacts_commit", err))?;
        Ok(())
    }

    async fn delete_source_artifacts(&self, task_id: &str, source: &Path) -> Result<(), CopyError> {
        sqlx::query("DELETE FROM task_source_artifacts WHERE task_id = ? AND source_path = ?")
            .bind(task_id)
            .bind(source.to_string_lossy().to_string())
            .execute(&self.pool)
            .await
            .map_err(|err| db_error("SqliteArtifactStore::delete_source_artifacts", err))?;
        Ok(())
    }

    async fn save_destination_artifacts(
        &self,
        task_id: &str,
        source: &Path,
        destination: &Path,
        artifacts: &PipelineArtifacts,
    ) -> Result<(), CopyError> {
        self.delete_destination_artifacts(task_id, source, destination)
            .await?;
        if artifacts.is_empty() {
            return Ok(());
        }

        let source_path = source.to_string_lossy().to_string();
        let destination_path = destination.to_string_lossy().to_string();
        let updated_at_ms = now_unix_ms();
        let mut tx = self.pool.begin().await.map_err(|err| {
            db_error("SqliteArtifactStore::save_destination_artifacts_begin", err)
        })?;

        for (stage_key, stage_artifacts) in artifacts.iter() {
            let artifacts_json = serde_json::to_string(stage_artifacts).map_err(|err| {
                serde_error("SqliteArtifactStore::encode_destination_artifacts", err)
            })?;
            sqlx::query(
                "INSERT OR REPLACE INTO task_destination_artifacts (task_id, source_path, destination_path, stage_key, artifacts_json, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(task_id)
            .bind(&source_path)
            .bind(&destination_path)
            .bind(stage_key)
            .bind(artifacts_json)
            .bind(updated_at_ms)
            .execute(&mut *tx)
            .await
            .map_err(|err| {
                db_error("SqliteArtifactStore::save_destination_artifacts_insert", err)
            })?;
        }

        tx.commit().await.map_err(|err| {
            db_error(
                "SqliteArtifactStore::save_destination_artifacts_commit",
                err,
            )
        })?;
        Ok(())
    }

    async fn delete_destination_artifacts(
        &self,
        task_id: &str,
        source: &Path,
        destination: &Path,
    ) -> Result<(), CopyError> {
        sqlx::query(
            "DELETE FROM task_destination_artifacts WHERE task_id = ? AND source_path = ? AND destination_path = ?",
        )
        .bind(task_id)
        .bind(source.to_string_lossy().to_string())
        .bind(destination.to_string_lossy().to_string())
        .execute(&self.pool)
        .await
        .map_err(|err| db_error("SqliteArtifactStore::delete_destination_artifacts", err))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use sqlx::SqlitePool;

    use crate::orchestrator::artifact_store::SourceFingerprint;

    use super::*;

    async fn setup_store() -> SqliteArtifactStore {
        let pool = SqlitePool::connect("sqlite::memory:")
            .await
            .expect("sqlite in-memory database should connect");
        sqlx::query(TASK_SOURCE_ARTIFACTS_SCHEMA_SQL)
            .execute(&pool)
            .await
            .expect("source artifacts schema should be created");
        sqlx::query(TASK_DESTINATION_ARTIFACTS_SCHEMA_SQL)
            .execute(&pool)
            .await
            .expect("destination artifacts schema should be created");
        sqlx::query(TASK_STAGE_STATES_SCHEMA_SQL)
            .execute(&pool)
            .await
            .expect("stage states schema should be created");

        SqliteArtifactStore::new(pool)
    }

    fn sample_fingerprint() -> SourceFingerprint {
        SourceFingerprint {
            size_bytes: 123,
            modified_unix_ms: Some(42),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn source_artifacts_roundtrip_and_delete_work() {
        let store = setup_store().await;
        let source = Path::new("/src/file.txt");

        let missing = store
            .load_source_artifacts("task-1", source)
            .await
            .expect("load should succeed");
        assert!(missing.is_none());

        let mut artifacts = PipelineArtifacts::new();
        artifacts.insert_stage_output("stage-a", StageArtifacts::new().with_value("k", "v"));
        artifacts.insert_stage_output("stage-b", StageArtifacts::new().with_value("n", 7_i64));

        let fingerprint = sample_fingerprint();
        store
            .save_source_artifacts("task-1", source, &fingerprint, &artifacts)
            .await
            .expect("save should succeed");

        let loaded = store
            .load_source_artifacts("task-1", source)
            .await
            .expect("load should succeed")
            .expect("artifacts should exist");
        assert_eq!(loaded.fingerprint, fingerprint);
        assert_eq!(loaded.source_path, source);
        assert_eq!(loaded.artifacts.get_string("stage-a", "k"), Some("v"));
        assert_eq!(
            loaded
                .artifacts
                .get("stage-b", "n")
                .and_then(|v| v.as_i64()),
            Some(7)
        );

        store
            .delete_source_artifacts("task-1", source)
            .await
            .expect("delete should succeed");
        assert!(store
            .load_source_artifacts("task-1", source)
            .await
            .expect("load after delete should succeed")
            .is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn saving_empty_artifacts_clears_existing_source_and_destination_rows() {
        let store = setup_store().await;
        let source = Path::new("/src/file.txt");
        let destination = Path::new("/dst/file.txt");

        let mut artifacts = PipelineArtifacts::new();
        artifacts.insert_stage_output("stage-a", StageArtifacts::new().with_value("k", "v"));

        store
            .save_source_artifacts("task-1", source, &sample_fingerprint(), &artifacts)
            .await
            .expect("save source should succeed");
        store
            .save_destination_artifacts("task-1", source, destination, &artifacts)
            .await
            .expect("save destination should succeed");

        store
            .save_source_artifacts(
                "task-1",
                source,
                &sample_fingerprint(),
                &PipelineArtifacts::new(),
            )
            .await
            .expect("saving empty source artifacts should succeed");
        store
            .save_destination_artifacts("task-1", source, destination, &PipelineArtifacts::new())
            .await
            .expect("saving empty destination artifacts should succeed");

        let source_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM task_source_artifacts WHERE task_id = ? AND source_path = ?",
        )
        .bind("task-1")
        .bind(source.to_string_lossy().to_string())
        .fetch_one(&store.pool)
        .await
        .expect("source count query should succeed");
        assert_eq!(source_count, 0);

        let destination_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM task_destination_artifacts WHERE task_id = ? AND source_path = ? AND destination_path = ?",
        )
        .bind("task-1")
        .bind(source.to_string_lossy().to_string())
        .bind(destination.to_string_lossy().to_string())
        .fetch_one(&store.pool)
        .await
        .expect("destination count query should succeed");
        assert_eq!(destination_count, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn load_source_artifacts_rejects_fingerprint_mismatch() {
        let store = setup_store().await;
        let source = Path::new("/src/file.txt");
        let source_path = source.to_string_lossy().to_string();

        let fp_a = serde_json::to_string(&SourceFingerprint {
            size_bytes: 10,
            modified_unix_ms: Some(1),
        })
        .expect("fingerprint json should encode");
        let fp_b = serde_json::to_string(&SourceFingerprint {
            size_bytes: 20,
            modified_unix_ms: Some(2),
        })
        .expect("fingerprint json should encode");

        let artifact_json = serde_json::to_string(&StageArtifacts::new().with_value("k", "v"))
            .expect("artifact json should encode");

        for (stage_key, fingerprint_json) in [("stage-a", fp_a), ("stage-b", fp_b)] {
            sqlx::query(
                "INSERT INTO task_source_artifacts (task_id, source_path, stage_key, artifacts_json, source_fingerprint_json, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind("task-1")
            .bind(&source_path)
            .bind(stage_key)
            .bind(&artifact_json)
            .bind(fingerprint_json)
            .bind(0_i64)
            .execute(&store.pool)
            .await
            .expect("seed row should insert");
        }

        let err = store
            .load_source_artifacts("task-1", source)
            .await
            .expect_err("mismatched fingerprint rows must fail");
        assert_eq!(err.code, "ARTIFACT_FINGERPRINT_MISMATCH");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn load_source_artifacts_rejects_invalid_artifact_json() {
        let store = setup_store().await;
        let source = Path::new("/src/file.txt");

        sqlx::query(
            "INSERT INTO task_source_artifacts (task_id, source_path, stage_key, artifacts_json, source_fingerprint_json, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind("task-1")
        .bind(source.to_string_lossy().to_string())
        .bind("stage-a")
        .bind("{not-json}")
        .bind(serde_json::to_string(&sample_fingerprint()).expect("fingerprint json should encode"))
        .bind(0_i64)
        .execute(&store.pool)
        .await
        .expect("seed row should insert");

        let err = store
            .load_source_artifacts("task-1", source)
            .await
            .expect_err("invalid artifact json must fail");
        assert_eq!(err.code, "SERDE_ERROR");
        assert!(err.message.contains("decode_source_artifacts"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn load_source_artifacts_rejects_invalid_fingerprint_json() {
        let store = setup_store().await;
        let source = Path::new("/src/file.txt");

        sqlx::query(
            "INSERT INTO task_source_artifacts (task_id, source_path, stage_key, artifacts_json, source_fingerprint_json, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind("task-1")
        .bind(source.to_string_lossy().to_string())
        .bind("stage-a")
        .bind(serde_json::to_string(&StageArtifacts::new().with_value("k", "v")).expect("artifact json should encode"))
        .bind("{not-json}")
        .bind(0_i64)
        .execute(&store.pool)
        .await
        .expect("seed row should insert");

        let err = store
            .load_source_artifacts("task-1", source)
            .await
            .expect_err("invalid fingerprint json must fail");
        assert_eq!(err.code, "SERDE_ERROR");
        assert!(err.message.contains("decode_source_fingerprint"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn save_source_artifacts_replaces_existing_stage_rows() {
        let store = setup_store().await;
        let source = Path::new("/src/file.txt");

        let mut first = PipelineArtifacts::new();
        first.insert_stage_output("stage-a", StageArtifacts::new().with_value("value", 1_i64));
        first.insert_stage_output("stage-b", StageArtifacts::new().with_value("value", 2_i64));
        store
            .save_source_artifacts("task-1", source, &sample_fingerprint(), &first)
            .await
            .expect("initial save should succeed");

        let mut second = PipelineArtifacts::new();
        second.insert_stage_output("stage-c", StageArtifacts::new().with_value("value", 3_i64));
        store
            .save_source_artifacts("task-1", source, &sample_fingerprint(), &second)
            .await
            .expect("replacement save should succeed");

        let loaded = store
            .load_source_artifacts("task-1", source)
            .await
            .expect("load should succeed")
            .expect("artifacts should exist");
        assert_eq!(loaded.artifacts.get("stage-a", "value"), None);
        assert_eq!(loaded.artifacts.get("stage-b", "value"), None);
        assert_eq!(
            loaded
                .artifacts
                .get("stage-c", "value")
                .and_then(|value| value.as_i64()),
            Some(3)
        );
    }
}
