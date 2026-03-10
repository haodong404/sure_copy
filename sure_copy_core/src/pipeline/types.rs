use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// Stable identifier for a runtime stage.
pub type StageId = &'static str;

/// Durable description of one stage instance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct StageSpec {
    pub kind: String,
    #[serde(default)]
    pub config: JsonValue,
}

impl StageSpec {
    pub fn new(kind: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            config: JsonValue::Null,
        }
    }

    pub fn with_config(mut self, config: JsonValue) -> Self {
        self.config = config;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SourceObserverPipelineSpec {
    #[serde(default)]
    pub mode: SourcePipelineMode,
    #[serde(default)]
    pub stages: Vec<StageSpec>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct PostWritePipelineSpec {
    #[serde(default)]
    pub mode: PostWritePipelineMode,
    #[serde(default)]
    pub stages: Vec<StageSpec>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct TaskFlowSpec {
    #[serde(default)]
    pub source_observer: Option<SourceObserverPipelineSpec>,
    #[serde(default)]
    pub post_write: Option<PostWritePipelineSpec>,
}

impl TaskFlowSpec {
    pub fn is_empty(&self) -> bool {
        self.source_observer
            .as_ref()
            .is_none_or(|pipeline| pipeline.stages.is_empty())
            && self
                .post_write
                .as_ref()
                .is_none_or(|pipeline| pipeline.stages.is_empty())
    }
}

/// Defines how source-side observer stages run relative to destination writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourcePipelineMode {
    /// Run the source observer pipeline before starting destination writes.
    SerialBeforeFanOut,
    /// Fan out each source chunk to destination writers and source observers together.
    ConcurrentWithFanOut,
}

impl Default for SourcePipelineMode {
    fn default() -> Self {
        Self::ConcurrentWithFanOut
    }
}

/// Defines how per-destination post-write stages run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PostWritePipelineMode {
    /// Run all post-write stages serially after a destination write completes.
    SerialAfterWrite,
}

impl Default for PostWritePipelineMode {
    fn default() -> Self {
        Self::SerialAfterWrite
    }
}

/// One immutable source chunk distributed to pipeline branches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceChunk {
    pub offset: u64,
    pub bytes: Arc<[u8]>,
}

impl SourceChunk {
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

/// Small structured outputs emitted by observer or post-write stages.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value")]
pub enum ArtifactValue {
    String(String),
    Integer(i64),
    Boolean(bool),
    Json(JsonValue),
}

impl ArtifactValue {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value.as_str()),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Integer(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Boolean(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_json(&self) -> Option<&JsonValue> {
        match self {
            Self::Json(value) => Some(value),
            _ => None,
        }
    }
}

impl From<String> for ArtifactValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for ArtifactValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<i64> for ArtifactValue {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<bool> for ArtifactValue {
    fn from(value: bool) -> Self {
        Self::Boolean(value)
    }
}

impl From<JsonValue> for ArtifactValue {
    fn from(value: JsonValue) -> Self {
        Self::Json(value)
    }
}

/// Durable snapshot of one stage's resumable internal state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct StageStateSpec {
    pub kind: String,
    #[serde(default)]
    pub state: JsonValue,
}

impl StageStateSpec {
    pub fn new(kind: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            state: JsonValue::Null,
        }
    }

    pub fn with_state(mut self, state: JsonValue) -> Self {
        self.state = state;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct StageArtifacts {
    pub values: BTreeMap<String, ArtifactValue>,
}

impl StageArtifacts {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_value(mut self, key: impl Into<String>, value: impl Into<ArtifactValue>) -> Self {
        self.values.insert(key.into(), value.into());
        self
    }

    pub fn get(&self, key: &str) -> Option<&ArtifactValue> {
        self.values.get(key)
    }

    pub fn get_string(&self, key: &str) -> Option<&str> {
        self.get(key)?.as_str()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

/// Aggregated stage outputs, namespaced by stage id.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct PipelineArtifacts {
    stage_outputs: BTreeMap<String, StageArtifacts>,
}

impl PipelineArtifacts {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert_stage_output(&mut self, stage_key: impl Into<String>, artifacts: StageArtifacts) {
        if artifacts.is_empty() {
            return;
        }
        self.stage_outputs.insert(stage_key.into(), artifacts);
    }

    pub fn get(&self, stage_id: &str, key: &str) -> Option<&ArtifactValue> {
        self.stage_outputs.get(stage_id)?.get(key)
    }

    pub fn get_string(&self, stage_id: &str, key: &str) -> Option<&str> {
        self.get(stage_id, key)?.as_str()
    }

    pub fn stage(&self, stage_id: &str) -> Option<&StageArtifacts> {
        self.stage_outputs.get(stage_id)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &StageArtifacts)> {
        self.stage_outputs
            .iter()
            .map(|(stage_id, artifacts)| (stage_id.as_str(), artifacts))
    }

    pub fn is_empty(&self) -> bool {
        self.stage_outputs.is_empty()
    }
}

/// Input for a single post-write stage execution.
#[derive(Debug, Clone, PartialEq)]
pub struct PostWriteContext {
    pub task_id: String,
    pub source_path: PathBuf,
    pub requested_destination_path: PathBuf,
    pub actual_destination_path: PathBuf,
    pub bytes_written: u64,
    pub expected_bytes: u64,
    pub pipeline_artifacts: PipelineArtifacts,
}

impl PostWriteContext {
    pub fn artifact(&self, stage_id: &str, key: &str) -> Option<&ArtifactValue> {
        self.pipeline_artifacts.get(stage_id, key)
    }
}
