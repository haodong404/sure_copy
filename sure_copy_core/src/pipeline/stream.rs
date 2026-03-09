use async_trait::async_trait;
use std::collections::VecDeque;

use super::types::StageItem;

/// Async stream contract used by pipeline stages.
#[async_trait]
pub trait StageStream: Send {
    async fn next(&mut self) -> Option<StageItem>;
}

/// Boxed stage stream trait object.
pub type BoxStageStream = Box<dyn StageStream>;

/// In-memory stream implementation useful for tests and skeleton wiring.
pub struct InMemoryStageStream {
    items: VecDeque<StageItem>,
}

impl InMemoryStageStream {
    pub fn new(items: Vec<StageItem>) -> Self {
        Self {
            items: items.into(),
        }
    }
}

#[async_trait]
impl StageStream for InMemoryStageStream {
    async fn next(&mut self) -> Option<StageItem> {
        self.items.pop_front()
    }
}
