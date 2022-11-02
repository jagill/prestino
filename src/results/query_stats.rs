use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QueryStats {
    pub state: String,
    pub queued: bool,
    pub scheduled: bool,
    pub nodes: u32,
    pub total_splits: u32,
    pub queued_splits: u32,
    pub running_splits: u32,
    pub completed_splits: u32,
    pub cpu_time_millis: u32,
    pub wall_time_millis: u32,
    pub queued_time_millis: u32,
    pub elapsed_time_millis: u32,
    pub processed_rows: u32,
    pub processed_bytes: u32,
    pub physical_input_bytes: u32,
    pub peak_memory_bytes: u32,
    pub spilled_bytes: u32,
}
