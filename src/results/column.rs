use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Column {
    name: String,
    #[serde(rename = "type")]
    type_name: String,
}

impl Column {
    pub fn name(&self) -> &str {
        &self.name
    }
}
