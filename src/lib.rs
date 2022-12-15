mod client_connection;
mod prestino_client;
mod prestino_error;
pub mod results;
mod statement_executor;

pub use prestino_client::PrestinoClient;
pub use prestino_error::PrestinoError;
pub use statement_executor::StatementExecutor;

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum Fork {
    Presto,
    Trino,
}

impl Fork {
    pub fn prefix(&self) -> &'static str {
        match self {
            Fork::Presto => "x-presto",
            Fork::Trino => "x-trino",
        }
    }

    pub fn name_for(&self, name: &str) -> String {
        format!("{}-{}", self.prefix(), name)
    }
}

#[cfg(test)]
mod tests;
