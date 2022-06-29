mod console;
mod cursor;
mod file;
mod pubsubber;
use std::borrow::Cow;

pub use console::ConsoleSink;
pub use file::FileSink;

/// main trait that pushes the binlog data to an service/sink
#[async_trait::async_trait]
pub trait BinLogSink {
    async fn push(
        &mut self,
        database_name: &Cow<str>,
        table_name: &Cow<str>,
        operation: &'static str,
        value: i64,
    ) -> Result<(), Box<dyn std::error::Error>>;
}
