use super::BinLogSink;
use std::borrow::Cow;

/// Binlog sink that writes to the console directly
/// mostly for debugging purposes
#[derive(Debug, Default)]
pub struct ConsoleSink;

#[async_trait::async_trait]
impl BinLogSink for ConsoleSink {
    async fn push(
        &mut self,
        database_name: &Cow<str>,
        table_name: &Cow<str>,
        operation: &'static str,
        value: i64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        println!(
            "{}, {} => {} {:?}",
            database_name.as_ref(),
            table_name.as_ref(),
            operation,
            value
        );
        Ok(())
    }
}
