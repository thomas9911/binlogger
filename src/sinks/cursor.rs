use super::BinLogSink;
use std::borrow::Cow;
use std::io::Write;

#[async_trait::async_trait]
impl BinLogSink for std::io::Cursor<Vec<u8>> {
    async fn push(
        &mut self,
        database_name: &Cow<str>,
        table_name: &Cow<str>,
        operation: &'static str,
        value: i64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let message = format!("{}::{}|{}::{}", database_name, table_name, operation, value);
        self.write_all(message.as_bytes())?;
        Ok(())
    }
}
