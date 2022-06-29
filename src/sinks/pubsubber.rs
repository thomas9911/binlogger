/// implement BinLongSink for all structs that implement `pubsubber::PubSubPublisherBackend `
///
use super::BinLogSink;
use std::borrow::Cow;

#[async_trait::async_trait]
impl BinLogSink for Box<dyn pubsubber::PubSubPublisherBackend + Sync + Send> {
    async fn push(
        &mut self,
        database_name: &Cow<str>,
        table_name: &Cow<str>,
        operation: &'static str,
        value: i64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let topic = format!("{}::{}", database_name, table_name);
        let message = format!("{}::{}", operation, value);
        self.publish(&topic, message)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}
