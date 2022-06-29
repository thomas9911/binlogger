use super::BinLogSink;
use std::borrow::Cow;
use std::path::Path;
use tokio::fs::File;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

/// Binlog sink that writes to a file in csv form
/// mostly for debugging purposes
#[derive(Debug)]
pub struct FileSink {
    file: File,
}

#[async_trait::async_trait]
impl BinLogSink for FileSink {
    async fn push(
        &mut self,
        database_name: &Cow<str>,
        table_name: &Cow<str>,
        operation: &'static str,
        value: i64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let line = format!("{},{},{},{}\n", database_name, table_name, operation, value);
        self.file.write_all(line.as_bytes()).await?;
        Ok(())
    }
}

impl FileSink {
    #[allow(dead_code)]
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<FileSink, Box<dyn std::error::Error>> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .await?;

        Ok(FileSink { file })
    }
}
