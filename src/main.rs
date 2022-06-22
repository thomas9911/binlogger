use futures::stream::StreamExt;
use mysql_async::binlog::events::EventData::{RowsEvent, TableMapEvent};
use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::binlog::BinlogVersion;
use mysql_async::prelude::*;
use mysql_async::BinlogRequest;
use mysql_async::BinlogStream;
use mysql_async::Result;
use mysql_async::Value;
use std::borrow::Cow;
use circular_queue::CircularQueue;

use std::time::{Duration, SystemTime};

const MINUTES_15: u64 = 3600 / 4;

/// wrapper for server_id
/// https://dev.mysql.com/doc/refman/8.0/en/mysqlbinlog-server-id.html 
pub enum KeepRunning{
    No = 0,
    Yes = 1
}

#[tokio::main]
async fn main() -> Result<()> {
    let database_url = "mysql://root@127.0.0.1:3306";

    let pool = mysql_async::Pool::new(database_url);
    let mut conn = pool.get_conn().await?;

    let start_from = (SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        - Duration::from_secs(MINUTES_15))
    .as_secs();

    let mut stream = conn.get_binlog_stream(BinlogRequest::new(KeepRunning::Yes as u32)).await?;
    main_loop(stream, start_from).await?;

    pool.disconnect().await?;

    Ok(())
}

fn handle_row(
    row: (Option<BinlogRow>, Option<BinlogRow>),
    table_name: &Cow<str>,
    database_name: &Cow<str>,
) {
    let row = match row {
        (Some(row), _) => row,
        (_, Some(row)) => row,
        _ => return,
    };

    // println!("{:?}", row.columns_ref().iter().map(|x| x.table_str()).collect::<Vec<_>>());
    // println!("{:?}", row.columns_ref().iter().map(|x| x.column_type()).collect::<Vec<_>>());
    if let Some(BinlogValue::Value(Value::Int(value))) = row.as_ref(0) {
        println!("{}, {} => {:?}", database_name, table_name, value);
    }
}


async fn main_loop(mut stream: BinlogStream, start_from: u64) -> Result<()> {
    let mut previous_table_id = 0;
    let mut table_events = CircularQueue::with_capacity(5);
    while let Some(Ok(event)) = stream.next().await {
        if (event.header().timestamp() as u64) < start_from {
            continue;
        }

        match event.read_data() {
            Ok(Some(TableMapEvent(ev))) => {
                previous_table_id = ev.table_id();
                table_events.push(ev.into_owned());
            }
            Ok(Some(RowsEvent(ev))) => {
                if ev.table_id() == previous_table_id && table_events.len() != 0 {
                    let current_table = table_events.iter().next().unwrap();
                    for row in ev.rows(&current_table) {
                        if let Ok(row) = row {
                            handle_row(
                                row,
                                &current_table.table_name(),
                                &current_table.database_name(),
                            );
                        }
                    }
                }
            }
            _ => (),
        }
    }

    Ok(())
}