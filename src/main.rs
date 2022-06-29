use circular_queue::CircularQueue;
use futures::stream::StreamExt;
use futures::Stream;
use mysql_async::binlog::events::{Event, EventData, RowsEventData, TableMapEvent};
use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::BinlogRequest;
use mysql_async::Value;
use std::borrow::Cow;
use std::pin::Pin;
use std::time::{Duration, SystemTime};
use tokio::signal;

mod sinks;
use pubsubber::PubSubPublisherBackend;
pub use sinks::BinLogSink;

/// Sets the behaviour of the BinLog stream
/// Normally you would just put `KeepRunnig::Yes`, but maybe for debug purposes it is handy to set it to `KeepRunnig::No`
/// so it exists after reading all the current binlog events
///
/// wrapper for server_id
/// <https://dev.mysql.com/doc/refman/8.0/en/mysqlbinlog-server-id.html>
pub enum KeepRunning {
    No = 0,
    Yes = 1,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_opts =
        mysql_async::Opts::from_url(&mysql_connection_url()).expect("invalid database url");

    let pool = mysql_async::Pool::new(database_opts);
    let conn = pool.get_conn().await?;

    let start_from = (SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("you start this before 1970 :|")
        - Duration::from_secs(INCLUDE_LAST_SECONDS))
    .as_secs();

    let stream = conn
        .get_binlog_stream(BinlogRequest::new(KeepRunning::Yes as u32))
        .await?;

    // let mut sink = sinks::ConsoleSink::default();
    // let mut sink = sinks::FileSink::new("out.txt").await?;
    let mut sink: Box<dyn PubSubPublisherBackend + Sync + Send> =
        Box::new(pubsubber::publisher(redis_connection_url()).await?);

    tokio::select! {
        _ = signal::ctrl_c() => {},
        res = main_loop(Box::pin(stream), &mut sink, start_from) => { res? },
    };

    pool.disconnect().await?;

    Ok(())
}

async fn main_loop(
    mut stream: Pin<Box<dyn Stream<Item = Result<Event, mysql_async::Error>>>>,
    sink: &mut dyn BinLogSink,
    start_from: u64,
) -> mysql_async::Result<()> {
    let mut previous_table_id = 0;
    let mut table_events = CircularQueue::with_capacity(5);
    while let Some(Ok(event)) = stream.next().await {
        if (event.header().timestamp() as u64) < start_from {
            continue;
        }

        match event.read_data() {
            Ok(Some(EventData::TableMapEvent(ev))) => {
                previous_table_id = ev.table_id();
                table_events.push(ev.into_owned());
            }
            Ok(Some(EventData::RowsEvent(ev))) => {
                if ev.table_id() == previous_table_id && table_events.len() != 0 {
                    let current_table = table_events
                        .iter()
                        .next()
                        .expect("we checked above that this exists");
                    handle_rows(sink, &ev, &current_table).await;
                }
            }
            _ => (),
        }
    }

    Ok(())
}

async fn handle_rows(
    sink: &mut dyn BinLogSink,
    event: &RowsEventData<'_>,
    current_table: &TableMapEvent<'_>,
) {
    let operation = row_to_operation(&event);

    for row in event.rows(&current_table) {
        if let Ok(row) = row {
            handle_row(
                sink,
                row,
                &current_table.table_name(),
                &current_table.database_name(),
                operation,
            )
            .await;
        }
    }
}

async fn handle_row(
    sink: &mut dyn BinLogSink,
    row: (Option<BinlogRow>, Option<BinlogRow>),
    table_name: &Cow<'_, str>,
    database_name: &Cow<'_, str>,
    operation: &'static str,
) {
    let row = match row {
        (Some(row), _) => row,
        (_, Some(row)) => row,
        _ => return,
    };
    // println!("{:?}", row.columns_ref().iter().map(|x| x.table_str()).collect::<Vec<_>>());
    // println!("{:?}", row.columns_ref().iter().map(|x| x.column_type()).collect::<Vec<_>>());

    // if the first argument is of type int then that is our id column
    // we cant check on column name because these are not stored in text form sadly
    if let Some(BinlogValue::Value(Value::Int(value))) = row.as_ref(0) {
        sink.push(database_name, table_name, operation, *value)
            .await
            .ok();
    }
}

/// convert event type to sql operation
fn row_to_operation(row_event_data: &RowsEventData) -> &'static str {
    match row_event_data {
        RowsEventData::WriteRowsEventV1(_) => "insert",
        RowsEventData::UpdateRowsEventV1(_) => "update",
        RowsEventData::DeleteRowsEventV1(_) => "delete",
        RowsEventData::WriteRowsEvent(_) => "insert",
        RowsEventData::UpdateRowsEvent(_) => "update",
        RowsEventData::DeleteRowsEvent(_) => "delete",
        RowsEventData::PartialUpdateRowsEvent(_) => "update",
    }
}

cfg_if::cfg_if! {
    if #[cfg(debug_assertions)] {
        const MINUTES_15: u64 = 3600 / 4;
        // for development also push the last 15 minutes of changes
        const INCLUDE_LAST_SECONDS: u64 = MINUTES_15;

        fn mysql_connection_url() -> String {
            std::env::var("BINLOGGER_MYSQL_URL").unwrap_or(String::from("mysql://root@127.0.0.1:3306"))
        }

        fn redis_connection_url() -> String {
            std::env::var("BINLOGGER_REDIS_URL").unwrap_or(String::from("redis://127.0.0.1"))
        }
    } else {
        // for production only push new changes (maybe in the future tweak this to compensate for restarting)
        const INCLUDE_LAST_SECONDS: u64 = 0;

        fn mysql_connection_url() -> String {
            std::env::var("BINLOGGER_MYSQL_URL").expect("mysql url not set")
        }

        fn redis_connection_url() -> String {
            std::env::var("BINLOGGER_REDIS_URL").expect("redis url not set")
        }
    }
}

#[tokio::test]
async fn handle_row_test() {
    use std::io::Cursor;
    use std::sync::Arc;

    let mut cursor = Cursor::new(Vec::new());
    let columns = Arc::new([mysql_async::Column::new(
        mysql_async::consts::ColumnType::MYSQL_TYPE_SHORT,
    )]);

    let row = BinlogRow::new(vec![Some(BinlogValue::Value(Value::Int(1)))], columns);
    handle_row(
        &mut cursor,
        (Some(row), None),
        &"table".into(),
        &"database".into(),
        "insert",
    )
    .await;

    let result = String::from_utf8(cursor.into_inner()).unwrap();

    assert_eq!(result, "database::table|insert::1")
}

#[tokio::test]
async fn main_loop_test() {
    use mysql_async::binlog::events::FormatDescriptionEvent;
    use mysql_async::binlog::BinlogVersion::Version4;
    use std::io::Cursor;

    let table_event_data = vec![
        220, 72, 188, 98, 19, 1, 0, 0, 0, 59, 0, 0, 0, 243, 95, 84, 2, 0, 0, 129, 4, 0, 0, 0, 0, 1,
        0, 13, 98, 101, 116, 116, 121, 95, 113, 108, 95, 116, 101, 115, 116, 0, 4, 116, 97, 103,
        115, 0, 2, 8, 15, 2, 252, 3, 2, 233, 55, 165, 90,
    ];
    let row_event_data = vec![
        53, 71, 188, 98, 23, 1, 0, 0, 0, 49, 0, 0, 0, 33, 95, 84, 2, 0, 0, 129, 4, 0, 0, 0, 0, 1,
        0, 2, 255, 252, 20, 0, 0, 0, 0, 0, 0, 0, 5, 0, 104, 97, 108, 108, 111, 181, 139, 181, 50,
    ];

    let table_event =
        Event::read(&FormatDescriptionEvent::new(Version4), &*table_event_data).unwrap();
    let row_event = Event::read(&FormatDescriptionEvent::new(Version4), &*row_event_data).unwrap();

    let stream = futures::stream::iter([Ok(table_event), Ok(row_event)]);
    let mut cursor = Cursor::new(Vec::new());

    main_loop(Box::pin(stream), &mut cursor, 0).await.unwrap();

    let result = String::from_utf8(cursor.into_inner()).unwrap();

    assert_eq!(result, "betty_ql_test::tags|insert::20")
}
