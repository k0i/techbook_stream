use anyhow::Result;
use chrono::NaiveDateTime;
use log::{debug, error, info};
use sqlx::types::BigDecimal;
use sqlx::Row;
use sqlx::{Connection, Executor, PgConnection};
use tokio::sync::oneshot::Sender;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub type TransactionID = Uuid;
pub type WalletID = i32;
pub type Amount = BigDecimal;

#[derive(Debug)]
pub struct TransactionEvent {
    wallet_id: WalletID,
    balance: Amount,
    last_transaction_time: NaiveDateTime,
    last_transaction_id: TransactionID,
}

pub enum StreamEventProcessResult {
    Ok,
    Stale,
    NoResponder,
}

pub async fn start_subscribe(
    cancel_token: CancellationToken,
    event_responder: tokio::sync::mpsc::Receiver<(
        WalletID,
        TransactionID,
        Amount,
        Sender<Result<Amount>>,
    )>,
) -> Result<()> {
    tokio::spawn(async move { _start_subscribe(cancel_token, event_responder).await }).await?
}

async fn _start_subscribe(
    cancel_token: CancellationToken,
    mut event_responder: tokio::sync::mpsc::Receiver<(
        WalletID,
        TransactionID,
        Amount,
        Sender<Result<Amount>>,
    )>,
) -> Result<()> {
    // TODO: replace single conn to connection pool
    let mut stream_conn = PgConnection::connect(
        "postgres://materialize@localhost:6875/materialize?options=--cluster quickstart",
    )
    .await?;
    let mut decline_conn = PgConnection::connect(
        "postgres://materialize@localhost:6875/materialize?options=--cluster quickstart",
    )
    .await?;

    let mut insert_conn =
        PgConnection::connect("postgres://materialize@localhost:6875/materialize").await?;
    // materialize automatically routes transactions to mz_instrospection cluster
    // we should disable this behavior
    stream_conn
        .execute("set auto_route_introspection_queries = false;")
        .await
        .expect("Failed to set auto_route_introspection_queries");

    // TODO: manage the case when the transaction is timed out
    stream_conn
        .execute(
            "BEGIN;DECLARE c CURSOR for SUBSCRIBE (SELECT * from wallet_balance) \
             WITH(snapshot=false)",
        )
        .await
        .expect("Failed to begin transaction");

    // TODO: we need to iterate over this hashmap and notify the client before shutting down
    let mut responder_hashmap = std::collections::HashMap::new();

    // channel that receives the stream events
    let (stream_chan_sender, mut stream_chan_receiver) = tokio::sync::mpsc::channel(100);
    let child_cancel_token = cancel_token.clone();
    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = child_cancel_token.cancelled() => {
                    break;
                }
                rows = stream_conn.fetch_all("FETCH ALL c;") => {
                    let rows = rows.expect("Failed to fetch rows");
                    for row in rows{
                    stream_chan_sender.send(row).await.unwrap();
                    }
                }
            }
        }
    });

    let mut last_transaction_processed_time = NaiveDateTime::MIN;
    let mut request_buffer = Vec::with_capacity(100);

    loop {
        tokio::select! {
            // receive the cancellation signal
            _ = cancel_token.cancelled() => {
                break;
            }
            // receive new transaction requests
            // insert the transaction into the transaction_events table
            // it triggers stream events
            _ = event_responder.recv_many(&mut request_buffer, 100) => {
                for row in request_buffer.drain(..) {
                    let (wallet_id, transaction_id, amount, responder) = row;
                    responder_hashmap.insert((wallet_id, transaction_id), responder);
                    let query = format!("INSERT INTO transaction_events values({}, '{}', {}, now(), NULL)", wallet_id, transaction_id, amount);
                    insert_conn.execute(&*query).await?;
                }
            }
            // receive the stream events: inserted new events into the transaction_events table
            row = stream_chan_receiver.recv() => {
                let row = row.expect("stream event receive failed");
                 process_event_stream(row, &mut responder_hashmap, &mut decline_conn,&mut last_transaction_processed_time).await?;

            }
        }
    }

    handle.await?;

    Ok(())
}

fn validate_transaction_event(event: &TransactionEvent) -> Result<()> {
    if event.balance < BigDecimal::from(0) {
        return Err(anyhow::anyhow!("Balance cannot be negative"));
    }
    Ok(())
}

async fn process_event_stream(
    row: sqlx::postgres::PgRow,
    responder_hashmap: &mut std::collections::HashMap<
        (WalletID, TransactionID),
        Sender<Result<Amount>>,
    >,
    decline_conn: &mut PgConnection,
    last_transaction_processed_time: &mut NaiveDateTime,
) -> Result<StreamEventProcessResult> {
    // transaction amount exceeds the wallet balance
    let declined = row.get::<Option<bool>, &str>("declined").map(|_| true);

    // stream catches the change produced by the transaction but it doesn not contain the diff
    let mz_diff = row.get::<i64, &str>("mz_diff");

    let last_transaction_id: Uuid = row.try_get("last_transaction_id")?;
    let last_transaction_time: NaiveDateTime = row.try_get("last_transaction_time")?;
    if declined.is_some()
        || mz_diff == -1
        || last_transaction_time <= *last_transaction_processed_time
    {
        debug!("Stale event: {:?}", last_transaction_id);
        return Ok(StreamEventProcessResult::Stale);
    }

    *last_transaction_processed_time = last_transaction_time;

    info!("Received Transaction Event: {:?}", last_transaction_id);

    let wallet_id: i32 = row.try_get("wallet_id")?;
    let balance: BigDecimal = row.try_get("balance")?;
    let transaction_event = TransactionEvent {
        wallet_id,
        balance,
        last_transaction_time,
        last_transaction_id,
    };

    match validate_transaction_event(&transaction_event) {
        // balance is positive
        Ok(_) => {
            match responder_hashmap.remove(&(
                transaction_event.wallet_id,
                transaction_event.last_transaction_id,
            )) {
                // there is a responder for this transaction: return response
                Some(responder) => {
                    let _ = responder.send(Ok(transaction_event.balance));
                    Ok(StreamEventProcessResult::Ok)
                }
                // there are no responders for this transaction: we need to compensate
                None => {
                    error!(
                        "No responder found:{:?}",
                        transaction_event.last_transaction_id
                    );
                    match compensate_transaction_event(decline_conn, &transaction_event).await {
                        Ok(_) => Ok(StreamEventProcessResult::NoResponder),
                        Err(e) => {
                            error!(
                                "{:?} Need Action!! Transaction Compensation Failed: {:?} {:?}",
                                transaction_event.last_transaction_time,
                                transaction_event.last_transaction_id,
                                e
                            );
                            Err(anyhow::anyhow!("Transaction Compensation Failed"))
                        }
                    }
                }
            }
        }
        Err(_) => {
            // balance is negative
            // do compensation: decline the transaction and retrieve the balance
            match compensate_transaction_event(decline_conn, &transaction_event).await {
                Ok(_) => {
                    match responder_hashmap.remove(&(
                        transaction_event.wallet_id,
                        transaction_event.last_transaction_id,
                    )) {
                        Some(responder) => {
                            let _ =
                                responder.send(Err(anyhow::anyhow!("Balance cannot be negative")));
                            Ok(StreamEventProcessResult::Ok)
                        }
                        None => {
                            error!(
                                "No responder found: {:?}",
                                transaction_event.last_transaction_id
                            );
                            Ok(StreamEventProcessResult::NoResponder)
                        }
                    }
                }
                Err(e) => {
                    error!(
                        "{:?} Need Action!! Transaction Compensation Failed: {:?} {:?}",
                        transaction_event.last_transaction_time,
                        transaction_event.last_transaction_id,
                        e
                    );
                    Err(anyhow::anyhow!("Transaction Compensation Failed"))
                }
            }
        }
    }
}

async fn compensate_transaction_event(
    conn: &mut PgConnection,
    event: &TransactionEvent,
) -> Result<()> {
    info!("Compensating transaction: {:?}", event.last_transaction_id);
    let transaction_id = event.last_transaction_id;
    let wallet_id = event.wallet_id;
    let query = format!(
        "UPDATE transaction_events SET declined = true WHERE transaction_id = '{}' AND wallet_id \
         = '{}';",
        transaction_id, wallet_id
    );
    let res = conn
        .execute(&*query)
        .await
        .expect("Failed to execute query");
    debug!("Compensation result: {:?}", res);
    Ok(())
}
