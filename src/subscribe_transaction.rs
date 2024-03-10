#![allow(dead_code)]

use anyhow::Result;
use chrono::NaiveDateTime;
use log::{debug, error, info, warn};
use sqlx::postgres::PgPoolOptions;
use sqlx::types::BigDecimal;
use sqlx::{Connection, Executor, PgConnection, Postgres};
use sqlx::{Pool, Row};
use tokio::sync::oneshot::Sender;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

pub type TransactionID = i32;
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
    Ok(WalletID, TransactionID, Amount),
    Stale(WalletID, TransactionID, Amount),
    NoResponder(TransactionID),
    InSufficientBalance(WalletID, TransactionID),
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
    let mut stream_conn = PgConnection::connect(
        "postgres://materialize@localhost:6875/materialize?options=--cluster quickstart",
    )
    .await?;
    let decline_pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://materialize@localhost:6875/materialize?options=--cluster quickstart")
        .await?;

    let insert_pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://materialize@localhost:6875/materialize")
        .await?;

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
                    stream_chan_sender.send(rows).await.unwrap();
                }
            }
        }
    });

    let mut last_transaction_processed_time = NaiveDateTime::MIN;
    let mut request_buffer = Vec::with_capacity(100);
    let mut stream_chan_buffer = Vec::with_capacity(100);

    info!("Subscribing to transaction events");
    let mut req_cnt = 0;

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
                let mut handles = Vec::new();
                req_cnt +=request_buffer.len();
                for row in request_buffer.drain(..) {
                    let (wallet_id, transaction_id, amount, responder) = row;
                    responder_hashmap.insert((wallet_id, transaction_id), responder);
                    let insert_pool = insert_pool.clone();
                    let query = format!("INSERT INTO transaction_events values({}, '{}', {}, now(), NULL)", wallet_id, transaction_id, amount);
                    let insert_handle = tokio::spawn(async move {
                        insert_pool.execute(&*query).await?;
                        Ok::<_, anyhow::Error>(())
                   });
                    handles.push(insert_handle);
                }
                let req_len = handles.len();
                futures::future::join_all(handles).await;
                warn!("Transaction requests processed: {:?}", req_len);
                warn!("Total requests processed: {:?}", req_cnt);
            }
            // receive the stream events: inserted new events into the transaction_events table
            _ = stream_chan_receiver.recv_many(&mut stream_chan_buffer, 100) => {
                info!("Processing stream events");
                println!("hashmap dbg: {:?}", responder_hashmap);
                let mut compensate_handles = JoinSet::new();
                for changed_rows in stream_chan_buffer.drain(..) {
                    let processed_transactions = changed_rows.iter().map(|row| process_event_stream(row, &mut last_transaction_processed_time));

                    processed_transactions.for_each(|result| {
                        match result {
                            // success
                            Ok(StreamEventProcessResult::Ok(wallet_id,transaction_id, balance)) => {
                                if let Some(responder) = responder_hashmap.remove(&(wallet_id, transaction_id)) {
                                    info!("Transaction Processed: {:?}", transaction_id);
                                    let _ = responder.send(Ok(balance));
                                }else {
                                    info!("No responder found: {:?}", transaction_id);
                                }
                            }
                            Ok(StreamEventProcessResult::Stale(wallet_id, transaction_id, balance)) => {
                                if let Some(responder) = responder_hashmap.remove(&(wallet_id, transaction_id)) {
                                    info!("Stale Transaction: {:?}", transaction_id);
                                    let _ = responder.send(Ok(balance));
                                }else {
                                    info!("No responder found: {:?}", transaction_id);
                                }
                            }
                            // insufficient balance: need compensation
                            Ok(StreamEventProcessResult::InSufficientBalance(wallet_id, transaction_id)) => {
                                let mut decline_pool = decline_pool.clone();
                                let _ = compensate_handles.spawn(async move {
                                    compensate_transaction_event(&mut decline_pool, wallet_id, transaction_id).await
                                });
                            }
                            // process failed: critical error
                            Err(e) => {
                                error!("Need Action!! Transaction Processing Failed: {:?}", e);
                            }
                            // if there are no responders, ignore
                            Ok(StreamEventProcessResult::NoResponder(_)) => {}
                        }
                    });
                }

                if !compensate_handles.is_empty() {
                    info!("Compensating transactions");
                    while let Some(handle) = compensate_handles.join_next().await {
                        let handle = handle?;
                        match handle {
                            Ok((wallet_id, transaction_id)) => {
                                match responder_hashmap.remove(&(wallet_id, transaction_id)) {
                                    Some(responder) => {
                                        let _ = responder.send(Err(anyhow::anyhow!("Balance cannot be negative")));
                                    }
                                    None => {
                                        info!("No responder found: {:?}", transaction_id);
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Need Action!! Transaction Compensation Failed: {:?}", e);
                            }
                        }

                    }
                }
                info!("Stream events processed");
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

fn process_event_stream(
    row: &sqlx::postgres::PgRow,
    last_transaction_processed_time: &mut NaiveDateTime,
) -> Result<StreamEventProcessResult> {
    // transaction amount exceeds the wallet balance
    let declined = row.get::<Option<bool>, &str>("declined").map(|_| true);

    // stream catches the change produced by the transaction but it doesn not contain the diff
    let mz_diff = row.get::<i64, &str>("mz_diff");

    let wallet_id: i32 = row.try_get("wallet_id")?;
    let last_transaction_id: i32 = row.try_get("last_transaction_id")?;
    let last_transaction_time: NaiveDateTime = row.try_get("last_transaction_time")?;
    let balance: BigDecimal = row.try_get("balance")?;

    if declined.is_some()
        || mz_diff == -1
        || last_transaction_time <= *last_transaction_processed_time
    {
        debug!("Stale event: {:?}", last_transaction_id);
        return Ok(StreamEventProcessResult::Stale(
            wallet_id,
            last_transaction_id,
            balance,
        ));
    }

    *last_transaction_processed_time = last_transaction_time;

    info!("Received Transaction Event: {:?}", last_transaction_id);

    let transaction_event = TransactionEvent {
        wallet_id,
        balance,
        last_transaction_time,
        last_transaction_id,
    };

    // validate the transaction event: balance should not be negative
    if validate_transaction_event(&transaction_event).is_err() {
        return Ok(StreamEventProcessResult::InSufficientBalance(
            wallet_id,
            last_transaction_id,
        ));
    }

    Ok(StreamEventProcessResult::Ok(
        wallet_id,
        last_transaction_id,
        transaction_event.balance,
    ))
}

async fn compensate_transaction_event(
    conn: &mut Pool<Postgres>,
    wallet_id: WalletID,
    transaction_id: TransactionID,
) -> Result<(WalletID, TransactionID)> {
    info!("Compensating transaction: {:?}", transaction_id);
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
    Ok((wallet_id, transaction_id))
}
