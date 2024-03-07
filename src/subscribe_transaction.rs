use anyhow::Result;
use postgres::Client;
use std::time::SystemTime;
use uuid::Uuid;

#[derive(Debug)]
struct TransactionEvent {
    wallet_id: u32,
    balance: i32,
    last_transaction_time: SystemTime,
    last_transaction_id: Uuid,
}

pub fn start_subscribe() -> Result<()> {
    let config = "postgres://materialize@localhost:6875/materialize?options=--cluster quickstart";
    let mut stream_client = Client::connect(config, tokio_postgres::NoTls).unwrap();
    let mut decline_client = Client::connect(config, tokio_postgres::NoTls).unwrap();

    stream_client.execute("set auto_route_introspection_queries = false", &[])?;

    // reduce the number of transactions to be processed
    let mut last_transaction_time = Option::<SystemTime>::None;

    stream_client.simple_query(
        "BEGIN;DECLARE c CURSOR for SUBSCRIBE (SELECT * from wallet_balance) WITH (SNAPSHOT=false);",
    )?;
    println!("Subscribed to the transaction stream");
    loop {
        let rows = stream_client.simple_query("FETCH ALL c").unwrap();
        for r in rows {
            if let tokio_postgres::SimpleQueryMessage::Row(row) = r {
                let declined = match row.get("declined") {
                    Some(_) => Some(true),
                    None => None,
                };
                if declined.is_some() {
                    // the change produced is due to compensation.
                    // we don't need to validate this transaction
                    continue;
                }

                let transaction_event = TransactionEvent {
                    wallet_id: row.get("wallet_id").unwrap().parse().unwrap(),
                    balance: row.get("balance").unwrap().parse().unwrap(),
                    last_transaction_time: convert_str_to_systemtime(
                        row.get("last_transaction_time").unwrap().to_owned(),
                    ),
                    last_transaction_id: Uuid::parse_str(row.get("last_transaction_id").unwrap())
                        .unwrap(),
                };
                match validate_transaction_event(&transaction_event) {
                    Ok(_) => {
                        if let Some(last_time) = last_transaction_time {
                            if last_time > transaction_event.last_transaction_time {
                                // ignore since we have already processed this or newer one
                                continue;
                            }
                            last_transaction_time = Some(transaction_event.last_transaction_time);
                        } else {
                            // for the first time
                            last_transaction_time = Some(transaction_event.last_transaction_time);
                        }
                    }
                    Err(_) => {
                        // balance is negative
                        // do compensation: decline the transaction and retrieve the balance
                        if let Err(e) =
                            compensate_transaction_event(&mut decline_client, &transaction_event)
                        {
                            eprintln!("Need Action!! Transaction Compensation Failed: {:?}", e);
                        }
                    }
                };
            }
        }
    }
}

fn convert_str_to_systemtime(time: String) -> SystemTime {
    let time = time.replace(" ", "T");
    let time = time + "Z";
    SystemTime::from(time.parse::<chrono::DateTime<chrono::Utc>>().unwrap())
}

fn validate_transaction_event(event: &TransactionEvent) -> Result<()> {
    if event.balance < 0 {
        return Err(anyhow::anyhow!("Balance cannot be negative"));
    }
    Ok(())
}

fn compensate_transaction_event(client: &mut Client, event: &TransactionEvent) -> Result<()> {
    let transaction_id = event.last_transaction_id;
    let wallet_id = event.wallet_id;
    let query = format!("UPDATE transaction_events SET declined = true WHERE transaction_id = '{}' AND wallet_id = '{}';", transaction_id, wallet_id);
    client.simple_query(&query)?;
    Ok(())
}
