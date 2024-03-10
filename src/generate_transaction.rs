use std::env;

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use serde_json::json;
use sqlx::{Connection, PgConnection, Row};

#[tokio::main]
pub async fn main() {
    let args = env::args().collect::<Vec<String>>();
    if args.len() != 4 {
        println!("Usage: generate_transaction <gen_time> <wallet_id> <amount>");
        return;
    }
    let gen_time = args[1].parse::<usize>().expect("gen_time must be a number");
    let wallet_id = args[2].parse::<i64>().expect("wallet_id must be a number");
    let amount = args[3].parse::<i64>().expect("amount must be a number");

    gen(gen_time, wallet_id, amount).await.unwrap();
}

pub async fn gen(
    gen_time: usize,
    wallet_id: i64,
    amount: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut tasks = FuturesUnordered::new();
    let mut conn = PgConnection::connect(
        "postgres://materialize@localhost:6875/materialize?options=--cluster quickstart",
    )
    .await?;

    let row = sqlx::query("SELECT max(transaction_id) FROM transaction_events")
        .fetch_one(&mut conn)
        .await?;

    let mut latest_transaction_id = row.try_get::<i32, usize>(0).unwrap_or(0);
    println!("latest_transaction_id: {:?}", latest_transaction_id);

    for _ in 0..gen_time {
        latest_transaction_id += 1;
        let data = json!({
            "wallet_id": wallet_id,
            "amount": amount,
            "transaction_id": latest_transaction_id,
        });
        tasks.push(tokio::spawn({
            reqwest::Client::new()
                .post("http://localhost:3000/transaction")
                .header("Content-Type", "application/json")
                .json(&data)
                .send()
        }));
    }

    let mut cnt = 0;
    while let Some(task) = tasks.next().await {
        match task {
            Ok(resp) => {
                cnt += 1;
                let resp = resp.expect("Error in response");
                if resp.status().is_success() {
                    let body = resp.text().await?;
                    println!("id, balance, cnt: {:?} {:?}", body, cnt);
                } else {
                    println!("Error: {:?}", resp.status());
                    println!("Error: {:?}", resp.text().await?);
                }
            }
            Err(e) => println!("Error: {e}"),
        }
    }

    Ok(())
}

// this function directrly connect to materialize and insert data
#[allow(dead_code)]
async fn gen_direct(gen_time: usize, amount: i64) -> Result<(), Box<dyn std::error::Error>> {
    let mut tasks = FuturesUnordered::new();

    let mut conn = PgConnection::connect(
        "postgres://materialize@localhost:6875/materialize?options=--cluster quickstart",
    )
    .await?;

    let row = sqlx::query("SELECT max(transaction_id) FROM transaction_events")
        .fetch_one(&mut conn)
        .await?;

    let mut latest_transaction_id = row.try_get::<i32, usize>(0).unwrap_or(0);
    println!("latest_transaction_id: {:?}", latest_transaction_id);

    for _ in 0..gen_time {
        latest_transaction_id += 1;
        let query = format!(
            "INSERT INTO transaction_events values(1, '{}', {}, now(), NULL)",
            latest_transaction_id, amount
        );
        let insert_sql = json!({
            "query": query,
        });
        tasks.push(tokio::spawn({
            reqwest::Client::new()
                .post("http://localhost:6876/api/sql")
                .basic_auth("materialize", Option::<String>::None)
                .header("Content-Type", "application/json")
                .json(&insert_sql)
                .send()
        }));
    }

    while let Some(task) = tasks.next().await {
        if let Err(e) = task? {
            println!("Error: {:?}", e);
        }
    }

    Ok(())
}
