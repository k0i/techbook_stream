use std::env;

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use serde_json::json;
use uuid::Uuid;

#[tokio::main]
pub async fn main() {
    let args = env::args().collect::<Vec<String>>();
    if args.len() != 3 {
        println!("Usage: generate_transaction <gen_time> <amount>");
        return;
    }
    let gen_time = args[1].parse::<usize>().expect("gen_time must be a number");
    let amount = args[2].parse::<i64>().expect("amount must be a number");

    gen(gen_time, amount).await.unwrap();
}

pub async fn gen(gen_time: usize, amount: i64) -> Result<(), Box<dyn std::error::Error>> {
    let mut tasks = FuturesUnordered::new();

    for _ in 0..gen_time {
        let uuid = Uuid::new_v4();
        let data = json!({
            "wallet_id": 1,
            "amount": amount,
            "transaction_id": uuid.to_string(),
        });
        tasks.push(tokio::spawn({
            reqwest::Client::new()
                .post("http://localhost:3000/transaction")
                .header("Content-Type", "application/json")
                .json(&data)
                .send()
        }));
    }

    while let Some(task) = tasks.next().await {
        match task {
            Ok(resp) => {
                let resp = resp.expect("Error in response");
                if resp.status().is_success() {
                    let body = resp.text().await?;
                    println!("balance: {:?}", body);
                } else {
                    println!("Error: {:?}", resp);
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

    for _ in 0..gen_time {
        let uuid = Uuid::new_v4();
        let query = format!(
            "INSERT INTO transaction_events values(1, '{}', {}, now(), NULL)",
            uuid, amount
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
