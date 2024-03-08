pub mod generate_transaction;
pub mod subscribe_transaction;

use std::env;

use actix_web::{post, web, App, HttpResponse, HttpServer, Responder};
use anyhow::Result;
use log::info;
use serde::{Deserialize, Serialize};
use sqlx::types::BigDecimal;
use subscribe_transaction::{Amount, TransactionID, WalletID};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Serialize, Deserialize)]
struct TransactionRequest {
    wallet_id: WalletID,
    amount: i64,
    transaction_id: TransactionID,
}
#[post("/transaction")]
async fn transaction(
    app_state: web::Data<AppState>,
    req: web::Json<TransactionRequest>,
) -> impl Responder {
    let (tx, rx) = tokio::sync::oneshot::channel();
    app_state
        .stream_sender
        .send((
            req.wallet_id,
            req.transaction_id,
            BigDecimal::from(req.amount),
            tx,
        ))
        .await
        .unwrap();
    let result = rx.await;
    if let Err(e) = result {
        return HttpResponse::InternalServerError().body(e.to_string());
    }

    match result.unwrap() {
        Ok(balance) => HttpResponse::Created().body(balance.to_string()),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

pub struct AppState {
    stream_sender: tokio::sync::mpsc::Sender<(
        WalletID,
        TransactionID,
        Amount,
        tokio::sync::oneshot::Sender<Result<Amount>>,
    )>,
}
#[actix_web::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // logger setup
    env::set_var("RUST_LOG", "info");
    env_logger::init();

    // cancellation token for graceful shutdown
    let token = CancellationToken::new();
    let child_token = token.clone();

    // spawn stream to listen for transaction events
    let (tx, rx) = tokio::sync::mpsc::channel(100);
    let subscribe_handle = tokio::spawn(async move {
        subscribe_transaction::start_subscribe(child_token, rx)
            .await
            .unwrap();
    });

    // launch the web server
    let app = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(AppState {
                stream_sender: tx.clone(),
            }))
            .service(transaction)
    })
    .bind(("127.0.0.1", 3000))
    .expect("Can not bind to port 3000")
    .run();
    let app_handle = app.handle();
    let app_task = tokio::spawn(app);

    // graceful shutdown
    let shutdown = tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for ctrl-c event");
        info!("Shutting down server");
        let app_stop = app_handle.stop(true);

        token.cancel();

        app_stop.await;
    });

    let _ = tokio::try_join!(subscribe_handle, app_task, shutdown).expect("Failed to run app");

    Ok(())
}
