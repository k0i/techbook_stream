pub mod generate_transaction;
pub mod subscribe_transaction;

use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};

#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("Hello world!")
}

pub struct AppState {
    stream_responder: tokio::mpsc::UnboundedSender<web::Json<subscribe_transaction::Transaction>>,
}

#[actix_web::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscribe_handle = tokio::spawn(async move {
        subscribe_transaction::start_subscribe().unwrap();
    });
    let app = HttpServer::new(|| App::new().service(hello))
        .bind(("127.0.0.1", 8080))
        .expect("Can not bind to port 8080")
        .run();
    let (sc, sv) = tokio::join!(subscribe_handle, app);
    if let Err(e) = sc {
        eprintln!("Error in subscribe_handle: {:?}", e);
    }
    if let Err(e) = sv {
        eprintln!("Error in app: {:?}", e);
    }

    Ok(())
}
