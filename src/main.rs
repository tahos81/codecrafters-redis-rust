#![warn(clippy::all)]
#![warn(rust_2018_idioms)]
#![warn(clippy::pedantic)]

use anyhow::Result;
use std::{collections::HashMap, sync::Arc};
use tokio::{net::TcpListener, sync::RwLock};

use crate::config::Config;

mod client;
mod command;
mod config;
mod resp;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Arc::new(Config::parse()?);
    let port = config.port();
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;
    println!("listening on port {}", port);
    let db = Arc::new(RwLock::new(HashMap::new()));

    loop {
        let (socket, _) = listener.accept().await?;
        println!("accepted new connection");
        let db = db.clone();
        let config = config.clone();
        tokio::spawn(async move {
            let res = client::handle(socket, db, config).await;
            if let Err(e) = res {
                eprintln!("error: {e:?}");
            }
        });
    }
}
