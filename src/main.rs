#![warn(clippy::all)]
#![warn(rust_2018_idioms)]
#![warn(clippy::pedantic)]

use anyhow::Result;
use config::Role;
use resp::Data;
use std::{collections::HashMap, sync::Arc};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::RwLock,
};

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
    if let Role::Slave { master } = config.role() {
        connect_to_master(master).await?;
    }
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

async fn connect_to_master(master_addr: &str) -> Result<TcpStream> {
    let mut stream = TcpStream::connect(master_addr).await?;
    let cmd = command::Command::Ping { message: None };
    let data = Into::<Data<'_>>::into(cmd);
    data.write_to(&mut stream).await?;

    Ok(stream)
}
