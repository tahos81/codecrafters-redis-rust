#![warn(clippy::all)]
#![warn(rust_2018_idioms)]
#![warn(clippy::pedantic)]

use crate::command::Command;
use anyhow::Result;
use resp::Data;
use std::{collections::HashMap, sync::Arc};
use tokio::{io::AsyncReadExt, net::TcpListener, sync::RwLock};

mod command;
mod resp;

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    println!("listening");
    let db = Arc::new(RwLock::new(HashMap::new()));

    loop {
        let (socket, _) = listener.accept().await?;
        println!("accepted new connection");
        let db = db.clone();
        tokio::spawn(async move {
            let res = handle_client(socket, db).await;
            if let Err(e) = res {
                eprintln!("error: {e:?}");
            }
        });
    }
}

async fn handle_client(
    socket: tokio::net::TcpStream,
    db: Arc<RwLock<HashMap<String, String>>>,
) -> Result<()> {
    let mut buf = [0; 512];
    let socket = Arc::new(RwLock::new(socket));
    let mut bytes_read;
    loop {
        {
            let mut socket_write = socket.write().await;
            bytes_read = socket_write.read(&mut buf).await?;
        }
        if bytes_read == 0 {
            break;
        }
        let data = Data::decode(&buf);
        let data = match data {
            Ok((data, _)) => data,
            Err(e) => {
                return write_error(socket.clone(), &e.to_string()).await;
            }
        };

        let cmd = Command::try_from(data);
        match cmd {
            Ok(cmd) => command::run(cmd, db.clone(), socket.clone()).await?,
            Err(e) => {
                write_error(socket.clone(), &e.to_string()).await?;
            }
        }
    }
    Ok(())
}

async fn write_error(socket: Arc<RwLock<tokio::net::TcpStream>>, msg: &str) -> Result<()> {
    let output = Data::SimpleError(msg);
    let mut socket_write = socket.write().await;
    output.write_to(&mut *socket_write).await
}
