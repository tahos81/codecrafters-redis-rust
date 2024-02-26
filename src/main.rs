#![warn(clippy::all)]
#![warn(rust_2018_idioms)]
#![warn(clippy::pedantic)]

use crate::command::Command;
use anyhow::Result;
use std::{collections::HashMap, sync::Arc};
use tokio::{io::AsyncReadExt, net::TcpListener, sync::RwLock};

mod command;
mod resp;

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    let db = Arc::new(RwLock::new(HashMap::new()));

    loop {
        let (socket, _) = listener.accept().await?;
        println!("accepted new connection");
        let db = db.clone();
        tokio::spawn(async move { handle_client(socket, db).await });
    }
}

async fn handle_client(
    mut socket: tokio::net::TcpStream,
    db: Arc<RwLock<HashMap<String, String>>>,
) -> Result<()> {
    let mut buf = [0; 512];
    loop {
        let bytes_read = socket.read(&mut buf).await?;
        if bytes_read == 0 {
            break;
        }
        let (data, _) = resp::Data::<&str>::decode(&buf)?;
        let cmd = Command::try_from(data)?;
        match cmd {
            Command::Get { .. } => {
                let resp = command::prep_get_response(cmd, db.clone()).await;
                resp.write_to(&mut socket).await?;
            }
            _ => {
                let resp = command::prep_response(cmd, db.clone()).await;
                resp.write_to(&mut socket).await?;
            }
        }
    }
    Ok(())
}
