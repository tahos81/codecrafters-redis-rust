use anyhow::Result;
use std::{collections::HashMap, sync::Arc};
use tokio::{io::AsyncReadExt, sync::RwLock};

use crate::command::{self, Command};
use crate::config::Config;
use crate::resp::Data;

pub async fn handle(
    socket: tokio::net::TcpStream,
    db: Arc<RwLock<HashMap<String, String>>>,
    config: Arc<Config>,
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
            Ok(cmd) => command::run(cmd, db.clone(), socket.clone(), config.clone()).await?,
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
