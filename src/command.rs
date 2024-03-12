use anyhow::{anyhow, bail, Context, Result};
use std::{collections::HashMap, sync::Arc};
use tokio::{
    net::TcpStream,
    sync::RwLock,
    time::{sleep, Duration},
};

use crate::{
    config::{Config, Role},
    resp::Data,
};

#[derive(Debug, Clone, Copy)]
pub enum Command<'a> {
    Ping {
        message: Option<&'a str>,
    },
    Echo {
        message: &'a str,
    },
    Set {
        key: &'a str,
        value: &'a str,
        ttl: Option<u64>,
    },
    Get {
        key: &'a str,
    },
    Info {
        sections: [Option<&'a str>; 3],
    },
}

impl<'a> TryFrom<Data<'a>> for Command<'a> {
    type Error = anyhow::Error;

    fn try_from(value: Data<'a>) -> Result<Self> {
        match value {
            Data::Array(arr) => {
                let cmd = &arr[0];

                let cmd = match cmd {
                    Data::BulkString(cmd) => cmd.with_context(|| "WRONGTYPE command is nil")?,
                    _ => bail!("RESP invalid command"),
                };

                match cmd {
                    "PING" | "ping" => match arr.len() {
                        1 => Ok(Command::Ping { message: None }),
                        2 => {
                            if let Data::BulkString(message) = arr[1] {
                                Ok(Command::Ping { message })
                            } else {
                                Err(anyhow!("RESP invalid command"))
                            }
                        }
                        _ => Err(anyhow!("ERR wrong number of arguments for 'ping' command")),
                    },
                    "ECHO" | "echo" => {
                        if arr.len() != 2 {
                            bail!("ERR wrong number of arguments for 'echo' command");
                        }

                        if let Data::BulkString(message) = arr[1] {
                            Ok(Command::Echo {
                                message: message.with_context(|| "WRONGTYPE message is nil")?,
                            })
                        } else {
                            Err(anyhow!("RESP invalid command"))
                        }
                    }
                    "SET" | "set" => {
                        if arr.len() < 3 {
                            bail!("ERR wrong number of arguments for 'set' command");
                        };

                        let key = &arr[1];
                        let value = &arr[2];
                        let option = arr.get(3);
                        let ttl = match option {
                            Some(Data::BulkString(Some("px"))) => {
                                if let Data::BulkString(ttl) =
                                    arr.get(4).with_context(|| "ERR syntax error")?
                                {
                                    Some(
                                        ttl.with_context(|| "WRONGTYPE ttl is nil")?
                                            .parse::<u64>()
                                            .map_err(|_| {
                                                anyhow!("WRONGTYPE ttl is not an integer")
                                            })?,
                                    )
                                } else {
                                    bail!("RESP invalid command");
                                }
                            }
                            None => None,
                            Some(Data::BulkString(_)) => {
                                bail!("UNIMPLEMENTED unknown option for set")
                            }
                            _ => bail!("RESP invalid command"),
                        };

                        match (key, value) {
                            (Data::BulkString(key), Data::BulkString(value)) => Ok(Command::Set {
                                key: key.with_context(|| "WRONGTYPE key is nil")?,
                                value: value.with_context(|| "WRONGTYPE value is nil")?,
                                ttl,
                            }),
                            _ => Err(anyhow!("RESP invalid command")),
                        }
                    }
                    "GET" | "get" => {
                        if arr.len() != 2 {
                            bail!("ERR wrong number of arguments for 'get' command");
                        }

                        if let Data::BulkString(key) = arr[1] {
                            Ok(Command::Get {
                                key: key.with_context(|| "WRONGTYPE key is nil")?,
                            })
                        } else {
                            Err(anyhow!("RESP invalid command"))
                        }
                    }
                    "INFO" | "info" => {
                        if arr.len() > 4 {
                            bail!("ERR 'info' supports up to 3 sections");
                        }

                        let mut sections = [None; 3];
                        for (i, section) in arr.iter().enumerate().skip(1) {
                            if let Data::BulkString(section) = section {
                                sections[i - 1] = *section;
                            } else {
                                bail!("RESP invalid command");
                            }
                        }

                        Ok(Command::Info { sections })
                    }
                    _ => Err(anyhow!("UNIMPLEMENTED unknown command")),
                }
            }
            _ => Err(anyhow!("RESP invalid command")),
        }
    }
}

pub async fn run<'a>(
    cmd: Command<'_>,
    db: Arc<RwLock<HashMap<String, String>>>,
    socket: Arc<RwLock<TcpStream>>,
    config: Arc<Config>,
) -> Result<()> {
    match cmd {
        Command::Ping { message } => {
            let msg = message.unwrap_or("PONG");
            let output = Data::SimpleString(msg);
            let mut socket_write = socket.write().await;
            output.write_to(&mut *socket_write).await
        }
        Command::Echo { message } => {
            let output = Data::SimpleString(message);
            let mut socket_write = socket.write().await;
            output.write_to(&mut *socket_write).await
        }
        Command::Set { key, value, ttl } => {
            let key = key.to_string();
            if let Some(ttl) = ttl {
                let key = key.clone();
                let db = db.clone();
                tokio::spawn(async move {
                    expire_key(key, db, ttl).await;
                });
            }
            let mut db_write = db.write().await;
            db_write.insert(key, value.to_string());
            let output = Data::SimpleString("OK");
            let mut socket_write = socket.write().await;
            output.write_to(&mut *socket_write).await
        }
        Command::Get { key } => {
            let db_read = db.read().await;
            let output = match db_read.get(key) {
                Some(val) => Data::BulkString(Some(val.as_str())),
                None => Data::BulkString(None),
            };
            let mut socket_write = socket.write().await;
            output.write_to(&mut *socket_write).await
        }
        Command::Info { .. } => {
            let mut info = String::new();
            info.push_str("# Replication\r\n");
            match config.role() {
                Role::Slave { .. } => {
                    info.push_str("role:slave\r\n");
                }
                Role::Master { id, offset } => {
                    info.push_str("role:master\r\n");
                    info.push_str("master_replid:");
                    info.push_str(id.as_str());
                    info.push_str("\r\n");
                    info.push_str("master_repl_offset:");
                    info.push_str(offset.to_string().as_str());
                    info.push_str("\r\n");
                }
            }
            let output = Data::BulkString(Some(info.as_str()));
            let mut socket_write = socket.write().await;
            output.write_to(&mut *socket_write).await
        }
    }
}

async fn expire_key(key: String, db: Arc<RwLock<HashMap<String, String>>>, ttl: u64) {
    sleep(Duration::from_millis(ttl)).await;
    let mut db_write = db.write().await;
    db_write.remove(&key);
}
