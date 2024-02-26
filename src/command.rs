use crate::resp::Data;
use anyhow::{anyhow, Result};
use std::{collections::HashMap, sync::Arc};
use tokio::{
    sync::RwLock,
    time::{sleep, Duration},
};

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
}

impl<'a> TryFrom<Data<'a, &'a str>> for Command<'a> {
    type Error = anyhow::Error;

    fn try_from(value: Data<'a, &'a str>) -> Result<Self> {
        match value {
            Data::Array(arr) => {
                //this is equivalent to copy
                let cmd = &arr[0];

                let cmd = match cmd {
                    Data::BulkString(cmd) => cmd.expect("nil cmd"),
                    _ => return Err(anyhow!("invalid command")),
                };

                match cmd {
                    "PING" | "ping" => match arr.len() {
                        1 => Ok(Command::Ping { message: None }),
                        2 => {
                            if let Data::BulkString(message) = arr[1] {
                                Ok(Command::Ping { message })
                            } else {
                                unreachable!()
                            }
                        }
                        _ => Err(anyhow!("invalid arg count for ping")),
                    },
                    "ECHO" | "echo" => {
                        if arr.len() != 2 {
                            return Err(anyhow!("invalid arg count for echo"));
                        }

                        if let Data::BulkString(message) = arr[1] {
                            Ok(Command::Echo {
                                message: message.expect("nil message"),
                            })
                        } else {
                            unreachable!()
                        }
                    }
                    "SET" | "set" => {
                        if arr.len() < 3 {
                            return Err(anyhow!("invalid arg count for set"));
                        };

                        let key = &arr[1];
                        let value = &arr[2];
                        let option = arr.get(3);
                        let ttl = match option {
                            Some(Data::BulkString(Some("px"))) => {
                                if let Data::BulkString(ttl) = arr.get(4).expect("no ttl") {
                                    Some(ttl.expect("nil ttl").parse::<u64>()?)
                                } else {
                                    return Err(anyhow!("invalid ttl"));
                                }
                            }
                            None => None,
                            _ => return Err(anyhow!("invalid option")),
                        };

                        match (key, value) {
                            (Data::BulkString(key), Data::BulkString(value)) => Ok(Command::Set {
                                key: key.expect("nil key"),
                                value: value.expect("nil value"),
                                ttl,
                            }),
                            _ => Err(anyhow!("invalid command")),
                        }
                    }
                    "GET" | "get" => {
                        if arr.len() != 2 {
                            return Err(anyhow!("invalid arg count for get"));
                        }

                        if let Data::BulkString(key) = arr[1] {
                            Ok(Command::Get {
                                key: key.expect("nil key"),
                            })
                        } else {
                            unreachable!()
                        }
                    }
                    _ => unimplemented!(),
                }
            }
            _ => Err(anyhow!("Command type must be array")),
        }
    }
}

pub async fn prep_response<'a>(
    cmd: Command<'a>,
    db: Arc<RwLock<HashMap<String, String>>>,
) -> Data<'a, &'a str> {
    match cmd {
        Command::Ping { message } => {
            let resp = message.unwrap_or("PONG");
            Data::SimpleString(resp)
        }
        Command::Echo { message } => Data::SimpleString(message),
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
            Data::SimpleString("OK")
        }
        Command::Get { .. } => unreachable!(),
    }
}

pub async fn prep_get_response<'a>(
    cmd: Command<'a>,
    db: Arc<RwLock<HashMap<String, String>>>,
) -> Data<'a, String> {
    match cmd {
        Command::Get { key } => {
            let db_read = db.read().await;
            match db_read.get(key) {
                Some(val) => Data::BulkString(Some(val.clone())),
                None => Data::BulkString(None),
            }
        }
        _ => unreachable!(),
    }
}

async fn expire_key(key: String, db: Arc<RwLock<HashMap<String, String>>>, ttl: u64) {
    sleep(Duration::from_millis(ttl)).await;
    let mut db_write = db.write().await;
    db_write.remove(&key);
}
