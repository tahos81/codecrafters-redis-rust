use anyhow::{Context, Result};
use std::env;

pub struct Config {
    port: String,
    role: Role,
}

pub enum Role {
    Master,
    Slave(String),
}

impl Config {
    pub fn parse() -> Result<Config> {
        let mut port = None;
        let mut replica_of = None;

        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--port" => {
                    port = Some(args.next().with_context(|| "missing port")?);
                }
                "--replicaof" => {
                    let master_host = args.next().with_context(|| "missing master host")?;
                    let master_port = args.next().with_context(|| "missing master port")?;
                    replica_of = Some(format!("{}:{}", master_host, master_port));
                }
                _ => {
                    eprintln!("unknown argument: {}", arg);
                }
            }
        }

        let port = port.unwrap_or("6379".to_string());
        let role = if let Some(master) = replica_of {
            Role::Slave(master)
        } else {
            Role::Master
        };

        Ok(Config { port, role })
    }

    pub fn port(&self) -> &str {
        self.port.as_ref()
    }

    pub fn role(&self) -> &Role {
        &self.role
    }
}
