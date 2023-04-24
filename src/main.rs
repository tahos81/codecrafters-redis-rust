use std::{
    collections::HashMap,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    process::exit,
    sync::Arc,
    thread::sleep,
    time::Duration,
};

use tokio::sync::Mutex;

enum Commands {
    Echo,
    Ping,
    Set,
    Get,
    Undefined,
}

type SafeMap = Arc<Mutex<HashMap<String, String>>>;

#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");

    let storage: SafeMap = Arc::new(Mutex::new(HashMap::new()));

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                tokio::spawn(handle_stream(_stream, storage.clone()));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

async fn handle_stream(mut stream: TcpStream, storage: SafeMap) {
    println!("accepted new connection");

    let mut buf = [0u8; 512];
    let mut _handle;

    loop {
        let bytes_read = stream.read(&mut buf).unwrap_or_default();

        if bytes_read == 0 {
            break;
        } else {
            let input = std::str::from_utf8(&buf[0..bytes_read]).unwrap();
            match handle_input(input) {
                Commands::Ping => {
                    stream.write_all(b"+PONG\r\n").unwrap();
                }
                Commands::Echo => {
                    let input_lines = input.lines().collect::<Vec<&str>>();
                    let echo_word = input_lines[4];
                    stream
                        .write_all(
                            format!("${}\r\n{}{}", echo_word.len(), echo_word, "\r\n").as_bytes(),
                        )
                        .unwrap();
                }
                Commands::Set => {
                    dbg!("setting");
                    let input_lines = input.lines().collect::<Vec<&str>>();
                    let key = input_lines[4];
                    let value = input_lines[6];
                    let option = input_lines.get(8);
                    match option {
                        Some(opt) => {
                            if opt == &"px" {
                                let expiry = input_lines[10];
                                let mut inner_map = storage.lock().await;
                                let _old_value =
                                    inner_map.insert(key.to_string(), value.to_string());
                                match _old_value {
                                    Some(value) => {
                                        stream
                                            .write_all(
                                                format!("${}\r\n{}{}", value.len(), value, "\r\n")
                                                    .as_bytes(),
                                            )
                                            .unwrap();
                                    }
                                    None => stream.write_all(b"+OK\r\n").unwrap(),
                                }
                                drop(inner_map);
                                // tokio::spawn(expire(
                                //     expiry.to_string(),
                                //     key.to_string(),
                                //     storage.clone(),
                                // ));
                                let map = storage.clone();
                                let exp = expiry.to_string();
                                let owned_key = key.to_string();
                                _handle = std::thread::spawn(move || expire(exp, owned_key, map));
                                //handle.join().unwrap().await;
                            } else {
                                eprintln!("something is wrong");
                                exit(1);
                            }
                        }
                        None => {
                            let mut inner_map = storage.lock().await;
                            let _old_value = inner_map.insert(key.to_string(), value.to_string());
                            match _old_value {
                                Some(value) => {
                                    stream
                                        .write_all(
                                            format!("${}\r\n{}{}", value.len(), value, "\r\n")
                                                .as_bytes(),
                                        )
                                        .unwrap();
                                }
                                None => stream.write_all(b"+OK\r\n").unwrap(),
                            }
                        }
                    }
                }
                Commands::Get => {
                    dbg!("getting");
                    let input_lines = input.lines().collect::<Vec<&str>>();
                    let key = input_lines[4];
                    let inner_map = storage.lock().await;
                    let value = inner_map.get(key);
                    match value {
                        Some(val) => {
                            stream
                                .write_all(
                                    format!("${}\r\n{}{}", val.len(), val, "\r\n").as_bytes(),
                                )
                                .unwrap();
                        }
                        None => {
                            stream.write_all(b"$-1\r\n").unwrap();
                        }
                    }
                }
                Commands::Undefined => {
                    stream.write_all(b"+UNDEFINED COMMAND\r\n").unwrap();
                }
            }
            buf = [0u8; 512];
            dbg!("buf cleaned");
        }
    }
}

fn handle_input(input: &str) -> Commands {
    for line in input.lines() {
        match line {
            "echo" => return Commands::Echo,
            "ping" => return Commands::Ping,
            "set" => return Commands::Set,
            "get" => return Commands::Get,
            _ => {}
        }
    }

    Commands::Undefined
}

async fn expire(expiry: String, key: String, storage: SafeMap) {
    let duration = expiry.parse::<u64>().unwrap();
    dbg!("start removing");
    sleep(Duration::from_millis(duration));
    let mut inner_map = storage.lock().await;
    inner_map.remove(&key);
    dbg!("removed");
}
