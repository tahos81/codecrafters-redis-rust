use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    process::exit,
};

enum Commands {
    Echo(usize),
    Ping,
    Undefined,
}

#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                tokio::spawn(handle_stream(_stream));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

async fn handle_stream(mut stream: TcpStream) {
    println!("accepted new connection");

    let mut input = [0u8; 512];

    loop {
        let bytes_read = stream.read(&mut input).unwrap_or_default();

        if bytes_read == 0 {
            break;
        } else {
            match handle_input(&input[0..bytes_read]) {
                Commands::Ping => {
                    stream.write_all(b"+PONG\r\n").unwrap();
                }
                Commands::Echo(idx) => {
                    let input_string = std::str::from_utf8(&input[0..bytes_read]).unwrap();
                    let input_lines = input_string.lines().collect::<Vec<&str>>();
                    let echo_word = input_lines[idx + 2];
                    stream
                        .write_all(
                            format!("${}\r\n{}{}", echo_word.len(), echo_word, "\r\n").as_bytes(),
                        )
                        .unwrap();
                }
                Commands::Undefined => {
                    eprintln!("something is wrong");
                    exit(1);
                }
            }

            input = [0u8; 512];
        }
    }
}

fn handle_input(input: &[u8]) -> Commands {
    let input_string = std::str::from_utf8(input).unwrap();
    for (idx, line) in input_string.lines().enumerate() {
        match line {
            "echo" => return Commands::Echo(idx),
            "ping" => return Commands::Ping,
            _ => {}
        }
    }

    Commands::Undefined
}
