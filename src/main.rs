use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                handle_stream(_stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_stream(mut stream: TcpStream) {
    println!("accepted new connection");

    let mut input = [0u8; 512];

    loop {
        let bytes_read = stream.read(&mut input).unwrap_or_default();

        if bytes_read == 0 {
            break;
        } else {
            println!("bytes read: {}", bytes_read);
            println!("{}", std::str::from_utf8(&input).unwrap());
            stream.write_all(b"+PONG\r\n").unwrap();
            input = [0u8; 512];
        }
    }
}
