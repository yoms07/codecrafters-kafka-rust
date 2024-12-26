#![allow(unused_imports)]
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:9092").expect("Listening error");

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                println!("accepted new connection");
                handle_connection(&_stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_connection(mut stream: &TcpStream) {
    let message_size: [u8; 4] = [0; 4];
    let correlation_id: [u8; 4] = [0, 0, 0, 7];
    stream
        .write_all(&message_size)
        .expect("fail to write message size");
    stream
        .write_all(&correlation_id)
        .expect("fail to write correlation id");
    println!("sampe sini");
}
