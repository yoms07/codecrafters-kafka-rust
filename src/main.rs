#![allow(unused_imports)]
use std::{
    io::{BufReader, Write},
    net::{TcpListener, TcpStream},
};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                handle_connection(&_stream);
                println!("accepted new connection");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_connection(mut stream: &TcpStream) {
    let buf_reader = BufReader::new(stream);
    let message_size: [u8; 4] = [0; 4];
    let correlation_id: [u8; 4] = [0, 0, 0, 7];
    stream
        .write_all(&message_size)
        .expect("fail to write message size");
    stream
        .write_all(&correlation_id)
        .expect("fail to write correlation id");
}
