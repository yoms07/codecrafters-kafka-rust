#![allow(unused_imports)]
use std::{
    borrow::BorrowMut,
    error::Error,
    io::{BufRead, BufReader, BufWriter, Read, Write},
    net::{TcpListener, TcpStream},
};

use bytes::{buf, Buf, BufMut};

mod protocol;

use protocol::request::Request;

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
    let mut buf_reader = BufReader::new(stream);
    let mut buf_writer = BufWriter::new(&mut stream);
    let request = Request::new(&mut buf_reader).expect("Error parsing request");
    request.log();

    buf_writer
        .write_all(&request.message_size.to_be_bytes())
        .expect("fail to write");
    buf_writer
        .write_all(&request.correlation_id.to_be_bytes())
        .expect("fail to write");

    match request.request_api_version {
        0..4 => {
            buf_writer
                .write_all(&request.request_api_version.to_be_bytes())
                .expect("error write version");
        }
        _ => {
            let api_version_not_supported_error_code: [u8; 2] = [0, 35];
            buf_writer
                .write_all(&api_version_not_supported_error_code)
                .expect("error write version not supported");
        }
    }
}
