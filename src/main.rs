#![allow(unused_imports)]
use std::{
    borrow::BorrowMut,
    error::Error,
    io::{BufRead, BufReader, BufWriter, Read, Write},
    net::{TcpListener, TcpStream},
};

use bytes::{buf, Buf, BufMut};

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

#[derive(Debug)]
struct Request {
    message_size: u32,
    request_api_key: u16,
    request_api_version: u16,
    correlation_id: u32,
    data: Vec<u8>,
}

impl Request {
    fn new<T: Read>(mut stream: T) -> Result<Request, Box<dyn Error>> {
        let mut buffer: [u8; 1024] = [0; 1024];
        stream.read(&mut buffer)?;

        let mut request = buffer.as_slice();

        let message_size: u32 = request.get_u32();
        let request_api_key = request.get_u16();
        let request_api_version = request.get_u16();
        let correlation_id = request.get_u32();

        Ok(Request {
            message_size,
            request_api_key,
            request_api_version,
            correlation_id,
            data: vec![0],
        })
    }
    fn log(&self) {
        println!("[REQUEST] message_size: {}", self.message_size);
        println!("[REQUEST] request_api_key: {}", self.request_api_key);
        println!(
            "[REQUEST] request_api_version: {}",
            self.request_api_version
        );
        println!("[REQUEST] correlation_id: {}", self.correlation_id);
        println!("[REQUEST] data: {:?}", self.data);
    }
}

fn handle_connection(mut stream: &TcpStream) {
    let mut buf_reader = BufReader::new(stream);
    let mut buf_writer = BufWriter::new(&mut stream);
    let request = Request::new(&mut buf_reader).unwrap();
    request.log();

    buf_writer
        .write_all(&request.message_size.to_be_bytes())
        .expect("fail to write");
    buf_writer
        .write_all(&request.correlation_id.to_be_bytes())
        .expect("fail to write");
}
