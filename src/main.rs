#![allow(unused_imports)]
use std::{
    error::Error,
    io::{BufRead, BufReader, BufWriter, Read, Write},
    net::{TcpListener, TcpStream},
};

use bytes::buf;

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
    fn new(mut stream: &TcpStream) -> Result<Request, Box<dyn Error>> {
        let mut message_size_bytes: [u8; 4] = [0; 4];
        stream.read_exact(&mut message_size_bytes)?;
        let message_size = u32::from_be_bytes(message_size_bytes);

        let mut request_api_key_bytes: [u8; 2] = [0; 2];
        stream.read_exact(&mut request_api_key_bytes)?;
        let request_api_key = u16::from_be_bytes(request_api_key_bytes);

        let mut request_api_version_bytes: [u8; 2] = [0; 2];
        stream.read_exact(&mut request_api_version_bytes)?;
        let request_api_version = u16::from_be_bytes(request_api_version_bytes);

        let mut correlation_id_bytes: [u8; 4] = [0; 4];
        stream.read_exact(&mut correlation_id_bytes)?;
        let correlation_id = u32::from_be_bytes(correlation_id_bytes);

        let mut data_bytes: Vec<u8> = Vec::new();
        stream.read_to_end(&mut data_bytes);

        Ok(Request {
            message_size,
            request_api_key,
            request_api_version,
            correlation_id,
            data: data_bytes,
        })
    }
    fn log(&self) {
        println!("[REQUEST] message_size: {}", self.message_size);
        println!("[REQUEST] request_api_key: {}", self.request_api_key);
        println!(
            "[REQUEST] request_api_version: {}",
            self.request_api_version
        );
        println!("[REQUEST] data: {:?}", self.data);
    }
}

fn handle_connection(stream: &TcpStream) {
    let mut buf_writer = BufWriter::new(stream);
    let request = Request::new(buf_writer.get_ref()).unwrap();
    println!("{:?}", request);
    request.log();
    let message_size: [u8; 4] = [10; 4];
    buf_writer
        .write_all(&message_size)
        .expect("fail to write message size");

    buf_writer
        .write_all(&request.correlation_id.to_be_bytes())
        .expect("fail to write correlation id");
}
