#![allow(unused_imports)]
use std::{
    borrow::BorrowMut,
    error::Error,
    io::{BufRead, BufReader, BufWriter, Read, Write},
    net::{TcpListener, TcpStream},
};

use bytes::{buf, Buf, BufMut};

mod protocol;

use protocol::{request::Request, response::Response};

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

    let mut response = Response::build_from_request(&request);

    match request.request_api_key {
        18 => {
            if request.request_api_version <= 4 {
                response.body.put_u16(0); // error code
                response.body.put_u8(2); // array length + 1
                                         // first element
                response.body.put_u16(18); // api key
                response.body.put_u16(0); // min version
                response.body.put_u16(4); // max version
                response.body.put_u8(0); // TAG_BUFFER length
                response.body.put_u32(0); // Throttle time
                response.body.put_u8(0); // TAG_BUFFER length
            } else {
                response.body.put_u16(35);
            }
        }
        _ => {
            response.body.put_u16(35); // error code
        }
    }

    response
        .send(&mut buf_writer)
        .expect("error sending response");
}
