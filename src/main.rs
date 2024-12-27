#![allow(unused_imports)]
use std::{borrow::BorrowMut, error::Error, time::Duration};

use bytes::{buf, Buf, BufMut};

mod protocol;

use protocol::{
    request::{Request, RequestError},
    response::Response,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() -> tokio::io::Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:9092").await?;
    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream).await {
                eprintln!("Error handling client: {}", e);
            }
        });
    }
}

async fn handle_connection(mut stream: TcpStream) -> tokio::io::Result<()> {
    loop {
        let request = match Request::new(&mut stream).await {
            Ok(r) => r,
            Err(RequestError::ClientDisconnected) => {
                break;
            }
            Err(RequestError::IoError(err)) => {
                break;
            }
        };
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
            .send(&mut stream)
            .await
            .expect("error sending response");
    }
    Ok(())
}
