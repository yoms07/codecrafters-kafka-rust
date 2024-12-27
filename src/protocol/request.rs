use core::fmt;
use std::{error::Error, io::Read};

use bytes::Buf;

#[derive(Debug)]
pub struct Request {
    pub message_size: u32,
    pub request_api_key: u16,
    pub request_api_version: u16,
    pub correlation_id: u32,
    pub data: Vec<u8>,
}

#[derive(Debug)]
pub enum RequestError {
    ClientDisconnected,
}

impl Request {
    pub fn new<T: Read>(mut stream: T) -> Result<Request, RequestError> {
        let mut buffer: [u8; 1024] = [0; 1024];
        let read_size = stream.read(&mut buffer).unwrap();
        if read_size == 0 {
            return Err(RequestError::ClientDisconnected);
        }

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
    pub fn log(&self) {
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
