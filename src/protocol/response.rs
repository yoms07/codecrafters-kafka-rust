use std::{error::Error, io::Write};

use tokio::{
    io::{AsyncWriteExt, BufWriter},
    net::TcpStream,
};

use super::request::Request;

static MESSAGE_SIZE_OFFSET: usize = 4;
static CORRELATION_ID_SIZE_OFFSET: u32 = 4;

pub struct Response {
    pub correlation_id: u32,
    pub body: Vec<u8>,
}

impl Response {
    pub fn build_from_request(res: &Request) -> Self {
        return Response {
            correlation_id: res.correlation_id,
            body: vec![],
        };
    }
    pub fn message_size(&self) -> u32 {
        return CORRELATION_ID_SIZE_OFFSET + self.body.len() as u32;
    }

    pub async fn send(&self, stream: &mut TcpStream) -> Result<(), Box<dyn Error>> {
        self.log().await;
        let mut writer = BufWriter::new(stream);

        // Write the message size (u32)
        writer.write_all(&self.message_size().to_be_bytes()).await?;

        // Write the correlation ID (u32)
        writer.write_all(&self.correlation_id.to_be_bytes()).await?;

        // Write the body of the response
        writer.write_all(&self.body).await?;

        writer.flush().await?;

        Ok(())
    }

    pub async fn log(&self) {
        println!("[RESPONSE] message_size: {}", self.message_size());
        println!("[RESPONSE] correlation_id: {}", self.correlation_id);
        println!("[RESPONSE] data: {:?}", self.body);
    }
}
