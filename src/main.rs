#![allow(unused_imports)]
use std::{
    borrow::BorrowMut, clone, error::Error, fs::metadata, process, sync::Arc, time::Duration,
};

use bytes::{buf, Buf, BufMut};

mod custom_trait;
mod handler;
mod metadata;
mod protocol;

use metadata::cluster::{Cluster, ClusterSummary};
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

    let cluster_metadata = Arc::new(match metadata::cluster::parse_metadata_cluster().await {
        Ok(res) => res,
        Err(e) => {
            println!("error parsing metadata - first try");
            tokio::time::sleep(Duration::from_millis(100)).await;
            let second_try = metadata::cluster::parse_metadata_cluster().await;
            if second_try.is_err() {
                process::exit(1);
            }
            second_try.unwrap()
        }
    });

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:9092").await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let cloned = cluster_metadata.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, &cloned).await {
                eprintln!("Error handling client: {}", e);
            }
        });
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    cluster_metadata: &Cluster,
) -> tokio::io::Result<()> {
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
            1 => {
                handler::fetch::handle(&request, &mut response, cluster_metadata)
                    .await
                    .unwrap();
            }
            18 => {
                handler::api_version::handle(&request, &mut response);
            }
            75 => {
                handler::describe_topic_partitions::handle(
                    &request,
                    &mut response,
                    cluster_metadata,
                )
                .await
                .unwrap();
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
