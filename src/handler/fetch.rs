use core::error;
use std::{any, io::Cursor};

use anyhow::Ok;
use bytes::{Buf, BufMut};
use tokio::io::AsyncReadExt;

use crate::{
    custom_trait::cursor::{AsyncReadVarint, ReadUUID, WriteVarint},
    metadata::cluster::{Cluster, ClusterSummary},
    protocol::{request::Request, response::Response},
};

#[derive(Debug)]
pub struct FetchRequest {
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub isolation_level: i8,
    pub session_id: i32,
    pub session_epoch: i32,
    pub topics: Vec<Topic>,
    pub forgotten_topics_data: Vec<ForgottenTopicData>,
    pub rack_id: String,
}

#[derive(Debug)]
pub struct Topic {
    pub topic_id: uuid::Uuid,
    pub partitions: Vec<Partition>,
}

#[derive(Debug)]
pub struct Partition {
    pub partition: i32,
    pub current_leader_epoch: i32,
    pub fetch_offset: i64,
    pub last_fetched_epoch: i32,
    pub log_start_offset: i64,
    pub partition_max_bytes: i32,
}

#[derive(Debug)]
pub struct ForgottenTopicData {
    pub topic_id: uuid::Uuid,
    pub partitions: Vec<i32>, // List of partitions
}

pub async fn handle<'a>(
    req: &Request,
    res: &mut Response<'a>,
    cluster: &Cluster,
) -> anyhow::Result<()> {
    if req.request_api_version == 16 {
        let available_topics = cluster.topics();
        let parsed = parse(req).await?;
        res.body.put_u32(0x00); // throttle time
        res.body.put_u16(0x00); // error code
        res.body.put_u32(0x00); // session_id

        res.body.put_u8((parsed.topics.len() + 1) as u8);
        for topic in parsed.topics {
            res.body.put_slice(topic.topic_id.as_ref());
            res.body.put_u8((1 + 1) as u8);

            res.body.put_i32(0); // partition index
            let topic = available_topics.iter().find(|x| x.uuid.eq(&topic.topic_id));
            let error_code = match topic {
                Some(_) => 0,
                None => 100,
            };
            res.body.put_i16(error_code); // error code
            res.body.put_i64(0); // high watermark
            res.body.put_i64(0); // last_stable_offset
            res.body.put_i64(0); // log_start_offset

            // aborted transactions
            res.body.put_u8((0 + 1) as u8); // aborted transaction length

            res.body.put_i32(0); // prefered read replica

            match topic {
                Some(topic) => {
                    let bytes_data = cluster
                        .get_partition_record_from_file(&topic.name, 0)
                        .await?;
                    res.body.write_uvarint((bytes_data.len() + 1) as u64);
                    res.body.put_slice(&bytes_data);
                }
                _ => res.body.write_uvarint((0 + 1) as u64),
            };
            res.body.put_u8(0); // partitions tag buffer

            res.body.put_u8(0); // topic tag buffer
        }

        // res.body.put_u8(0x00); // num response
        res.body.put_u8(0x00); // tag buffer
    }
    Ok(())
}

async fn parse(req: &Request) -> anyhow::Result<FetchRequest> {
    let mut cursor = Cursor::new(&req.data);

    let max_wait_ms = cursor.get_i32();
    let min_bytes = cursor.get_i32();
    let max_bytes = cursor.get_i32();
    let isolation_level = cursor.get_i8();
    let session_id = cursor.get_i32();
    let session_epoch = cursor.get_i32();

    let topic_length = cursor.async_read_uvarint().await? - 1;
    let mut topics = Vec::new();
    for _ in 0..topic_length {
        let topic_id = cursor.read_uuid().await?;
        let partitions_length = cursor.async_read_uvarint().await? - 1;
        let mut partitions = Vec::new();

        for _ in 0..partitions_length {
            let partition = cursor.get_i32(); // Partition ID (INT32)
            let current_leader_epoch = cursor.get_i32(); // Current leader epoch (INT32)
            let fetch_offset = cursor.get_i64(); // Fetch offset (INT64)
            let last_fetched_epoch = cursor.get_i32(); // Last fetched epoch (INT32)
            let log_start_offset = cursor.get_i64(); // Log start offset (INT64)
            let partition_max_bytes = cursor.get_i32(); // Partition max bytes (INT32)
            partitions.push(Partition {
                partition,
                current_leader_epoch,
                fetch_offset,
                last_fetched_epoch,
                log_start_offset,
                partition_max_bytes,
            });

            cursor.advance(1); // tag buffer
        }
        topics.push(Topic {
            topic_id,
            partitions,
        });

        cursor.advance(1); // tag buffer
    }

    let forgotten_topic_length = cursor.async_read_uvarint().await? - 1;
    let mut forgotten_topics = Vec::new();
    for _ in 0..forgotten_topic_length {
        let topic_id = cursor.read_uuid().await?;
        let forgotten_partitions_length = cursor.async_read_uvarint().await?;
        let mut forgotten_partitions: Vec<i32> = Vec::new();

        for _ in 0..forgotten_partitions_length {
            let partition = cursor.get_i32(); // Partition ID (INT32)
            forgotten_partitions.push(partition);
        }

        forgotten_topics.push(ForgottenTopicData {
            topic_id,
            partitions: forgotten_partitions,
        });

        cursor.advance(1); // tag buffer
    }

    // Read rack_id as a compact string
    let rack_id_length = cursor.async_read_uvarint().await? - 1; // Read length of rack_id
    let mut rack_id_bytes = vec![0u8; rack_id_length as usize];
    cursor.read_exact(&mut rack_id_bytes).await?;
    let rack_id = String::from_utf8(rack_id_bytes)?;
    cursor.advance(1); // tag buffer

    Ok(FetchRequest {
        max_wait_ms,
        min_bytes,
        max_bytes,
        isolation_level,
        session_id,
        session_epoch,
        topics,
        forgotten_topics_data: forgotten_topics,
        rack_id,
    })
}
