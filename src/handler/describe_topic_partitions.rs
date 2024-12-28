use std::{array, error::Error, io::Cursor};

use anyhow::Ok;
use bytes::{Buf, BufMut, Bytes};
use tokio::io::AsyncReadExt;

use crate::{
    custom_trait::cursor::{AsyncReadVarint, ReadVarint, WriteVarint},
    metadata::cluster::{self, Cluster, ClusterSummary},
    protocol::{request::Request, response::Response},
};

#[derive(Debug)]
struct DescribeRequest {
    length: u8,
    response_partition_limit: u32,
    cursor: u8,
    tag_buffer: u8,
    topics: Vec<DescribeTopic>,
}

#[derive(Debug)]
struct DescribeTopic {
    name_length: u64,
    topic_name: String,
    tag_buffer: u8,
}
struct DescribeResponse {}

pub async fn handle<'a>(
    req: &Request,
    res: &mut Response<'a>,
    cluster: &Cluster,
) -> anyhow::Result<()> {
    if req.request_api_version == 0 {
        res.body.put_u32(0x00); // Throttle time
        let parsed_request = parse(&req).await?;
        dbg!(&parsed_request);
        res.body.put_u8(parsed_request.length + 1);

        let list_available_topic = cluster.topics();
        let list_available_partitions = cluster.partitions();

        for topic in parsed_request.topics {
            match list_available_topic
                .iter()
                .find(|t| t.name == topic.topic_name)
            {
                None => {
                    res.body.put_u16(3); // error_code
                    res.body.write_uvarint((topic.topic_name.len() + 1) as u64); // topic_name length + 1
                    res.body.put_slice(topic.topic_name.as_ref()); // topic_name
                    res.body.put_slice(&[0; 16]); // topic_id
                    res.body.put_u8(0); // is_internal
                    res.body.put_u8(0); // empty partitions
                }
                Some(t) => {
                    res.body.put_u16(0); // error_code
                    res.body.write_uvarint((topic.topic_name.len() + 1) as u64); // topic_name length + 1
                    res.body.put_slice(topic.topic_name.as_ref()); // topic_name
                    res.body.put_slice(t.uuid.as_ref()); // topic_id
                    res.body.put_u8(0); // is_internal

                    let partitions = list_available_partitions
                        .iter()
                        .filter(|part| part.topic_uuid == t.uuid)
                        .cloned()
                        .collect::<Vec<&cluster::PartitionValueRecord>>();
                    res.body.put_u8((partitions.len() + 1) as u8);
                    for (index, partition) in partitions.iter().enumerate() {
                        res.body.put_u16(0); // error_code
                        res.body.put_u32(index as u32); // partition_index
                        res.body.put_u32(partition.leader_id); // leader
                        res.body.put_u32(partition.leader_epoch); // leader_epoch
                        res.body.put_u8((partition.replica_nodes.len() + 1) as u8); // replica_nodes count + 1
                        for replica_node in partition.replica_nodes.iter().copied() {
                            res.body.put_u32(replica_node);
                        }
                        res.body
                            .put_u8((partition.in_sync_replica_nodes.len() + 1) as u8); // replica_nodes count + 1
                        for replica_node in partition.in_sync_replica_nodes.iter().copied() {
                            res.body.put_u32(replica_node);
                        }
                        res.body.put_u8(0); // empty eligible_leader_replicas
                        res.body.put_u8(0); // empty last_known_eligible_leader_replicas
                        res.body.put_u8(0); // empty offline_replicas
                        res.body.put_u8(0); // empty tagged_fields
                    }
                }
            };
            res.body.put_u32(0x00000df8); // topic_authorized_operations
            res.body.put_u8(0x00); // empty tagged_fields
        }
        res.body.put_u8(0xff);
        res.body.put_u8(0x00);
    } else {
        res.body.put_u16(35);
    }
    Ok(())
}

async fn parse(req: &Request) -> anyhow::Result<DescribeRequest> {
    let mut cursor = Cursor::new(&req.data);
    let array_length = cursor.async_read_uvarint().await? - 1;
    println!("array_length: {}", array_length);

    let mut topics: Vec<DescribeTopic> = Vec::new();

    for _ in 0..array_length {
        let name_length = cursor.async_read_uvarint().await? - 1;
        let mut buf = vec![0u8; name_length as usize];
        cursor.read_exact(&mut buf).await?;
        let topic_name = String::from_utf8(buf)?;
        let tag_buffer = cursor.read_u8().await?;
        topics.push(DescribeTopic {
            name_length,
            tag_buffer,
            topic_name,
        });
    }

    let response_partition_limit = cursor.read_u32().await?;
    let curs = cursor.read_u8().await?;
    let tag_buffer = cursor.read_u8().await?;

    return Ok(DescribeRequest {
        cursor: curs,
        length: array_length as u8,
        response_partition_limit,
        tag_buffer,
        topics,
    });
}
