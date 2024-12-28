use crate::custom_trait::cursor::{AsyncReadVarint, ReadUUID};
use std::{any, io::Cursor, path::Path};

use crate::protocol::response;
use anyhow::Ok;
use bytes::{Buf, BufMut};
use tokio::io::AsyncReadExt;

pub type Cluster = Vec<Batch>;

#[derive(Clone, Debug)]
pub struct Batch {
    pub batch_offset: u64,
    pub batch_length: u32,
    pub partition_leader_epoch: u32,
    pub magic_byte: u8,
    pub crc: u32,
    pub attributes: u16,
    pub last_offset_delta: u32,
    pub base_timestamp: u64,
    pub max_timestamp: u64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub record_batch_length: u32,
    pub records: Vec<Record>,
}
#[derive(Clone, Debug)]
pub struct Record {
    pub record_length: i64,
    pub attributes: u8,
    pub timestamp_delta: i64,
    pub offset_delta: i64,
    pub key_length: i64,
    pub key: Option<Vec<u8>>,
    pub value_length: i64,
    pub value: Value,
}

#[derive(Clone, Debug)]
pub struct Value {
    pub frame_version: u8,
    pub type_: u8,
    pub version: u8,
    pub value: ValueRecord,
    pub tagged_fields: u64,
}
#[derive(Clone, Debug)]
pub enum ValueRecord {
    FeatureValue(FeatureValueRecord),
    TopicValue(TopicValueRecord),
    PartitionValue(PartitionValueRecord),
    Unknown,
}
#[derive(Clone, Debug)]
pub struct FeatureValueRecord {
    pub name_length: i64,
    pub name: String,
    pub feature_level: i16,
}

#[derive(Clone, Debug)]
pub struct TopicValueRecord {
    pub name_length: u64,
    pub name: String,
    pub uuid: uuid::Uuid,
}

#[derive(Clone, Debug)]
pub struct PartitionValueRecord {
    pub id: u32,
    pub topic_uuid: uuid::Uuid,
    pub leader_id: u32,
    pub leader_epoch: u32,
    pub replica_nodes: Vec<u32>,
    pub in_sync_replica_nodes: Vec<u32>,
}

pub async fn parse_metadata_cluster() -> anyhow::Result<Cluster> {
    let path = Path::new("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log");
    let content = tokio::fs::read(path).await?;
    let mut cursor = Cursor::new(&content);

    let mut cluster: Vec<Batch> = Vec::new();
    while cursor.has_remaining() {
        let base_offset = cursor.read_u64().await?;
        let batch_length = cursor.read_u32().await?;

        let mut single_batch_buf = vec![0u8; batch_length as usize];
        cursor.read_exact(&mut single_batch_buf).await?;
        let mut single_batch_cursor = Cursor::new(&single_batch_buf);
        let batch = parse_single_batch(&mut single_batch_cursor, base_offset, batch_length).await?;

        cluster.push(batch);
    }
    Ok(cluster)
}

async fn parse_single_batch(
    cursor: &mut Cursor<&Vec<u8>>,
    batch_offset: u64,
    batch_length: u32,
) -> anyhow::Result<Batch> {
    let partition_leader_epoch = cursor.read_u32().await?;
    let magic_byte = cursor.read_u8().await?;
    let crc = cursor.read_u32().await?;
    let attributes = cursor.read_u16().await?;
    let last_offset_delta = cursor.read_u32().await?;
    let base_timestamp = cursor.read_u64().await?;
    let max_timestamp = cursor.read_u64().await?;
    let producer_id = cursor.read_i64().await?;
    let producer_epoch = cursor.read_i16().await?;
    let base_sequence = cursor.read_i32().await?;
    let record_batch_length = cursor.read_u32().await?;

    let mut records: Vec<Record> = Vec::new();

    for _ in 0..record_batch_length {
        let record_length = cursor.async_read_varint().await?; // 1 byte
        let mut record_buf = vec![0u8; record_length as usize];
        cursor.read_exact(&mut record_buf).await?;
        let mut record_cursor = Cursor::new(&record_buf);
        let record = parse_record(&mut record_cursor, record_length).await?;
        records.push(record);
    }
    Ok(Batch {
        partition_leader_epoch,
        magic_byte,
        crc,
        attributes,
        last_offset_delta,
        base_timestamp,
        max_timestamp,
        producer_id,
        producer_epoch,
        base_sequence,
        record_batch_length,
        batch_length,
        batch_offset,
        records,
    })
}

async fn parse_record(cursor: &mut Cursor<&Vec<u8>>, record_length: i64) -> anyhow::Result<Record> {
    let attributes = cursor.read_u8().await?;
    let timestamp_delta = cursor.async_read_varint().await?;
    let offset_delta = cursor.async_read_varint().await?;
    let key_length = cursor.async_read_varint().await?;
    let key = match key_length {
        -1 => None,
        0 => Some(vec![]),
        _ => {
            let mut buf = vec![0u8; key_length as usize];
            cursor.read_exact(&mut buf).await?;
            Some(buf)
        }
    };

    let value_length = cursor.async_read_varint().await?;
    let mut value_buf = vec![0u8; value_length as usize];
    cursor.read_exact(&mut value_buf).await?;

    let mut value_cursor = Cursor::new(&value_buf);

    let value = parse_value(&mut value_cursor).await?;

    let header_array_count = cursor.async_read_uvarint().await?;
    if header_array_count > 0 {
        cursor.advance(header_array_count as usize);
    }
    Ok(Record {
        record_length,
        attributes,
        timestamp_delta,
        offset_delta,
        key_length,
        key,
        value_length,
        value,
    })
}

async fn parse_value(cursor: &mut Cursor<&Vec<u8>>) -> anyhow::Result<Value> {
    let frame_version = cursor.read_u8().await?;
    let type_ = cursor.read_u8().await?;
    let version = cursor.read_u8().await?;
    let value: ValueRecord = match type_ {
        2 => ValueRecord::TopicValue(parse_topic_record(cursor).await?),
        3 => ValueRecord::PartitionValue(parse_partition_record(cursor).await?),
        _ => ValueRecord::Unknown,
    };
    let tagged_fields = cursor.async_read_uvarint().await?;
    Ok(Value {
        frame_version,
        tagged_fields,
        type_,
        value,
        version,
    })
}

async fn parse_topic_record(cursor: &mut Cursor<&Vec<u8>>) -> anyhow::Result<TopicValueRecord> {
    let topic_name_length = cursor.async_read_uvarint().await?;
    let topic_name = if topic_name_length <= 1 {
        "".to_string()
    } else {
        let mut data = vec![0u8; (topic_name_length - 1) as usize];
        cursor.read_exact(&mut data).await?;
        String::from_utf8(data)?
    };
    let topic_uuid = cursor.read_uuid().await?;
    Ok(TopicValueRecord {
        name_length: topic_name_length,
        name: topic_name,
        uuid: topic_uuid,
    })
}

async fn parse_partition_record(
    cursor: &mut Cursor<&Vec<u8>>,
) -> anyhow::Result<PartitionValueRecord> {
    let partition_id = cursor.read_u32().await?;
    let topic_uuid = cursor.read_uuid().await?;

    let mut replica_nodes = vec![];
    let replica_array_length = cursor.async_read_uvarint().await?;
    for _ in 0..replica_array_length - 1 {
        replica_nodes.push(cursor.read_u32().await?);
    }
    let mut in_sync_replica_nodes = vec![];
    let in_sync_replica_array_length = cursor.async_read_uvarint().await?;
    for _ in 0..in_sync_replica_array_length - 1 {
        in_sync_replica_nodes.push(cursor.read_u32().await?);
    }
    let removing_replica_array_length = cursor.async_read_uvarint().await?;
    for _ in 0..removing_replica_array_length - 1 {
        let _replica_id = cursor.read_u32().await?;
    }
    let adding_replica_array_length = cursor.async_read_uvarint().await?;
    for _ in 0..adding_replica_array_length - 1 {
        let _replica_id = cursor.read_u32().await?;
    }
    let leader_id = cursor.read_u32().await?;
    let leader_epoch = cursor.read_u32().await?;
    let _partition_epoch = cursor.read_u32().await?;
    let directories_array_length = cursor.async_read_uvarint().await?;
    for _ in 0..directories_array_length - 1 {
        let _directory = cursor.read_uuid().await?;
    }
    Ok(PartitionValueRecord {
        id: partition_id,
        topic_uuid,
        leader_id,
        leader_epoch,
        replica_nodes,
        in_sync_replica_nodes,
    })
}

pub trait ClusterSummary {
    fn partitions(&self) -> Vec<&PartitionValueRecord>;
    fn topics(&self) -> Vec<&TopicValueRecord>;
}

impl ClusterSummary for Cluster {
    fn partitions(&self) -> Vec<&PartitionValueRecord> {
        self.iter()
            .flat_map(|batch| batch.records.iter())
            .filter_map(|record| match &record.value.value {
                ValueRecord::PartitionValue(partition) => Some(partition),
                _ => None,
            })
            .collect::<Vec<&PartitionValueRecord>>()
    }

    fn topics(&self) -> Vec<&TopicValueRecord> {
        self.iter()
            .flat_map(|batch| batch.records.iter())
            .filter_map(|record| match &record.value.value {
                ValueRecord::TopicValue(topic) => Some(topic),
                _ => None,
            })
            .collect::<Vec<&TopicValueRecord>>()
    }
}
