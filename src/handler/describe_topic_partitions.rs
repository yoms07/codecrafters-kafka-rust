use std::array;

use bytes::{Buf, BufMut, Bytes};

use crate::protocol::{request::Request, response::Response};

pub fn handle(req: &Request, res: &mut Response) {
    println!("sampe sini");
    if req.request_api_version == 0 {
        let mut data = req.data.as_slice();

        data.copy_to_bytes(1);
        let array_length = (data.get_u8() - 1) as usize;
        let mut topics: Vec<Bytes> = Vec::new();

        for _ in 0..array_length {
            let topic_name_length = (data.get_u8() - 1) as usize;
            let topic_name = data.copy_to_bytes(topic_name_length);
            println!(
                "Topic name: {}",
                String::from_utf8(topic_name.to_vec()).expect("Hello")
            );
            topics.push(topic_name);
        }

        let response_partition_limit = data.copy_to_bytes(4);
        let cursor = data.get_u8();
        data.copy_to_bytes(1); // TAG_BUFFER

        res.body.put_u8(0); // TAG_BUFFER length
        res.body.put_u32(0); // Throttle time
        res.body.put_u8((array_length + 1) as u8); // TAG_BUFFER length
        res.body.put_u16(3); // error code

        for topic in topics {
            res.body.put_u8((topic.len() + 1) as u8);
            res.body.put(topic);
            res.body.put_bytes(0, 16); // uuid topic id
        }
        res.body.put_u8(0); // is internal
        res.body.put_u8(1); // partition array length
        res.body.put_slice(&[0x0, 0x0, 0x0d, 0xf8]);
        res.body.put_u8(0); // TAG_BUFFER length
        res.body.put_u8(0xff); // next cursor
        res.body.put_u8(0); // TAG_BUFFER length
    } else {
        res.body.put_u16(35);
    }
}
