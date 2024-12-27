use bytes::BufMut;

use crate::protocol::{request::Request, response::Response};

pub fn handle(req: &Request, res: &mut Response) {
    if req.request_api_version <= 4 {
        res.body.put_u16(0); // error code
        res.body.put_u8(3); // array length + 1

        res.body.put_u16(18); // api key
        res.body.put_u16(0); // min version
        res.body.put_u16(4); // max version
        res.body.put_u8(0); // TAG_BUFFER length

        res.body.put_u16(75); // api key
        res.body.put_u16(0); // min version
        res.body.put_u16(4); // max version
        res.body.put_u8(0); // TAG_BUFFER length

        res.body.put_u32(0); // Throttle time
        res.body.put_u8(0); // TAG_BUFFER length
    } else {
        res.body.put_u16(35);
    }
}
