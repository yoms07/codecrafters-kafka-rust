use bytes::BufMut;

use crate::protocol::{request::Request, response::Response};

pub fn handle(req: &Request, res: &mut Response) {
    if req.request_api_version == 16 {
        res.body.put_u32(0x00); // throttle time
        res.body.put_u16(0x00); // error code
        res.body.put_u32(0x00); // session_id

        res.body.put_u8(0x00); // num response
        res.body.put_u8(0x00); // tag buffer
    }
}
