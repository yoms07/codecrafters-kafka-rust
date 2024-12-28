use bytes::BufMut;

use crate::protocol::{request::Request, response::Response};

struct SupportedAPI {
    pub api_key: u16,
    pub min_version: u16,
    pub max_version: u16,
}

static SUPPORTED_APIS: &[SupportedAPI] = &[
    SupportedAPI {
        api_key: 18,
        min_version: 0,
        max_version: 4,
    },
    SupportedAPI {
        api_key: 75,
        min_version: 0,
        max_version: 4,
    },
    SupportedAPI {
        api_key: 1,
        min_version: 0,
        max_version: 16,
    },
];

pub fn handle(req: &Request, res: &mut Response) {
    if req.request_api_version <= 4 {
        res.body.put_u16(0); // error code
        res.body.put_u8((SUPPORTED_APIS.len() + 1) as u8); // array length + 1

        for api in SUPPORTED_APIS {
            res.body.put_u16(api.api_key); // api key
            res.body.put_u16(api.min_version); // min version
            res.body.put_u16(api.max_version); // max version
            res.body.put_u8(0); // TAG_BUFFER length
        }
        res.body.put_u32(0); // Throttle time
        res.body.put_u8(0); // TAG_BUFFER length
    } else {
        res.body.put_u16(35);
    }
}
