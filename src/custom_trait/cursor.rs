use std::io::Cursor;

use tokio::io::AsyncReadExt;

pub trait AsyncReadVarint {
    async fn async_read_varint(&mut self) -> anyhow::Result<i64>;
    async fn async_read_uvarint(&mut self) -> anyhow::Result<u64>;
}
pub trait AsyncWriteVarint {
    async fn async_write_varint(&mut self, num: i64) -> anyhow::Result<()>;
    async fn async_write_uvarint(&mut self, num: u64) -> anyhow::Result<()>;
}

pub trait ReadVarint {
    fn read_varint(&mut self) -> anyhow::Result<i64>;
    fn read_uvarint(&mut self) -> anyhow::Result<u64>;
}

pub trait WriteVarint {
    fn write_varint(&mut self, num: i64);
    fn write_uvarint(&mut self, num: u64);
}

pub trait ReadUUID {
    async fn read_uuid(&mut self) -> anyhow::Result<uuid::Uuid>;
}

impl AsyncReadVarint for Cursor<&Vec<u8>> {
    async fn async_read_varint(&mut self) -> anyhow::Result<i64> {
        // Step 1: Decode varint to get the unsigned value
        let unsigned_value = self.async_read_uvarint().await?;

        // Step 2: Apply ZigZag decoding to get the signed value
        let signed_value = zigzag_decode(unsigned_value);
        Ok(signed_value)
    }

    async fn async_read_uvarint(&mut self) -> anyhow::Result<u64> {
        let mut value = 0u64;
        let mut shift = 0;

        loop {
            let mut buffer = [0u8; 1];
            self.read_exact(&mut buffer).await?;

            let byte = buffer[0];
            value |= ((byte & 0x7F) as u64) << shift;

            // If the MSB is not set, we've reached the end of the varint.
            if byte & 0x80 == 0 {
                break;
            }

            shift += 7;
        }

        Ok(value)
    }
}

impl AsyncWriteVarint for Vec<u8> {
    async fn async_write_varint(&mut self, num: i64) -> anyhow::Result<()> {
        let mut n = num;
        let mut result = Vec::new();

        // Zig-zag encode the signed integer
        n = (n << 1) ^ (n >> 63); // Apply zig-zag encoding

        while (n & !0x7F) != 0 {
            result.push(((n & 0x7F) | 0x80) as u8); // Push 7 bits with continuation bit
            n >>= 7;
        }

        result.push(n as u8); // Push the last byte

        // Extend the result into the original vector
        self.extend(result);

        Ok(())
    }

    async fn async_write_uvarint(&mut self, num: u64) -> anyhow::Result<()> {
        let mut n = num;
        let mut result = Vec::new();

        while (n & !0x7F) != 0 {
            result.push(((n & 0x7F) | 0x80) as u8); // Push 7 bits with continuation bit
            n >>= 7;
        }

        result.push(n as u8); // Push the last byte

        // Extend the result into the original vector
        self.extend(result);

        Ok(())
    }
}

impl ReadVarint for Cursor<&Vec<u8>> {
    fn read_varint(&mut self) -> anyhow::Result<i64> {
        // Step 1: Decode varint to get the unsigned value
        let unsigned_value = self.read_uvarint()?;

        // Step 2: Apply ZigZag decoding to get the signed value
        let signed_value = zigzag_decode(unsigned_value);
        Ok(signed_value)
    }

    fn read_uvarint(&mut self) -> anyhow::Result<u64> {
        let mut value = 0u64;
        let mut shift = 0;

        loop {
            let mut buffer = [0u8; 1];
            self.read_exact(&mut buffer);

            let byte = buffer[0];
            value |= ((byte & 0x7F) as u64) << shift;

            // If the MSB is not set, we've reached the end of the varint.
            if byte & 0x80 == 0 {
                break;
            }

            shift += 7;
        }

        Ok(value)
    }
}

impl WriteVarint for Vec<u8> {
    fn write_varint(&mut self, num: i64) {
        let mut n = num;
        let mut result = Vec::new();

        // Zig-zag encode the signed integer
        n = (n << 1) ^ (n >> 63); // Apply zig-zag encoding

        while (n & !0x7F) != 0 {
            result.push(((n & 0x7F) | 0x80) as u8); // Push 7 bits with continuation bit
            n >>= 7;
        }

        result.push(n as u8); // Push the last byte

        // Extend the result into the original vector
        self.extend(result);
    }

    fn write_uvarint(&mut self, num: u64) {
        let mut n = num;
        let mut result = Vec::new();

        while (n & !0x7F) != 0 {
            result.push(((n & 0x7F) | 0x80) as u8); // Push 7 bits with continuation bit
            n >>= 7;
        }

        result.push(n as u8); // Push the last byte

        println!("writing for {}", num);
        println!("get result {:?}", result);

        // Extend the result into the original vector
        self.extend(result);
    }
}

impl ReadUUID for Cursor<&Vec<u8>> {
    async fn read_uuid(&mut self) -> anyhow::Result<uuid::Uuid> {
        let bytes = self.read_u128().await?;
        let result = uuid::Uuid::from_u128(bytes);
        Ok(result)
    }
}

fn zigzag_decode(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn read_uuid() -> anyhow::Result<()> {
        let data: Vec<u8> = vec![
            0xa0, 0xe9, 0xcc, 0xc6, // First 4 bytes
            0x6e, 0x0a, // Next 2 bytes
            0x47, 0xe5, // Next 2 bytes
            0x81, 0xd4, // Next 2 bytes
            0xf1, 0x2d, 0x93, 0x42, 0xcc, 0x7e, // Last 6 bytes
        ];

        let mut cursor = Cursor::new(&data);

        let res = cursor.read_uuid().await?;
        assert_eq!(
            res.to_string(),
            "a0e9ccc6-6e0a-47e5-81d4-f12d9342cc7e".to_string()
        );
        Ok(())
    }

    #[tokio::test]
    async fn read_uvarint() -> anyhow::Result<()> {
        let data = vec![0xAC, 0x02];
        let mut cursor = Cursor::new(&data);
        let result = cursor.async_read_uvarint().await?;
        assert_eq!(result, 300);

        let data = vec![0x04];
        let mut cursor = Cursor::new(&data);
        let result = cursor.async_read_uvarint().await?;
        assert_eq!(result, 4);

        let data = vec![0x12];
        let mut cursor = Cursor::new(&data);
        let result = cursor.async_read_uvarint().await?;
        assert_eq!(result, 18);
        Ok(())
    }

    #[tokio::test]
    async fn read_varint() -> anyhow::Result<()> {
        let data = vec![0x30];
        let mut cursor = Cursor::new(&data);
        let result = cursor.async_read_varint().await?;
        assert_eq!(result, 24);

        let data = vec![0x01];
        let mut cursor = Cursor::new(&data);
        let result = cursor.async_read_varint().await?;
        assert_eq!(result, -1);

        let data = vec![0x3a];
        let mut cursor = Cursor::new(&data);
        let result = cursor.async_read_varint().await?;
        assert_eq!(result, 29);

        let data = vec![0x02];
        let mut cursor = Cursor::new(&data);
        let result = cursor.async_read_uvarint().await?;
        assert_eq!(result, 2);
        Ok(())
    }
}
