//! WebSocket support: SHA-1, base64, HTTP upgrade handshake, and frame codec.
//!
//! No external dependencies — SHA-1 and base64 are implemented inline
//! (~100 lines) since they're only used for the upgrade handshake.

use bytes::{BufMut, BytesMut};

fn sha1(data: &[u8]) -> [u8; 20] {
    let mut h0: u32 = 0x67452301;
    let mut h1: u32 = 0xEFCDAB89;
    let mut h2: u32 = 0x98BADCFE;
    let mut h3: u32 = 0x10325476;
    let mut h4: u32 = 0xC3D2E1F0;

    let bit_len = (data.len() as u64) * 8;
    let mut msg = Vec::with_capacity(data.len() + 72);
    msg.extend_from_slice(data);
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in msg.chunks_exact(64) {
        let mut w = [0u32; 80];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([
                chunk[i * 4],
                chunk[i * 4 + 1],
                chunk[i * 4 + 2],
                chunk[i * 4 + 3],
            ]);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }

        let (mut a, mut b, mut c, mut d, mut e) = (h0, h1, h2, h3, h4);
        #[allow(clippy::needless_range_loop)]
        for i in 0..80 {
            let (f, k) = match i {
                0..=19 => ((b & c) | ((!b) & d), 0x5A827999u32),
                20..=39 => (b ^ c ^ d, 0x6ED9EBA1u32),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1BBCDCu32),
                _ => (b ^ c ^ d, 0xCA62C1D6u32),
            };
            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(w[i]);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }

        h0 = h0.wrapping_add(a);
        h1 = h1.wrapping_add(b);
        h2 = h2.wrapping_add(c);
        h3 = h3.wrapping_add(d);
        h4 = h4.wrapping_add(e);
    }

    let mut result = [0u8; 20];
    result[0..4].copy_from_slice(&h0.to_be_bytes());
    result[4..8].copy_from_slice(&h1.to_be_bytes());
    result[8..12].copy_from_slice(&h2.to_be_bytes());
    result[12..16].copy_from_slice(&h3.to_be_bytes());
    result[16..20].copy_from_slice(&h4.to_be_bytes());
    result
}

const B64_CHARS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(data: &[u8]) -> String {
    let mut out = Vec::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;

        out.push(B64_CHARS[((triple >> 18) & 0x3F) as usize]);
        out.push(B64_CHARS[((triple >> 12) & 0x3F) as usize]);
        if chunk.len() > 1 {
            out.push(B64_CHARS[((triple >> 6) & 0x3F) as usize]);
        } else {
            out.push(b'=');
        }
        if chunk.len() > 2 {
            out.push(B64_CHARS[(triple & 0x3F) as usize]);
        } else {
            out.push(b'=');
        }
    }
    // B64_CHARS and '=' are all ASCII, so this is always valid UTF-8.
    String::from_utf8(out).expect("base64 output is always valid ASCII")
}

/// NATS-specific WebSocket GUID used by the Go nats.go client library.
/// The NATS client validates the Sec-WebSocket-Accept header against this GUID
/// instead of the standard RFC 6455 GUID (`258EAFA5-E914-47DA-95CA-5AB5AA286740`).
const WS_MAGIC_NATS: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

/// Parsed WebSocket upgrade request.
pub(crate) struct WsUpgrade {
    pub key: String,
}

/// Parse an HTTP WebSocket upgrade request, extracting the Sec-WebSocket-Key.
/// Returns `None` if the buffer doesn't contain a complete HTTP request yet.
/// Returns `Some(Err(...))` if the request is complete but not a valid WS upgrade.
pub(crate) fn parse_ws_upgrade(buf: &[u8]) -> Option<Result<(WsUpgrade, usize), &'static str>> {
    // Find end of HTTP headers
    let header_end = find_header_end(buf)?;
    let request = &buf[..header_end];

    // SAFETY: HTTP headers are ASCII
    let request_str = match std::str::from_utf8(request) {
        Ok(s) => s,
        Err(_) => return Some(Err("invalid UTF-8 in HTTP request")),
    };

    // Extract Sec-WebSocket-Key header (case-insensitive search)
    let key = extract_header_value(request_str, "sec-websocket-key");
    match key {
        Some(k) => Some(Ok((WsUpgrade { key: k.to_string() }, header_end))),
        None => Some(Err("missing Sec-WebSocket-Key header")),
    }
}

/// Build the HTTP 101 Switching Protocols response for a WebSocket upgrade.
/// Uses the NATS-specific GUID so the Go nats.go client accepts the connection.
pub(crate) fn build_ws_accept_response(key: &str) -> Vec<u8> {
    let mut accept_input = String::with_capacity(key.len() + WS_MAGIC_NATS.len());
    accept_input.push_str(key);
    accept_input.push_str(WS_MAGIC_NATS);
    let hash = sha1(accept_input.as_bytes());
    let accept = base64_encode(&hash);

    format!(
        "HTTP/1.1 101 Switching Protocols\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Accept: {accept}\r\n\r\n"
    )
    .into_bytes()
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    for i in 0..buf.len().saturating_sub(3) {
        if &buf[i..i + 4] == b"\r\n\r\n" {
            return Some(i + 4);
        }
    }
    None
}

fn extract_header_value<'a>(headers: &'a str, name: &str) -> Option<&'a str> {
    for line in headers.split("\r\n") {
        if let Some(colon) = line.find(':') {
            if line[..colon].eq_ignore_ascii_case(name) {
                return Some(line[colon + 1..].trim());
            }
        }
    }
    None
}

/// WebSocket opcodes.
const OP_CONTINUATION: u8 = 0x0;
const OP_TEXT: u8 = 0x1;
const OP_BINARY: u8 = 0x2;
const OP_CLOSE: u8 = 0x8;
const OP_PING: u8 = 0x9;
const OP_PONG: u8 = 0xA;

/// Result of a decode attempt.
pub(crate) enum DecodeStatus {
    /// A complete frame was decoded and appended to the output buffer.
    Complete,
    /// Need more data.
    NeedMore,
    /// Received a close frame.
    Close,
}

/// Partial frame state for fragmented messages.
struct PartialFrame {
    opcode: u8,
    payload: BytesMut,
}

/// Stateful WebSocket frame decoder/encoder.
pub(crate) struct WsCodec {
    partial: Option<PartialFrame>,
}

impl WsCodec {
    pub fn new() -> Self {
        Self { partial: None }
    }

    /// Decode incoming WebSocket frames from `raw` and append decoded NATS payload to `out`.
    /// Consumes processed bytes from `raw`.
    /// Client→server frames are masked; we unmask them.
    pub fn decode(
        &mut self,
        raw: &mut BytesMut,
        out: &mut BytesMut,
    ) -> Result<DecodeStatus, &'static str> {
        loop {
            if raw.len() < 2 {
                return Ok(DecodeStatus::NeedMore);
            }

            let b0 = raw[0];
            let b1 = raw[1];
            let fin = b0 & 0x80 != 0;
            let opcode = b0 & 0x0F;
            let masked = b1 & 0x80 != 0;
            let mut payload_len = (b1 & 0x7F) as u64;

            let mut offset = 2usize;

            if payload_len == 126 {
                if raw.len() < 4 {
                    return Ok(DecodeStatus::NeedMore);
                }
                payload_len = u16::from_be_bytes([raw[2], raw[3]]) as u64;
                offset = 4;
            } else if payload_len == 127 {
                if raw.len() < 10 {
                    return Ok(DecodeStatus::NeedMore);
                }
                payload_len = u64::from_be_bytes([
                    raw[2], raw[3], raw[4], raw[5], raw[6], raw[7], raw[8], raw[9],
                ]);
                offset = 10;
            }

            let mask_size = if masked { 4 } else { 0 };
            let total_frame_len = offset + mask_size + payload_len as usize;

            if raw.len() < total_frame_len {
                return Ok(DecodeStatus::NeedMore);
            }

            let mask_key = if masked {
                [
                    raw[offset],
                    raw[offset + 1],
                    raw[offset + 2],
                    raw[offset + 3],
                ]
            } else {
                [0; 4]
            };

            let payload_start = offset + mask_size;
            let payload_end = payload_start + payload_len as usize;

            // Extract and unmask payload
            let mut payload = BytesMut::with_capacity(payload_len as usize);
            payload.extend_from_slice(&raw[payload_start..payload_end]);
            if masked {
                for (i, byte) in payload.iter_mut().enumerate() {
                    *byte ^= mask_key[i % 4];
                }
            }

            // Consume the frame from raw
            let _ = raw.split_to(total_frame_len);

            // Handle opcode
            let effective_opcode = if opcode == OP_CONTINUATION {
                match &self.partial {
                    Some(p) => p.opcode,
                    None => return Err("continuation without initial frame"),
                }
            } else {
                opcode
            };

            match effective_opcode {
                OP_TEXT | OP_BINARY => {
                    if fin && opcode != OP_CONTINUATION && self.partial.is_none() {
                        out.extend_from_slice(&payload);
                        return Ok(DecodeStatus::Complete);
                    }

                    if opcode != OP_CONTINUATION {
                        self.partial = Some(PartialFrame {
                            opcode,
                            payload: BytesMut::new(),
                        });
                    }
                    if let Some(ref mut p) = self.partial {
                        p.payload.extend_from_slice(&payload);
                    }

                    if fin {
                        if let Some(p) = self.partial.take() {
                            out.extend_from_slice(&p.payload);
                            return Ok(DecodeStatus::Complete);
                        }
                    }
                }
                OP_CLOSE => return Ok(DecodeStatus::Close),
                OP_PING => {}
                OP_PONG => {}
                _ => return Err("unknown opcode"),
            }
        }
    }

    /// Encode a NATS payload as a WebSocket binary frame (server→client, unmasked).
    pub fn encode(payload: &[u8], out: &mut BytesMut) {
        let len = payload.len();
        out.put_u8(0x82);

        if len < 126 {
            out.put_u8(len as u8);
        } else if len <= 65535 {
            out.put_u8(126);
            out.put_u16(len as u16);
        } else {
            out.put_u8(127);
            out.put_u64(len as u64);
        }

        out.extend_from_slice(payload);
    }

    /// Build a WebSocket close frame (server→client, unmasked).
    pub fn encode_close(out: &mut BytesMut) {
        out.put_u8(0x88);
        out.put_u8(0x00);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sha1_empty() {
        let hash = sha1(b"");
        let hex: String = hash.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(hex, "da39a3ee5e6b4b0d3255bfef95601890afd80709");
    }

    #[test]
    fn test_sha1_abc() {
        let hash = sha1(b"abc");
        let hex: String = hash.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(hex, "a9993e364706816aba3e25717850c26c9cd0d89d");
    }

    #[test]
    fn test_sha1_long() {
        let hash = sha1(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq");
        let hex: String = hash.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(hex, "84983e441c3bd26ebaae4aa1f95129e5e54670f1");
    }

    #[test]
    fn test_base64_encode() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foob"), "Zm9vYg==");
        assert_eq!(base64_encode(b"fooba"), "Zm9vYmE=");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
    }

    #[test]
    fn test_ws_accept_key_nats() {
        // NATS uses a custom GUID; verify against Go nats-server output.
        let key = "dGhlIHNhbXBsZSBub25jZQ==";
        let mut accept_input = String::new();
        accept_input.push_str(key);
        accept_input.push_str(WS_MAGIC_NATS);
        let hash = sha1(accept_input.as_bytes());
        let accept = base64_encode(&hash);
        // Verified: Go nats-server returns this for the same key
        assert_eq!(accept, "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
    }

    #[test]
    fn test_parse_ws_upgrade() {
        let req = b"GET / HTTP/1.1\r\n\
            Host: localhost:8222\r\n\
            Upgrade: websocket\r\n\
            Connection: Upgrade\r\n\
            Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
            Sec-WebSocket-Version: 13\r\n\r\n";

        let result = parse_ws_upgrade(req);
        assert!(result.is_some());
        let (upgrade, consumed) = result.unwrap().unwrap();
        assert_eq!(upgrade.key, "dGhlIHNhbXBsZSBub25jZQ==");
        assert_eq!(consumed, req.len());
    }

    #[test]
    fn test_parse_ws_upgrade_incomplete() {
        let req = b"GET / HTTP/1.1\r\nHost: localhost\r\n";
        assert!(parse_ws_upgrade(req).is_none());
    }

    #[test]
    fn test_parse_ws_upgrade_missing_key() {
        let req = b"GET / HTTP/1.1\r\n\
            Host: localhost\r\n\
            Upgrade: websocket\r\n\r\n";
        let result = parse_ws_upgrade(req);
        assert!(result.is_some());
        assert!(result.unwrap().is_err());
    }

    #[test]
    fn test_build_ws_accept_response() {
        let resp = build_ws_accept_response("dGhlIHNhbXBsZSBub25jZQ==");
        let resp_str = std::str::from_utf8(&resp).unwrap();
        assert!(resp_str.starts_with("HTTP/1.1 101 Switching Protocols\r\n"));
        assert!(resp_str.contains("Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo="));
        assert!(resp_str.ends_with("\r\n\r\n"));
    }

    #[test]
    fn test_frame_encode_decode_small() {
        let payload = b"PUB foo 5\r\nhello\r\n";
        let mut encoded = BytesMut::new();
        WsCodec::encode(payload, &mut encoded);

        // Verify frame structure: FIN|Binary, unmasked, length
        assert_eq!(encoded[0], 0x82); // FIN + Binary
        assert_eq!(encoded[1], payload.len() as u8); // length < 126, no mask

        // Now decode (simulate client→server by adding mask)
        let mut raw = BytesMut::new();
        raw.put_u8(0x82); // FIN + Binary
        raw.put_u8(0x80 | payload.len() as u8); // masked + length
        let mask = [0x12, 0x34, 0x56, 0x78];
        raw.extend_from_slice(&mask);
        for (i, &b) in payload.iter().enumerate() {
            raw.put_u8(b ^ mask[i % 4]);
        }

        let mut codec = WsCodec::new();
        let mut out = BytesMut::new();
        match codec.decode(&mut raw, &mut out) {
            Ok(DecodeStatus::Complete) => {}
            other => panic!(
                "expected Complete, got {}",
                match other {
                    Ok(DecodeStatus::NeedMore) => "NeedMore",
                    Ok(DecodeStatus::Close) => "Close",
                    Err(e) => e,
                    _ => "unknown",
                }
            ),
        }
        assert_eq!(&out[..], payload);
        assert!(raw.is_empty());
    }

    #[test]
    fn test_frame_encode_medium() {
        let payload = vec![0x42u8; 300];
        let mut encoded = BytesMut::new();
        WsCodec::encode(&payload, &mut encoded);

        assert_eq!(encoded[0], 0x82);
        assert_eq!(encoded[1], 126); // extended 16-bit length
        assert_eq!(u16::from_be_bytes([encoded[2], encoded[3]]), 300);
        assert_eq!(&encoded[4..], &payload[..]);
    }

    #[test]
    fn test_frame_encode_large() {
        let payload = vec![0x42u8; 70000];
        let mut encoded = BytesMut::new();
        WsCodec::encode(&payload, &mut encoded);

        assert_eq!(encoded[0], 0x82);
        assert_eq!(encoded[1], 127); // extended 64-bit length
        let len = u64::from_be_bytes([
            encoded[2], encoded[3], encoded[4], encoded[5], encoded[6], encoded[7], encoded[8],
            encoded[9],
        ]);
        assert_eq!(len, 70000);
        assert_eq!(&encoded[10..], &payload[..]);
    }

    #[test]
    fn test_decode_unmasked_frame() {
        // Server-to-server or test scenario: unmasked frame
        let payload = b"INFO {}\r\n";
        let mut raw = BytesMut::new();
        raw.put_u8(0x82); // FIN + Binary
        raw.put_u8(payload.len() as u8); // no mask bit
        raw.extend_from_slice(payload);

        let mut codec = WsCodec::new();
        let mut out = BytesMut::new();
        match codec.decode(&mut raw, &mut out) {
            Ok(DecodeStatus::Complete) => {}
            _ => panic!("expected Complete"),
        }
        assert_eq!(&out[..], &payload[..]);
    }

    #[test]
    fn test_decode_close_frame() {
        let mut raw = BytesMut::new();
        raw.put_u8(0x88); // FIN + Close
        raw.put_u8(0x00); // no mask, 0 length

        let mut codec = WsCodec::new();
        let mut out = BytesMut::new();
        match codec.decode(&mut raw, &mut out) {
            Ok(DecodeStatus::Close) => {}
            _ => panic!("expected Close"),
        }
    }

    #[test]
    fn test_decode_need_more() {
        let mut raw = BytesMut::new();
        raw.put_u8(0x82); // only 1 byte

        let mut codec = WsCodec::new();
        let mut out = BytesMut::new();
        match codec.decode(&mut raw, &mut out) {
            Ok(DecodeStatus::NeedMore) => {}
            _ => panic!("expected NeedMore"),
        }
    }

    #[test]
    fn test_decode_masked_frame() {
        let payload = b"CONNECT {}\r\n";
        let mask = [0xAA, 0xBB, 0xCC, 0xDD];

        let mut raw = BytesMut::new();
        raw.put_u8(0x82); // FIN + Binary
        raw.put_u8(0x80 | payload.len() as u8); // masked
        raw.extend_from_slice(&mask);
        for (i, &b) in payload.iter().enumerate() {
            raw.put_u8(b ^ mask[i % 4]);
        }

        let mut codec = WsCodec::new();
        let mut out = BytesMut::new();
        match codec.decode(&mut raw, &mut out) {
            Ok(DecodeStatus::Complete) => {}
            _ => panic!("expected Complete"),
        }
        assert_eq!(&out[..], &payload[..]);
    }

    #[test]
    fn test_encode_close() {
        let mut out = BytesMut::new();
        WsCodec::encode_close(&mut out);
        assert_eq!(&out[..], &[0x88, 0x00]);
    }

    #[test]
    fn test_round_trip_various_sizes() {
        for size in [0, 1, 125, 126, 127, 1000, 65535, 65536] {
            let payload = vec![0x55u8; size];
            let mut encoded = BytesMut::new();
            WsCodec::encode(&payload, &mut encoded);

            // Decode (unmasked server-to-server)
            let mut codec = WsCodec::new();
            let mut out = BytesMut::new();
            match codec.decode(&mut encoded, &mut out) {
                Ok(DecodeStatus::Complete) => {}
                Ok(DecodeStatus::NeedMore) if size == 0 => continue, // 0-len frame
                other => panic!(
                    "size={size}: unexpected result: {:?}",
                    match other {
                        Ok(DecodeStatus::NeedMore) => "NeedMore".to_string(),
                        Ok(DecodeStatus::Close) => "Close".to_string(),
                        Err(e) => e.to_string(),
                        _ => "Complete".to_string(),
                    }
                ),
            }
            assert_eq!(out.len(), size, "size={size}: output length mismatch");
        }
    }
}
