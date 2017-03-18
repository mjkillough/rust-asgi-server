// We need to wrap Vec<u8>/&[u8] in this in order to make sure serde
// serializes it as a byte string rather than a list of bytes.
use serde::bytes::{ByteBuf, Bytes};


#[derive(Debug, Serialize)]
pub struct Request<'a> {
    pub reply_channel: &'a str,
    pub http_version: &'a str,
    pub method: &'a str,
    pub scheme: &'a str,
    pub path: &'a str,
    pub query_string: &'a str,
    // It'd be nice if headers didn't have to own their byte-strings. See #5.
    pub headers: Vec<(ByteBuf, ByteBuf)>,
    pub body: Bytes<'a>,
    pub body_channel: Option<&'a str>,
    // Again, it would be nice if we didn't have to own a String, but hey-ho.
    pub client: Option<(String, u16)>,
    pub server: (String, u16),
}

#[derive(Debug, Serialize)]
pub struct RequestBodyChunk<'a> {
    pub content: Bytes<'a>,
    pub closed: bool,
    pub more_content: bool,
}


#[derive(Debug, Deserialize)]
pub struct Response {
    pub status: u16,
    pub headers: Vec<(ByteBuf, ByteBuf)>,
    pub content: ByteBuf,
    pub more_content: bool,
}

#[derive(Debug, Deserialize)]
pub struct ResponseBodyChunk {
    #[serde(default)]
    pub content: ByteBuf,
    pub more_content: bool,
}
