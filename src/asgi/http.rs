extern crate rmp;

use std::error::Error;
use std::io::{Cursor, Read, Write};

use super::{AsgiDeserialize, AsgiSerialize};


type HeaderTuple = (Vec<u8>, Vec<u8>);

#[derive(Debug)]
pub struct Request<'a> {
    pub reply_channel: &'a str,
    pub http_version: &'a str,
    pub method: &'a str,
    /* pub scheme: String, */
    pub path: &'a str,
    pub query_string: &'a str,
    /* pub root_path: String, */
    pub headers: Vec<HeaderTuple>,
    pub body: Vec<u8>,
    /* pub body_channel: ?, */
    /* pub client: ?, */
    /* pub server: ?, */
}

impl<'a> AsgiSerialize for Request<'a> {
    fn serialize<W: Write>(&self, mut buf: &mut W) -> Result<(), Box<Error>> {
        rmp::encode::write_map_len(&mut buf, 7)?;
        rmp::encode::write_str(&mut buf, "reply_channel")?;
        rmp::encode::write_str(&mut buf, &self.reply_channel)?;
        rmp::encode::write_str(&mut buf, "http_version")?;
        rmp::encode::write_str(&mut buf, &self.http_version)?;
        rmp::encode::write_str(&mut buf, "method")?;
        rmp::encode::write_str(&mut buf, &self.method)?;
        rmp::encode::write_str(&mut buf, "path")?;
        rmp::encode::write_str(&mut buf, &self.path)?;
        rmp::encode::write_str(&mut buf, "query_string")?;
        rmp::encode::write_str(&mut buf, &self.query_string)?;

        rmp::encode::write_str(&mut buf, "headers")?;
        rmp::encode::write_array_len(&mut buf, self.headers.len() as u32)?;
        for &(ref name, ref value) in &self.headers {
            rmp::encode::write_array_len(&mut buf, 2)?;
            rmp::encode::write_bin(&mut buf, &name)?;
            rmp::encode::write_bin(&mut buf, &value)?;
        }

        rmp::encode::write_str(&mut buf, "body")?;
        rmp::encode::write_bin(&mut buf, &self.body)?;
        Ok(())
    }
}

/*
status: Integer HTTP status code.
headers: A list of [name, value] pairs, where name is the byte string header name, and value is the byte string header value. Order should be preserved in the HTTP response. Header names must be lowercased.
content: Byte string of HTTP body content. Optional, defaults to empty string.
more_content: Boolean value signifying if there is additional content to come (as part of a Response Chunk message). If False, response will be taken as complete and closed off, and any further messages on the channel will be ignored. Optional, defaults to False.
*/

#[derive(Debug)]
pub struct Response {
    pub status: u16,
    pub headers: Vec<HeaderTuple>,
    pub content: Vec<u8>,
    pub more_content: bool,
}

impl Response {
    fn new() -> Self {
        Response {
            status: 0,
            headers: Vec::new(),
            content: Vec::new(),
            more_content: false,
        }
    }
}

fn msgpack_read_bin<R: Read>(mut buf: &mut R) -> Result<Vec<u8>, Box<Error>> {
    let len = rmp::decode::read_bin_len(&mut buf)?;
    let mut out_buf = vec![0; len as usize];
    buf.read_exact(&mut out_buf);
    Ok(out_buf)
}

fn msgpack_read_string<R: Read>(mut buf: &mut R) -> Result<String, Box<Error>> {
    let len = rmp::decode::read_str_len(&mut buf)?;
    let mut out_buf = vec![0; len as usize];
    buf.read_exact(&mut out_buf)?;
    Ok(String::from_utf8(out_buf)?)
}


impl AsgiDeserialize for Response {
    fn deserialize(buf: &Vec<u8>) -> Result<Self, Box<Error>> {
        let mut resp = Response::new();
        let mut cur = Cursor::new(buf);
        let num_elements = rmp::decode::read_map_len(&mut cur)?;
        for _ in 0..num_elements {
            let name = msgpack_read_string(&mut cur)?;
            match name.as_ref() {
                "status" => resp.status = rmp::decode::read_int(&mut cur)?,
                "content" => resp.content = msgpack_read_bin(&mut cur)?,
                "more_content" => resp.more_content = rmp::decode::read_bool(&mut cur)?,
                "headers" => {
                    let num_headers = rmp::decode::read_array_len(&mut cur)?;
                    for _ in 0..num_headers {
                        let tuple_len = rmp::decode::read_array_len(&mut cur)?;
                        assert!(tuple_len == 2);
                        resp.headers.push((msgpack_read_bin(&mut cur)?, msgpack_read_bin(&mut cur)?))
                    }
                }
                _ => (), // don't care about unsupported members
            }
        }
        Ok(resp)
    }
}