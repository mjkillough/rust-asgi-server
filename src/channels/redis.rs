extern crate redis;
extern crate rmp_serde;
extern crate rmp;
extern crate serde;

use std::io::Write;

use super::{random_string, ChannelLayer};

use self::redis::Commands;
use self::rmp_serde::{Deserializer, Serializer};
use self::rmp_serde::encode::VariantWriter;
use self::rmp::Marker;
use self::rmp::encode::ValueWriteError;
use self::serde::{Deserialize, Serialize};


// asgi_redis expects msgpack map objects, which it'll deserialize to Python dicts.
// We want to represent them as structs in Rust, but rmp-serde will serialize structs
// to msgpack arrays by default. This custom VariantWriter impl is used by rmp-serde's
// serde::Serializer to instead represent them as maps.
struct RmpStructMapWriter;

impl VariantWriter for RmpStructMapWriter {
    fn write_struct_len<W>(&self, wr: &mut W, len: u32) -> Result<Marker, ValueWriteError>
        where W: Write
    {
        rmp::encode::write_map_len(wr, len)
    }

    fn write_field_name<W>(&self, wr: &mut W, key: &str) -> Result<(), ValueWriteError>
        where W: Write
    {
        rmp::encode::write_str(wr, key)
    }
}


fn msgpack_serialize<S: Serialize>(val: &S) -> Result<Vec<u8>, self::rmp_serde::encode::Error> {
    let mut buf = Vec::new();
    {
        // Create a Serializer using our custom RmpStructMapWriter.
        let mut serializer = rmp_serde::Serializer::with(&mut buf, RmpStructMapWriter);
        val.serialize(&mut serializer)?;
    }
    Ok(buf)
}

fn msgpack_deserialize<D: Deserialize>(buf: &[u8]) -> Result<D, self::rmp_serde::decode::Error> {
    // We don't have to do anything fancy here - rmp_serde will convert a msgpack map to
    // a Rust strut just fine.
    let mut deserializer = rmp_serde::Deserializer::new(buf);
    serde::Deserialize::deserialize(&mut deserializer)
}


pub struct RedisChannelLayer {
    client: redis::Client,
    conn: redis::Connection,

    prefix: String,
    expiry: usize, // seconds
}

impl RedisChannelLayer {
    pub fn new() -> RedisChannelLayer {
        let client = redis::Client::open("redis://127.0.0.1/").unwrap();
        let conn = client.get_connection().unwrap();

        RedisChannelLayer {
            client: client,
            conn: conn,
            prefix: "asgi:".to_owned(),
            expiry: 60, // seconds
        }
    }
}

impl ChannelLayer for RedisChannelLayer {
    fn send<S: Serialize>(&self, channel: &str, msg: &S) {
        let message_key = self.prefix.to_owned() + "msg:" + &random_string(10);
        let channel_key = self.prefix.to_owned() + channel;

        let buf = msgpack_serialize(msg).unwrap();

        // TODO: Check the channel isn't full.
        let _: () = self.conn.set(&message_key, buf).unwrap();
        let _: () = self.conn.expire(&message_key, self.expiry).unwrap();
        let _: () = self.conn.rpush(&channel_key, message_key).unwrap();
        let _: () = self.conn.expire(&channel_key, self.expiry + 1).unwrap();
    }

    fn receive_one<D: Deserialize>(&self, channel: &str) -> D {
        let channel_key = self.prefix.to_owned() + channel;
        let (_, message_key): ((), String) = self.conn.blpop(&channel_key, 0).unwrap();
        let message: Vec<u8> = self.conn.get(&message_key).unwrap();
        msgpack_deserialize(&message).unwrap()
    }

    fn new_channel(&self, pattern: &str) -> String {
        // TODO: Check pattern ends in ! or ?
        // TODO: Check the new channel doesn't already exist.
        pattern.to_owned() + &random_string(10)
    }
}
