use std::io::Write;
use std::time::Duration;

use redis;
use redis::Commands;
use rmp_serde::encode::VariantWriter;
use rmp;
use rmp::Marker;
use rmp::encode::ValueWriteError;
use rmp_serde;
use serde;
use serde::{Deserialize, Serialize};

use super::{random_string, shuffle, ChannelLayer};


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
    conn: redis::Connection,

    prefix: String,
    expiry: Duration,
    blpop_timeout: Duration,

    lpopmany: redis::Script,
}

impl RedisChannelLayer {
    pub fn new() -> RedisChannelLayer {
        let client = redis::Client::open("redis://127.0.0.1/").unwrap();
        let conn = client.get_connection().unwrap();

        let lpopmany = redis::Script::new(r"
            for keyCount = 1, #KEYS do
                local result = redis.call('LPOP', KEYS[keyCount])
                if result then
                    return {KEYS[keyCount], result}
                end
            end
            return nil
        ");

        RedisChannelLayer {
            conn: conn,

            prefix: "asgi:".to_owned(),
            expiry: Duration::from_secs(60),
            blpop_timeout: Duration::from_secs(5),

            lpopmany: lpopmany,
        }
    }
}

impl ChannelLayer for RedisChannelLayer {
    fn send<S: Serialize>(&self, channel: &str, msg: &S) {
        let message_key = self.prefix.to_owned() + "msg:" + &random_string(10);
        let channel_key = self.prefix.to_owned() + channel;

        let buf = msgpack_serialize(msg).unwrap();

        let message_expiry = self.expiry.as_secs() as usize;
        let channel_expiry = (self.expiry.as_secs() + 1) as usize;

        // TODO: Check the channel isn't full.
        let _: () = self.conn.set(&message_key, buf).unwrap();
        let _: () = self.conn.expire(&message_key, message_expiry).unwrap();
        let _: () = self.conn.rpush(&channel_key, message_key).unwrap();
        let _: () = self.conn.expire(&channel_key, channel_expiry).unwrap();
    }

    fn receive<D: Deserialize>(&self, channels: &[&str], block: bool) -> Option<(String, D)> {
        loop {
            let mut channels: Vec<String> = channels.iter()
                .map(|channel| self.prefix.to_owned() + channel)
                .collect();

            // Prevent one channel from starving the others.
            shuffle(channels.as_mut_slice());

            let result: Option<(String, String)> = match block {
                true => {
                    let mut cmd = redis::cmd("BLPOP");
                    for channel in channels {
                        cmd.arg(channel);
                    }
                    cmd.arg(self.blpop_timeout.as_secs());
                    cmd.query(&self.conn).unwrap()
                }
                false => {
                    let mut script = self.lpopmany.prepare_invoke();
                    for channel in channels {
                        script.arg(channel);
                    }
                    script.invoke(&self.conn).unwrap()
                }
            };

            match result {
                Some((channel_name, message_key)) => {
                    let message: Option<Vec<u8>> = self.conn.get(&message_key).unwrap();
                    match message {
                        Some(buf) => {
                            // Remove prefix from returned channel name.
                            let channel_name = channel_name[self.prefix.len()..].to_owned();
                            return Some((channel_name, msgpack_deserialize(&buf).unwrap()));
                        }
                        // If the message has expired, move on to the next available channel.
                        None => {}
                    }
                }
                // If the channels didn't return any in the time available, return nothing.
                None => return None,
            }
        }
    }

    fn new_channel(&self, pattern: &str) -> String {
        // TODO: Check pattern ends in ! or ?
        // TODO: Check the new channel doesn't already exist.
        pattern.to_owned() + &random_string(10)
    }
}
