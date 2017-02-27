use std::io::Write;
use std::time::Duration;

use r2d2;
use redis;
use redis::{ConnectionInfo, Commands, IntoConnectionInfo};
use rmp_serde::encode::VariantWriter;
use rmp;
use rmp::Marker;
use rmp::encode::ValueWriteError;
use rmp_serde;
use serde;
use serde::{Deserialize, Serialize};

use super::{random_string, shuffle, validate_channel_name, ChannelError, ChannelLayer, ChannelReply};


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


fn msgpack_serialize<S: Serialize>(val: &S) -> Result<Vec<u8>, rmp_serde::encode::Error> {
    let mut buf = Vec::new();
    {
        // Create a Serializer using our custom RmpStructMapWriter.
        let mut serializer = rmp_serde::Serializer::with(&mut buf, RmpStructMapWriter);
        val.serialize(&mut serializer)?;
    }
    Ok(buf)
}

fn msgpack_deserialize<D: Deserialize>(buf: &[u8]) -> Result<D, rmp_serde::decode::Error> {
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
    pub fn new<I>(info: I) -> Result<Self, ChannelError>
        where I: IntoConnectionInfo
    {
        let client = redis::Client::open(info)?;
        let conn = client.get_connection()?;

        // This script comes from asgi_redis:
        let lpopmany = redis::Script::new(r"
            for keyCount = 1, #KEYS do
                local result = redis.call('LPOP', KEYS[keyCount])
                if result then
                    return {KEYS[keyCount], result}
                end
            end
            return nil
        ");

        Ok(RedisChannelLayer {
            conn: conn,

            prefix: "asgi:".to_owned(),
            expiry: Duration::from_secs(60),
            blpop_timeout: Duration::from_secs(5),

            lpopmany: lpopmany,
        })
    }
}

impl ChannelLayer for RedisChannelLayer {
    type Manager = RedisChannelLayerManager;

    fn send<S: Serialize>(&self, channel: &str, msg: &S) -> Result<(), ChannelError> {
        validate_channel_name(channel)?;

        let message_key = self.prefix.to_owned() + "msg:" + &random_string(10);
        let channel_key = self.prefix.to_owned() + channel;

        let buf = msgpack_serialize(msg).unwrap();

        let message_expiry = self.expiry.as_secs() as usize;
        let channel_expiry = (self.expiry.as_secs() + 1) as usize;

        // TODO: Check the channel isn't full.
        self.conn.set(&message_key, buf)?;
        self.conn.expire(&message_key, message_expiry)?;
        self.conn.rpush(&channel_key, message_key)?;
        self.conn.expire(&channel_key, channel_expiry)?;

        Ok(())
    }

    fn receive<'a, I>(&self,
                      channels: I,
                      block: bool)
                      -> Result<Option<(String, ChannelReply)>, ChannelError>
        where I: Iterator<Item = &'a String> + Clone
    {
        let valid_channel_names: Result<Vec<()>, ChannelError> = channels.clone()
            .map(String::as_ref)
            .map(validate_channel_name)
            .collect();
        valid_channel_names?;

        loop {
            let mut channels: Vec<String> = channels.clone()
                .map(|channel| self.prefix.to_owned() + &channel)
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
                    cmd.query(&self.conn)?
                }
                false => {
                    let mut script = self.lpopmany.prepare_invoke();
                    for channel in channels {
                        script.key(channel);
                    }
                    script.invoke(&self.conn)?
                }
            };

            match result {
                Some((channel_name, message_key)) => {
                    let message: Option<Vec<u8>> = self.conn.get(&message_key)?;
                    match message {
                        Some(buf) => {
                            // Remove prefix from returned channel name.
                            let channel_name = channel_name[self.prefix.len()..].to_owned();
                            let reply = ChannelReply { buf: buf };
                            return Ok(Some((channel_name, reply)));
                        }
                        // If the message has expired, move on to the next available channel.
                        None => {}
                    }
                }
                // If the channels didn't return any in the time available, return nothing.
                None => return Ok(None),
            }
        }
    }

    fn deserialize<D: Deserialize>(reply: ChannelReply) -> Result<D, ChannelError> {
        Ok(msgpack_deserialize(&reply.buf)?)
    }

    fn new_channel(&self, pattern: &str) -> Result<String, ChannelError> {
        validate_channel_name(pattern)?;
        if !pattern.ends_with("!") && !pattern.ends_with("?") {
            return Err(ChannelError::InvalidChannelName);
        }

        // TODO: Check the new channel doesn't already exist.
        Ok(pattern.to_owned() + &random_string(10))
    }
}


#[derive(Debug)]
pub struct RedisChannelLayerManager {
    info: ConnectionInfo,
}

impl RedisChannelLayerManager {
    pub fn new<I>(info: I) -> Result<Self, ChannelError>
        where I: IntoConnectionInfo
    {
        Ok(RedisChannelLayerManager { info: info.into_connection_info()? })
    }
}

impl r2d2::ManageConnection for RedisChannelLayerManager {
    type Connection = RedisChannelLayer;
    type Error = ChannelError;

    fn connect(&self) -> Result<Self::Connection, ChannelError> {
        Ok(RedisChannelLayer::new(self.info.clone())?)
    }

    fn is_valid(&self, channel_layer: &mut Self::Connection) -> Result<(), ChannelError> {
        Ok(redis::cmd("PING").query(&channel_layer.conn)?)
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}


// Our channel layer specific error types. We don't expect the user will be able to do much with
// these, so we don't feel too bad erasing the type. We may later want to parse the useful bits
// of the error out into something ChannelError understands.
impl From<redis::RedisError> for ChannelError {
    fn from(err: redis::RedisError) -> ChannelError {
        ChannelError::Transport(Box::new(err))
    }
}

impl From<rmp_serde::encode::Error> for ChannelError {
    fn from(err: rmp_serde::encode::Error) -> ChannelError {
        ChannelError::Serialize(Box::new(err))
    }
}

impl From<rmp_serde::decode::Error> for ChannelError {
    fn from(err: rmp_serde::decode::Error) -> ChannelError {
        ChannelError::Deserialize(Box::new(err))
    }
}
