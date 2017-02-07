extern crate redis;

use super::{random_string, ChannelLayer};
use self::redis::Commands;


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
    fn send(&self, channel: &str, buf: &[u8]) {
        let message_key = self.prefix.to_owned() + "msg:" + &random_string(10);
        let channel_key = self.prefix.to_owned() + channel;

        // TODO: Check the channel isn't full.
        let _: () = self.conn.set(&message_key, buf).unwrap();
        let _: () = self.conn.expire(&message_key, self.expiry).unwrap();
        let _: () = self.conn.rpush(&channel_key, message_key).unwrap();
        let _: () = self.conn.expire(&channel_key, self.expiry + 1).unwrap();
    }

    fn receive_one(&self, channel: &str) -> Vec<u8> {
        let channel_key = self.prefix.to_owned() + channel;
        let (_, message_key): ((), String) = self.conn.blpop(&channel_key, 0).unwrap();
        self.conn.get(&message_key).unwrap()
    }

    fn new_channel(&self, pattern: &str) -> String {
        // TODO: Check pattern ends in ! or ?
        // TODO: Check the new channel doesn't already exist.
        pattern.to_owned() + &random_string(10)
    }
}
