use std;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};

pub mod redis;
pub use self::redis::{RedisChannelLayer, RedisChannelError};


fn random_string(n: usize) -> String {
    thread_rng().gen_ascii_chars().take(n).collect()
}

fn shuffle<T>(values: &mut [T]) {
    thread_rng().shuffle(values)
}


pub struct ChannelReply {
    pub buf: Vec<u8>,
}


pub trait ChannelLayer {
    type Error: std::error::Error;

    fn send<S: Serialize>(&self, channel: &str, msg: &S) -> Result<(), Self::Error>;
    fn receive<'a, I>(&self,
                      channels: I,
                      block: bool)
                      -> Result<Option<(String, ChannelReply)>, Self::Error>
        where I: Iterator<Item = &'a String> + Clone;
    fn deserialize<D: Deserialize>(reply: ChannelReply) -> Result<D, Self::Error>;
    fn new_channel(&self, pattern: &str) -> Result<String, Self::Error>;
}
