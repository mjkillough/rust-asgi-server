extern crate rand;

use self::rand::{thread_rng, Rng};

pub mod redis;
pub use self::redis::RedisChannelLayer;


fn random_string(n: usize) -> String {
    thread_rng().gen_ascii_chars().take(n).collect()
}


pub trait ChannelLayer {
    fn send(&self, channel: &str, buf: &[u8]);
    // ASGI spec actually calls for a `receive(channels, block=True)`, but
    // I don't want to implement waiting on multiple channels or blocking
    // just yet.
    fn receive_one(&self, channel: &str) -> Vec<u8>;
    fn new_channel(&self, pattern: &str) -> String;
}
