extern crate rand;
extern crate serde;

use self::rand::{thread_rng, Rng};
use self::serde::{Deserialize, Serialize};

pub mod redis;
pub use self::redis::RedisChannelLayer;


fn random_string(n: usize) -> String {
    thread_rng().gen_ascii_chars().take(n).collect()
}


pub trait ChannelLayer {
    fn send<S: Serialize>(&self, channel: &str, msg: &S);
    // ASGI spec actually calls for a `receive(channels, block=True)`, but
    // I don't want to implement waiting on multiple channels or blocking
    // just yet.
    fn receive_one<D: Deserialize>(&self, channel: &str) -> D;
    fn new_channel(&self, pattern: &str) -> String;
}
