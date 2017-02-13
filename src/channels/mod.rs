use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};

pub mod redis;
pub use self::redis::RedisChannelLayer;


fn random_string(n: usize) -> String {
    thread_rng().gen_ascii_chars().take(n).collect()
}

fn shuffle<T>(values: &mut [T]) {
    thread_rng().shuffle(values)
}


pub trait ChannelLayer {
    fn send<S: Serialize>(&self, channel: &str, msg: &S);
    fn receive<D: Deserialize>(&self, channels: &[&str], block: bool) -> Option<(String, D)>;
    fn new_channel(&self, pattern: &str) -> String;
}
