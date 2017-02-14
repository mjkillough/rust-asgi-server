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


pub trait ChannelLayer {
    type Error;

    fn send<S: Serialize>(&self, channel: &str, msg: &S) -> Result<(), Self::Error>;
    fn receive<D: Deserialize>(&self,
                               channels: &[&str],
                               block: bool)
                               -> Result<Option<(String, D)>, Self::Error>;
    fn new_channel(&self, pattern: &str) -> Result<String, Self::Error>;
}
