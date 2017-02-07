extern crate rand;

use self::rand::{thread_rng, Rng};

pub mod redis;
pub use self::redis::RedisChannelLayer;

fn random_string(n: usize) -> String {
    thread_rng().gen_ascii_chars().take(n).collect()
}
