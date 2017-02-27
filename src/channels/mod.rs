use std;
use std::ascii::AsciiExt;
use std::error::Error;

use r2d2;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};

pub mod redis;
pub mod reply_pump;
pub use self::redis::{RedisChannelLayer, RedisChannelLayerManager};
pub use self::reply_pump::ReplyPump;


fn random_string(n: usize) -> String {
    thread_rng().gen_ascii_chars().take(n).collect()
}

fn shuffle<T>(values: &mut [T]) {
    thread_rng().shuffle(values)
}

fn is_valid_channel_name(name: &str) -> bool {
    let too_long = name.len() >= 100;
    let invalid_chars = name.chars().all(|c| {
        c.is_ascii() &&
        (c.is_alphanumeric() || ".-_?!".contains(c))
    });
    too_long || invalid_chars
}

fn validate_channel_name(name: &str) -> Result<(), ChannelError> {
    if is_valid_channel_name(name) {
        Ok(())
    } else {
        Err(ChannelError::InvalidChannelName)
    }
}


pub struct ChannelReply {
    pub buf: Vec<u8>,
}


pub trait ChannelLayer
    where Self: 'static + Send + Sized
{
    type Manager: r2d2::ManageConnection<Connection = Self>;

    fn send<S: Serialize>(&self, channel: &str, msg: &S) -> Result<(), ChannelError>;
    fn receive<'a, I>(&self,
                      channels: I,
                      block: bool)
                      -> Result<Option<(String, ChannelReply)>, ChannelError>
        where I: Iterator<Item = &'a String> + Clone;
    fn deserialize<D: Deserialize>(reply: ChannelReply) -> Result<D, ChannelError>;
    fn new_channel(&self, pattern: &str) -> Result<String, ChannelError>;
}


#[derive(Debug)]
pub enum ChannelError {
    ChannelFull,
    MessageTooLarge,
    InvalidChannelName,

    // These hold errors specific to the channel layer being used.
    Transport(Box<Error + Send>),
    Serialize(Box<Error + Send>),
    Deserialize(Box<Error + Send>),
}

impl std::fmt::Display for ChannelError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            ChannelError::ChannelFull => write!(f, "Channel full"),
            ChannelError::MessageTooLarge => write!(f, "Message too large"),
            ChannelError::InvalidChannelName => write!(f, "Invalid channel name"),

            ChannelError::Transport(ref err) => write!(f, "Error in channel transport: {}", err),
            ChannelError::Serialize(ref err) => write!(f, "Error serializing message: {}", err),
            ChannelError::Deserialize(ref err) => write!(f, "Error deserializing message: {}", err),
        }
    }
}

impl Error for ChannelError {
    fn description(&self) -> &str {
        match *self {
            ChannelError::ChannelFull => "Channel full",
            ChannelError::MessageTooLarge => "Message too large",
            ChannelError::InvalidChannelName => "Invalid channel name",

            ChannelError::Transport(ref err) => err.description(),
            ChannelError::Serialize(ref err) => err.description(),
            ChannelError::Deserialize(ref err) => err.description(),
        }
    }

    fn cause(&self) -> Option<&Error> {
        match *self {
            ChannelError::ChannelFull => None,
            ChannelError::MessageTooLarge => None,
            ChannelError::InvalidChannelName => None,

            ChannelError::Transport(ref err) => err.cause(),
            ChannelError::Serialize(ref err) => err.cause(),
            ChannelError::Deserialize(ref err) => err.cause(),
        }
    }
}


#[cfg(test)]
mod tests {
    use super::{
        is_valid_channel_name, validate_channel_name,
    };

    #[test]
    fn test_is_valid_channel_name() {
        assert_eq!(is_valid_channel_name("http.request"), true);
        assert_eq!(is_valid_channel_name("http.request!aS45543"), true);
        assert_eq!(is_valid_channel_name("http.response.body?aS45543"), true);
        assert_eq!(is_valid_channel_name("aA1!?-_."), true);

        assert_eq!(is_valid_channel_name("@"), false);
        assert_eq!(is_valid_channel_name("☃"), false);
    }

    #[test]
    fn test_validate_channel_name() {
        assert!(validate_channel_name("http.request").is_ok());
        assert!(validate_channel_name("@").is_err());
    }
}
