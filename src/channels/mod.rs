use std;
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
