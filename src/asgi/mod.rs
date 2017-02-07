pub mod http;

use std::error::Error;
use std::io::Write;


// It would be nice to use serde/rmp_serde instead, but I couldn't get this
// to work. Do it manually for now - it should be simple enough to come back
// to when I know more rust!
pub trait AsgiSerialize {
    fn serialize<W: Write>(&self, buf: &mut W) -> Result<(), Box<Error>>;
}
pub trait AsgiDeserialize: Sized {
    fn deserialize(&Vec<u8>) -> Result<Self, Box<Error>>;
}
