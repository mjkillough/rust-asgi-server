extern crate crossbeam;
extern crate futures_cpupool;
extern crate futures;
extern crate hyper;
extern crate r2d2;
extern crate rand;
extern crate redis;
extern crate rmp_serde;
extern crate rmp;
#[macro_use]
extern crate serde_derive;
extern crate serde;

mod body;
mod channels;
mod http;
mod msgs;

use hyper::server::Http;

use channels::RedisChannelLayer;
use http::AsgiHttpServiceFactory;


fn main() {
    println!("Hello, world!");
    let addr = "127.0.0.1:8000".parse().unwrap();
    let server =
        Http::new().bind(&addr, AsgiHttpServiceFactory::<RedisChannelLayer>::new(&addr)).unwrap();
    server.run().unwrap();
}
