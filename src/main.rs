mod asgi;
mod channels;

#[macro_use]
extern crate serde_derive;
extern crate hyper;
extern crate serde;
extern crate futures;

use std::io::{Read, Write};

use futures::Future;
use hyper::{HttpVersion, Uri};
use hyper::status::StatusCode;
use hyper::server::{Http, Service, Request, Response};
use serde::{Deserialize, Serialize};
use serde::bytes::{ByteBuf, Bytes};

use channels::{ChannelLayer, RedisChannelLayer};
use asgi::http;


fn http_version_to_str(ver: &HttpVersion) -> &'static str {
    match *ver {
        // ASGI spec doesn't actually allow 0.9 to be used, but it's easiest
        // for us to keep our mouth shut.
        HttpVersion::Http09 => "0.9",
        HttpVersion::Http10 => "1.0",
        HttpVersion::Http11 => "1.1",
        _ => panic!("Unsupported HTTP version")
    }
}

fn munge_headers(req: &Request) -> Vec<(ByteBuf, ByteBuf)> {
    req.headers()
        .iter()
        .map(|header| {
            // ASGI spec states to lowercase ASCII characters in header names.
            // We're save to use str.to_lowercase() (which lowercases non-ASCII)
            // because the HTTP spec says header names must be ASCII.
            let name = header.name().to_lowercase().into_bytes();
            let value = header.value_string().into_bytes();
            (ByteBuf::from(name), ByteBuf::from(value))
        })
        .collect()
}


struct AsgiInterface;

impl Service for AsgiInterface {
    type Request = Request;
    type Response = Response;
    type Error = hyper::Error;
    type Future = Box<Future<Item=Response, Error=hyper::Error>>;

    fn call(&self, req: Request) -> Self::Future {
        let channels = RedisChannelLayer::new();

        // TODO: Handle body stream and chunking.
        let body: Vec<u8> = vec![];
        let asgi_req = asgi::http::Request {
            reply_channel: &channels.new_channel("http.response!"),
            http_version: &http_version_to_str(req.version()),
            method: req.method().as_ref(),
            path: req.uri().path(),
            query_string: req.uri().query().unwrap_or(""),
            headers: munge_headers(&req),
            body: Bytes::new(&body),
        };
        channels.send("http.request", &asgi_req);

        let (channel, asgi_resp): (String, asgi::http::Response) =
            channels.receive(&vec![asgi_req.reply_channel], true).unwrap();

        let mut resp = Response::new();
        resp.set_status(StatusCode::from_u16(asgi_resp.status));
        for (name, value) in asgi_resp.headers {
            // TODO: We may set_raw() multiple times for the same header. We should instead
            // be collecting the duplicate in a values vec and calling set_raw() once.
            let name = String::from_utf8(name.into()).unwrap();
            let value: Vec<u8> = value.into();
            resp.headers_mut().set_raw(name, value);
        }
        let body: Vec<u8> = asgi_resp.content.into();
        let resp = resp.with_body(body);
        futures::future::ok(resp).boxed()
    }
}

fn main() {
    println!("Hello, world!");

    let addr = "127.0.0.1:8000".parse().unwrap();
    let server = Http::new().bind(&addr, || Ok(AsgiInterface)).unwrap();
    server.run().unwrap();
}