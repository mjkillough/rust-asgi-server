mod asgi;
mod channels;

#[macro_use]
extern crate serde_derive;
extern crate hyper;
extern crate serde;

use std::io::{Read, Write};

use hyper::version::HttpVersion;
use hyper::uri::RequestUri;
use hyper::status::StatusCode;
use hyper::server::{Server, Request, Response};
use serde::{Deserialize, Serialize};
use serde::bytes::{ByteBuf, Bytes};

use channels::{ChannelLayer, RedisChannelLayer};
use asgi::http;


fn http_version_to_str(ver: HttpVersion) -> &'static str {
    match ver {
        // ASGI spec doesn't actually allow 0.9 to be used, but it's easiest
        // for us to keep our mouth shut.
        HttpVersion::Http09 => "0.9",
        HttpVersion::Http10 => "1.0",
        HttpVersion::Http11 => "1.1",
        HttpVersion::Http20 => "2.0",
    }
}

fn get_path(uri: &RequestUri) -> &str {
    match uri {
        &RequestUri::AbsolutePath(ref path) => path,
        _ => panic!("Don't know how to handle anything else!"),
    }
}

fn munge_headers(req: &Request) -> Vec<(ByteBuf, ByteBuf)> {
    req.headers
        .iter()
        .map(|header| {
            // TODO: lower-case as per ASGI spec.
            let name = header.name().to_owned().into_bytes();
            let value = header.value_string().into_bytes(); // XXX extra alloc?
            (ByteBuf::from(name), ByteBuf::from(value))
        })
        .collect()
}

fn hello(mut req: Request, mut res: Response) {
    let channels = RedisChannelLayer::new();

    let mut body = Vec::new();
    req.read_to_end(&mut body);

    let path = get_path(&req.uri);

    let asgi_req = asgi::http::Request {
        reply_channel: &channels.new_channel("http.response!"),
        http_version: &http_version_to_str(req.version).to_owned(),
        method: req.method.as_ref(),
        path: path,
        query_string: "",
        headers: munge_headers(&req),
        body: Bytes::new(&body),
    };
    channels.send("http.request", &asgi_req);

    // throw away the result, just make sure it does not fail
    // println!("{:?}", buf);

    let asgi_resp: asgi::http::Response = channels.receive_one(&asgi_req.reply_channel);

    *res.status_mut() = StatusCode::from_u16(asgi_resp.status);
    for (name, value) in asgi_resp.headers {
        // TODO: We may set_raw() multiple times for the same header. We should instead
        // be collecting the duplicate in a values vec and calling set_raw() once.
        let name = String::from_utf8(name.into()).unwrap();
        let values = vec![value.into()];
        res.headers_mut().set_raw(name, values);
    }

    let mut res = res.start().unwrap();
    res.write_all(&asgi_resp.content);
    ()
}


fn main() {
    println!("Hello, world!");

    Server::http("0.0.0.0:8080").unwrap().handle(hello).unwrap();
}