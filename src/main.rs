mod asgi;
mod channels;

extern crate hyper;

use std::io::{Read, Write};

use hyper::version::HttpVersion;
use hyper::uri::RequestUri;
use hyper::status::StatusCode;
use hyper::server::{Server, Request, Response};

use asgi::{AsgiDeserialize, AsgiSerialize};
use channels::RedisChannelLayer;


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

fn munge_headers(req: &Request) -> Vec<(Vec<u8>, Vec<u8>)> {
    req.headers
        .iter()
        .map(|header| (header.name().to_owned().into_bytes(), header.value_string().into_bytes()))
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
        body: body,
    };
    let mut buf = Vec::new();
    asgi_req.serialize(&mut buf).unwrap();

    println!("{:?}", buf);

    channels.send("http.request", &buf);

    // throw away the result, just make sure it does not fail
    // println!("{:?}", buf);

    let buf = channels.receive_one(&asgi_req.reply_channel);
    let asgi_resp = asgi::http::Response::deserialize(&buf).unwrap();

    *res.status_mut() = StatusCode::from_u16(asgi_resp.status);
    for (name, value) in asgi_resp.headers {
        res.headers_mut().set_raw(String::from_utf8(name).unwrap(), vec![value])
    }

    let mut res = res.start().unwrap();
    res.write_all(&asgi_resp.content);
    ()
}


fn main() {
    println!("Hello, world!");

    Server::http("0.0.0.0:8080").unwrap().handle(hello).unwrap();
}