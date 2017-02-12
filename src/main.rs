mod asgi;
mod channels;

#[macro_use]
extern crate serde_derive;
extern crate hyper;
extern crate serde;
extern crate futures;

use std::io::{Read, Write};

use futures::stream::Peekable;
use futures::{Async, BoxFuture, Future, Stream};
use hyper::{Body, Chunk, Headers, HttpVersion, Method, Uri};
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
        _ => panic!("Unsupported HTTP version"),
    }
}

fn munge_headers(headers: &Headers) -> Vec<(ByteBuf, ByteBuf)> {
    headers.iter()
        .map(|header| {
            let name = header.name().to_lowercase().into_bytes();
            let value = header.value_string().into_bytes();
            (ByteBuf::from(name), ByteBuf::from(value))
        })
        .collect()
}


struct AsgiInterface;

fn send_request(method: Method,
                uri: Uri,
                version: HttpVersion,
                headers: Headers,
                initial_chunk: Option<Chunk>,
                body: Body)
                -> BoxFuture<String, hyper::Error> {
    let channels = RedisChannelLayer::new();

    let initial_chunk = match initial_chunk {
        Some(ref chunk) => Bytes::new(&chunk),
        None => Bytes::new(&[0; 0]),
    };

    let mut body = body.peekable();
    let more_content = match body.peek() {
        Ok(Async::Ready(None)) => false,
        Ok(_) => true,
        Err(e) => panic!("Unhandled error"),
    };
    let body_channel = match more_content {
        true => channels.new_channel("http.request?"),
        false => "".to_owned(),
    };

    let reply_channel = channels.new_channel("http.response!");
    let asgi_req = asgi::http::Request {
        reply_channel: &reply_channel,
        http_version: &http_version_to_str(&version),
        method: method.as_ref(),
        path: uri.path(),
        query_string: uri.query().unwrap_or(""),
        headers: munge_headers(&headers),
        body: initial_chunk,
        body_channel: Some(&body_channel),
    };

    // TODO: Do this on a thread pool?
    channels.send("http.request", &asgi_req);

    if more_content {
        futures::future::ok((reply_channel.clone())).boxed()
    } else {
        send_body_chunks(reply_channel.clone(), body_channel.clone(), body)
    }
}

fn send_body_chunks(reply_channel: String,
                    body_channel: String,
                    body: Peekable<Body>)
                    -> BoxFuture<String, hyper::Error> {
    // TODO: Could we have just one future (and pass state through) if we did a .fold()?
    let body_channel2 = body_channel.clone();
    body
        // Send each chunk in the body of the request, saying there's more to
        // come, even if we're unsure. We'll send a final chunk with no content
        // and more_content=false.
        .for_each(move |chunk| {
            let asgi_chunk = asgi::http::RequestBodyChunk {
                content: Bytes::new(&chunk),
                closed: false,
                more_content: true,
            };
            let channels = RedisChannelLayer::new();
            channels.send(&body_channel, &asgi_chunk);

            futures::future::ok(())
         })
         // Send final chunk with no content and more_content=false.
        .and_then(move |_| {
            let asgi_chunk = asgi::http::RequestBodyChunk {
                content: Bytes::new(&[0; 0]),
                closed: false,
                more_content: false,
            };
            let channels = RedisChannelLayer::new();
            channels.send(&body_channel2, &asgi_chunk);

            futures::future::ok(reply_channel)
        })
        .boxed()
}


impl Service for AsgiInterface {
    type Request = Request;
    type Response = Response;
    type Error = hyper::Error;
    type Future = Box<Future<Item = Response, Error = hyper::Error>>;

    fn call(&self, req: Request) -> Self::Future {
        let (method, uri, version, headers, body) = req.deconstruct();

        body.into_future()
            .or_else(|(e, _)| futures::future::err(e))
            .and_then(move |(chunk, body)| {
                send_request(method, uri, version, headers, chunk, body)
            })
            .and_then(|reply_channel| {
                let channels = RedisChannelLayer::new();
                let reply_channels = vec![&*reply_channel];
                let (_, asgi_resp): (_, asgi::http::Response) = channels.receive(&reply_channels, true).unwrap();

                let mut resp = Response::new();
                resp.set_status(StatusCode::from_u16(asgi_resp.status));
                for (name, value) in asgi_resp.headers {
                    let name = String::from_utf8(name.into()).unwrap();
                    let value: Vec<u8> = value.into();
                    resp.headers_mut().set_raw(name, value);
                }
                let content: Vec<u8> = asgi_resp.content.into();
                futures::future::ok(resp.with_body(content))
            })
            .boxed()
    }
}

fn main() {
    println!("Hello, world!");

    let addr = "127.0.0.1:8000".parse().unwrap();
    let server = Http::new().bind(&addr, || Ok(AsgiInterface)).unwrap();
    server.run().unwrap();
}