extern crate futures;
extern crate hyper;
extern crate rand;
extern crate redis;
extern crate rmp;
extern crate rmp_serde;
extern crate serde;
#[macro_use]
extern crate serde_derive;

use futures::{BoxFuture, Future, Stream};
use hyper::{Headers, HttpVersion, Method, Uri};
use hyper::status::StatusCode;
use hyper::server::{Http, Service, Request, Response};
use serde::bytes::{ByteBuf, Bytes};

use channels::{ChannelLayer, RedisChannelLayer};

mod asgi;
mod channels;


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
                body: Vec<u8>)
                -> BoxFuture<String, hyper::Error> {
    let channels = RedisChannelLayer::new();

    let chunk_size = 1024 * 1024 * 1024; // 1 MB
    let mut chunks = body.chunks(chunk_size).peekable();
    let initial_chunk = match chunks.next() {
        Some(chunk) => chunk,
        None => &[0; 0],
    };
    let body_channel = match chunks.len() > 0 {
        true => Some(channels.new_channel("http.request.body?").unwrap()),
        false => None,
    };

    // Send the initial chunk of the request to http.request. We must use an extra scope for this
    // because otherwise we'll find reply_channel is borrowed for longer than necessary.
    let reply_channel = channels.new_channel("http.response!").unwrap();
    {
        // TODO: make this async.
        channels.send("http.request",
                  &asgi::http::Request {
                      reply_channel: &reply_channel,
                      http_version: &http_version_to_str(&version),
                      method: method.as_ref(),
                      path: uri.path(),
                      query_string: uri.query().unwrap_or(""),
                      headers: munge_headers(&headers),
                      body: Bytes::from(initial_chunk),
                      // Dance to turn Option<String> to Option<&str>:
                      body_channel: body_channel.as_ref().map(String::as_ref),
                  })
            .unwrap();
    }

    // If the body of the request is over a certain size, then we must break it up and send each
    // chunk separately on a per request http.request.body? channel. We must do some iterator
    // dancing, as we must know which chunk is the last.
    if let Some(body_channel) = body_channel {
        loop {
            match chunks.next() {
                Some(chunk) => {
                    // TODO: make this async.
                    channels.send(&body_channel,
                              &asgi::http::RequestBodyChunk {
                                  content: Bytes::from(chunk),
                                  closed: false,
                                  more_content: !chunks.peek().is_none(),
                              })
                        .unwrap();
                }
                None => break,
            }
        }
    }

    futures::future::ok(reply_channel).boxed()
}

fn wait_for_response(reply_channel: String) -> BoxFuture<asgi::http::Response, hyper::Error> {
    let channels = RedisChannelLayer::new();
    let reply_channels = vec![&*reply_channel];
    let (_, asgi_resp): (_, asgi::http::Response) = channels.receive(&reply_channels, true)
        .unwrap()
        .unwrap();
    futures::future::ok(asgi_resp).boxed()
}

fn send_response(asgi_resp: asgi::http::Response) -> BoxFuture<Response, hyper::Error> {
    let mut resp = Response::new();
    resp.set_status(StatusCode::from_u16(asgi_resp.status));
    for (name, value) in asgi_resp.headers {
        let name = String::from_utf8(name.into()).unwrap();
        let value: Vec<u8> = value.into();
        resp.headers_mut().set_raw(name, value);
    }
    let content: Vec<u8> = asgi_resp.content.into();
    futures::future::ok(resp.with_body(content)).boxed()
}


impl Service for AsgiInterface {
    type Request = Request;
    type Response = Response;
    type Error = hyper::Error;
    type Future = Box<Future<Item = Response, Error = hyper::Error>>;

    fn call(&self, req: Request) -> Self::Future {
        let (method, uri, version, headers, body) = req.deconstruct();

        // Wait for the entire body of the request to be in memory before proceeding. It feels like
        // it would be nice to send each chunk over ASGI separately, but once Channels receives a
        // http.request, it blocks while it waits for its body. Buffer the entire body here to
        // avoid blocking in the sync back-end Channels worker processes.
        body.collect()
            .and_then(|body| {
                // We get a Vec<Chunk> - flatten into a simple array of bytes. We could avoid the
                // copies here if we made send_request smarter and have it correct re-chunk a
                // Vec<Chunk> into chunks of the right size. (We have no control over how hyper chunks).
                let body = body.iter()
                    .fold(Vec::new(), |mut vec, chunk| { vec.extend_from_slice(&chunk); vec });
                futures::future::ok(body)
            })
            .and_then(move |body| {
                send_request(method, uri, version, headers, body)
            })
            .and_then(wait_for_response)
            .and_then(send_response)
            .boxed()
    }
}

fn main() {
    println!("Hello, world!");

    let addr = "127.0.0.1:8000".parse().unwrap();
    let server = Http::new().bind(&addr, || Ok(AsgiInterface)).unwrap();
    server.run().unwrap();
}