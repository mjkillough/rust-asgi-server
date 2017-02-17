extern crate crossbeam;
extern crate futures;
extern crate futures_cpupool;
extern crate hyper;
extern crate rand;
extern crate redis;
extern crate rmp;
extern crate rmp_serde;
extern crate serde;
#[macro_use]
extern crate serde_derive;

use std::clone::Clone;

use futures::{BoxFuture, Future, Stream};
use futures_cpupool::CpuPool;
use hyper::{Headers, HttpVersion, Method, Uri};
use hyper::status::StatusCode;
use hyper::server::{Http, NewService, Service, Request, Response};
use serde::bytes::{ByteBuf, Bytes};

use channels::{ChannelLayer, RedisChannelLayer, RedisChannelError};
use reply_pump::ReplyPump;

mod asgi;
mod channels;
mod reply_pump;


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

fn send_request_sync(method: Method,
                     uri: Uri,
                     version: HttpVersion,
                     headers: Headers,
                     body: Vec<u8>)
                     -> Result<String, RedisChannelError> {
    let channels = RedisChannelLayer::new();

    let chunk_size = 1 * 1024 * 1024; // 1 MB
    let mut chunks = body.chunks(chunk_size).peekable();
    let initial_chunk = match chunks.next() {
        Some(chunk) => chunk,
        None => &[0; 0],
    };
    let body_channel = match chunks.len() > 0 {
        true => Some(channels.new_channel("http.request.body?")?),
        false => None,
    };

    // Send the initial chunk of the request to http.request. We must use an extra scope for this
    // because otherwise we'll find reply_channel is borrowed for longer than necessary.
    let reply_channel = channels.new_channel("http.response!")?;
    {
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
                  })?
    }

    // If the body of the request is over a certain size, then we must break it up and send each
    // chunk separately on a per request http.request.body? channel. We must do some iterator
    // dancing, as we must know which chunk is the last.
    if let Some(body_channel) = body_channel {
        loop {
            match chunks.next() {
                Some(chunk) => {
                    channels.send(&body_channel,
                              &asgi::http::RequestBodyChunk {
                                  content: Bytes::from(chunk),
                                  closed: false,
                                  more_content: !chunks.peek().is_none(),
                              })?
                }
                None => break,
            }
        }
    }

    Ok(reply_channel)
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


struct AsgiHttpServiceFactory {
    reply_pump: ReplyPump<RedisChannelLayer>,
}

impl AsgiHttpServiceFactory {
    fn new() -> AsgiHttpServiceFactory {
        let channel_layer = RedisChannelLayer::new();
        let reply_pump = reply_pump::ReplyPump::new(channel_layer);
        AsgiHttpServiceFactory { reply_pump: reply_pump }
    }
}

impl NewService for AsgiHttpServiceFactory {
    type Request = Request;
    type Response = Response;
    type Error = hyper::Error;
    type Instance = AsgiHttpService;

    fn new_service(&self) -> Result<Self::Instance, std::io::Error> {
        Ok(AsgiHttpService { reply_pump: self.reply_pump.clone() })
    }
}


struct AsgiHttpService {
    reply_pump: ReplyPump<RedisChannelLayer>,
}

impl Service for AsgiHttpService {
    type Request = Request;
    type Response = Response;
    type Error = hyper::Error;
    type Future = Box<Future<Item = Response, Error = hyper::Error>>;

    fn call(&self, req: Request) -> Self::Future {
        let (method, uri, version, headers, body) = req.deconstruct();
        let cpu_pool = CpuPool::new(4);

        let reply_pump = self.reply_pump.clone();

        // Wait for the entire body of the request to be in memory before proceeding. It feels like
        // it would be nice to send each chunk over ASGI separately, but once Channels receives a
        // http.request, it blocks while it waits for its body. Buffer the entire body here to
        // avoid blocking in the sync back-end Channels worker processes.
        body.collect()
            .and_then(|body| {
                // We get a Vec<Chunk> - flatten into a simple Vec<u8>. We could avoid copies if
                // send_request_sync() were smarter, but it probably isn't worth it.
                let body = body.iter()
                    .fold(Vec::new(), |mut vec, chunk| {
                        vec.extend_from_slice(&chunk);
                        vec
                    });
                futures::future::ok(body)
            })
            .and_then(move |body| {
                // In order to avoid complex code where ownership of various parts of the request
                // body is unclear, we send the request synchronously. We do this on the
                // thread-pool to avoid blocking the event loop.
                cpu_pool.spawn_fn(move || send_request_sync(method, uri, version, headers, body))
                    // TODO: Figure out how to communicate this error properly.
                    .or_else(|_| futures::future::err(hyper::error::Error::Method))
            })
            // XXX The error handling here is a bit skew-whiff!
            .map_err(|e| ())
            .and_then(move |reply_channel| reply_pump.wait_for_reply_async(reply_channel))
            .map_err(|e| hyper::Error::Incomplete)
            .and_then(send_response)
            .boxed()
    }
}

fn main() {
    println!("Hello, world!");
    let addr = "127.0.0.1:8000".parse().unwrap();
    let server = Http::new().bind(&addr, AsgiHttpServiceFactory::new()).unwrap();
    server.run().unwrap();
}