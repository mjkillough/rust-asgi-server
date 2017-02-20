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

use futures::{Async, BoxFuture, Future, Stream, Poll};
use futures::stream::unfold;
use futures_cpupool::CpuPool;
use hyper::{Headers, HttpVersion, Method, Uri};
use hyper::header::ContentType;
use hyper::status::StatusCode;
use hyper::server::{Http, NewService, Service, Request, Response};
use hyper::mime::{Mime, TopLevel, SubLevel, Attr, Value};
use serde::bytes::{ByteBuf, Bytes};

use channels::{ChannelLayer, RedisChannelLayer, RedisChannelError};
use reply_pump::ReplyPump;

mod asgi;
mod channels;
mod reply_pump;


fn error_response(status: StatusCode, body: &str) -> Response<BodyStream> {
    let body = format!(include_str!("error.html"),
                       status = status,
                       body = body);
    Response::new()
        .with_status(status)
        .with_header(ContentType(Mime(TopLevel::Text,
                                      SubLevel::Html,
                                      vec![(Attr::Charset, Value::Utf8)])))
        .with_body(BodyStream::error(body))
}


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


enum BodyStream {
    Error(ErrorBodyStream),
    Response(ResponseBodyStream),
}

impl BodyStream {
    fn response(pump: ReplyPump<RedisChannelLayer>,
                channel: String,
                initial_chunk: asgi::http::ResponseBodyChunk)
                -> Self {
        BodyStream::Response(ResponseBodyStream {
            pump: pump,
            channel: channel,
            future: Some(futures::future::ok(initial_chunk).boxed()),
        })
    }

    fn error(body: String) -> Self {
        BodyStream::Error(ErrorBodyStream(Some(body)))
    }
}

impl Stream for BodyStream {
    type Item = Vec<u8>;
    type Error = hyper::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match self {
            &mut BodyStream::Error(ref mut resp) => resp.poll(),
            &mut BodyStream::Response(ref mut resp) => resp.poll(),
        }
    }
}


struct ErrorBodyStream(Option<String>);

impl Stream for ErrorBodyStream {
    type Item = Vec<u8>;
    type Error = hyper::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match std::mem::replace(&mut self.0, None) {
            Some(body) => Ok(Async::Ready(Some(body.into_bytes()))),
            None => Ok(Async::Ready(None)),
        }
    }
}


struct ResponseBodyStream {
    pump: ReplyPump<RedisChannelLayer>,
    channel: String,
    future: Option<BoxFuture<asgi::http::ResponseBodyChunk, ()>>,
}

impl Stream for ResponseBodyStream {
    type Item = Vec<u8>;
    type Error = hyper::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match std::mem::replace(&mut self.future, None) {
            // We are waiting for the next chunk to be ready - poll the future.
            Some(mut future) => {
                match future.poll() {
                    // We have a chunk. If it isn't our last, then start waiting for the next chunk,
                    // whilst we yield this one.
                    Ok(Async::Ready(resp)) => {
                        self.future = match resp.more_content {
                            true => Some(self.pump.wait_for_reply_async(self.channel.clone())),
                            false => None,
                        };
                        // Yield the chunk we've received.
                        Ok(Async::Ready(Some(resp.content.into())))
                    }
                    // Our future isn't ready - remember to poll it next time.
                    Ok(Async::NotReady) => {
                        self.future = Some(future);
                        Ok(Async::NotReady)
                    }
                    // XXX What error should we be sending here?
                    Err(()) => Err(hyper::Error::Incomplete),
                }
            }
            // The last chunk was our last. Indicate end-of-stream.
            None => Ok(Async::Ready(None)),
        }
    }
}


fn send_response((pump, channel, asgi_resp): (ReplyPump<RedisChannelLayer>,
                                              String,
                                              asgi::http::Response))
                 -> Result<Response<BodyStream>, ()> {
    let mut resp: Response<BodyStream> = Response::new();
    resp.set_status(StatusCode::from_u16(asgi_resp.status));
    for (name, value) in asgi_resp.headers {
        let name = String::from_utf8(name.into()).unwrap();
        let value: Vec<u8> = value.into();
        resp.headers_mut().set_raw(name, value);
    }

    let initial_chunk = asgi::http::ResponseBodyChunk {
        content: asgi_resp.content,
        more_content: asgi_resp.more_content,
    };
    let stream = BodyStream::response(pump, channel, initial_chunk);
    Ok(resp.with_body(stream))
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
    type Response = Response<BodyStream>;
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
    type Response = Response<BodyStream>;
    type Error = hyper::Error;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn call(&self, req: Request) -> Self::Future {
        let (method, uri, version, headers, body) = req.deconstruct();
        let cpu_pool = CpuPool::new(4);

        let reply_pump = self.reply_pump.clone();

        // We chain a series of futures together in order to handle the request/response async.
        // We don't actually care about the errors of the individual stages, as we'll return a
        // generic error response to the client, so we keep it simple and  map all errors to ().
        body
            // Wait for the entire body of the request to be in memory before proceeding. It feels like
            // it would be nice to send each chunk over ASGI separately, but once Channels receives a
            // http.request, it blocks while it waits for its body. Buffer the entire body here to
            // avoid blocking in the sync back-end Channels worker processes.
            .collect()
            .map_err(|_| ())
            // Convert our Vec<Chunk> to a Vec<u8>.
            .map(|body| {
                body.iter()
                    .fold(Vec::new(), |mut vec, chunk| {
                        vec.extend_from_slice(&chunk);
                        vec
                    })
            })
            // Send our body down the channel. To keep the code simple we do this in one synchronous
            // operation on the thread-pool.
            .and_then(move |body| {
                cpu_pool.spawn_fn(move || send_request_sync(method, uri, version, headers, body))
                    .map_err(|_| ())
            })
            .map_err(|_| ())
            // We wait for the initial response on the request's reply channel. We'll wait for
            // subsequent chunks inside the body stream.
            .and_then(move |reply_channel| {
                reply_pump.wait_for_reply_async(reply_channel.clone())
                    .map(move |asgi_response: asgi::http::Response| (reply_pump, reply_channel, asgi_response))
                    .map_err(|_| ())
            })
            // Start sending the response to the client. If this is a streaming response, we'll
            // return a body stream which continues to send chunks as we receive them.
            .and_then(send_response)
            // If we encountered any error along the way, give a generic error response to the
            // client. Note that we do not propogate the error to Hyper - the request has succeeded
            // as far as it's concerned.
            .or_else(|_| {
                futures::future::ok(error_response(StatusCode::InternalServerError, "Unknown server error"))
            })
            .boxed()
    }
}

fn main() {
    println!("Hello, world!");
    let addr = "127.0.0.1:8000".parse().unwrap();
    let server = Http::new().bind(&addr, AsgiHttpServiceFactory::new()).unwrap();
    server.run().unwrap();
}